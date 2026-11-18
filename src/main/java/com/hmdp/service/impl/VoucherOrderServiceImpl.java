package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;

@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT = new DefaultRedisScript<>();
    static {
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
                voucherId.toString(), UserHolder.getUser().getId().toString(), Long.toString(orderId));
        if (result == null) {
            throw new IllegalStateException("No result from stock reservation");
        }
        if (result != 0) {
            return Result.fail(result == 1 ? "库存不足" : "禁止重复下单");
        }
        // Accepted for asynchronous processing; this is not a DB commit acknowledgment.
        return Result.ok(orderId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result getResult(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        if (alreadyOrdered(userId, voucherId)) {
            return Result.fail("禁止重复购买");
        }
        if (!decrementStock(voucherId)) {
            return Result.fail("库存不足");
        }
        VoucherOrder order = new VoucherOrder();
        order.setId(redisIdWorker.nextId("order"));
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        if (!save(order)) {
            throw new IllegalStateException("Order insert failed");
        }
        return Result.ok(order.getId());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder order) {
        // Re-delivery after an ambiguous publish/ACK or restart is expected.
        if (alreadyOrdered(order.getUserId(), order.getVoucherId())) {
            return;
        }
        if (!decrementStock(order.getVoucherId())) {
            // Roll back and retry, then DLQ. Never create an order without DB stock.
            throw new IllegalStateException("DB stock unavailable for accepted order " + order.getId());
        }
        if (!save(order)) {
            throw new IllegalStateException("Order insert failed");
        }
        // Unique(user_id,voucher_id) also guards races; a rejected insert rolls back stock.
    }

    private boolean alreadyOrdered(Long userId, Long voucherId) {
        return count(new QueryWrapper<VoucherOrder>()
                .eq("user_id", userId).eq("voucher_id", voucherId)) > 0;
    }

    private boolean decrementStock(Long voucherId) {
        return seckillVoucherService.update(new LambdaUpdateWrapper<SeckillVoucher>()
                .eq(SeckillVoucher::getVoucherId, voucherId)
                .gt(SeckillVoucher::getStock, 0).setSql("stock=stock-1"));
    }
}
