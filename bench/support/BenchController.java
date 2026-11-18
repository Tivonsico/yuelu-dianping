package com.hmdp.bench;

import com.hmdp.dto.Result;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import javax.annotation.Resource;

/** Reconstructed baselines, compiled ONLY into the benchmark artifact. */
@Profile("bench")
@RestController
@RequestMapping("/bench")
public class BenchController {
    @Resource private IShopService shops;
    @Resource private IVoucherOrderService orders;
    @Resource private RedissonClient redisson;

    @GetMapping("/shop/{id}")
    public Result directShop(@PathVariable Long id) {
        Object shop = shops.getById(id);
        return shop == null ? Result.fail("店铺不存在") : Result.ok(shop);
    }

    @PostMapping("/sync/{id}")
    public Result synchronousOrder(@PathVariable Long id) {
        RLock lock = redisson.getLock("lock:order:" + UserHolder.getUser().getId());
        if (!lock.tryLock()) return Result.fail("禁止重复下单");
        try { return orders.getResult(id); }
        finally { lock.unlock(); }
    }
}
