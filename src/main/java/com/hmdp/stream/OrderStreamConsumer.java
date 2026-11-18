package com.hmdp.stream;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Single-worker Redis Stream consumer. Reservations and XADD are atomic in seckill.lua. */
@Component
@Slf4j
public class OrderStreamConsumer {
    static final String STREAM = "stream.orders";
    static final String GROUP = "g1";
    static final String CONSUMER = "c1";
    private final StringRedisTemplate redis;
    private final IVoucherOrderService orders;
    private final RedissonClient redisson;
    private volatile boolean running;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "order-stream-consumer");
        thread.setDaemon(true);
        return thread;
    });

    public OrderStreamConsumer(StringRedisTemplate redis, IVoucherOrderService orders, RedissonClient redisson) {
        this.redis = redis;
        this.orders = orders;
        this.redisson = redisson;
    }

    @PostConstruct
    public void start() {
        running = true;
        executor.submit(() -> {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    ensureGroup();
                    List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                            Consumer.from(GROUP, CONSUMER), StreamReadOptions.empty().count(20),
                            StreamOffset.create(STREAM, ReadOffset.from("0")));
                    if (records == null || records.isEmpty()) {
                        records = redis.opsForStream().read(Consumer.from(GROUP, CONSUMER),
                                StreamReadOptions.empty().count(20).block(Duration.ofSeconds(1)),
                                StreamOffset.create(STREAM, ReadOffset.lastConsumed()));
                    }
                    if (records != null) {
                        for (MapRecord<String, Object, Object> record : records) {
                            if (!running) break;
                            VoucherOrder order = BeanUtil.fillBeanWithMap(record.getValue(), new VoucherOrder(), true);
                            handle(order); // Proxied transaction has committed before XACK.
                            redis.opsForStream().acknowledge(STREAM, GROUP, record.getId());
                            redis.opsForStream().delete(STREAM, record.getId());
                        }
                    }
                } catch (Exception failure) {
                    // Retain pending messages; never compensate an ambiguous database result blindly.
                    if (running) log.warn("Order retained in Stream pending for retry: {}", failure.toString());
                    try { Thread.sleep(200); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
            }
        });
    }

    void handle(VoucherOrder order) {
        RLock lock = redisson.getLock("lock:order:" + order.getUserId());
        if (!lock.tryLock()) {
            if (orders.count(new QueryWrapper<VoucherOrder>()
                    .eq("user_id", order.getUserId()).eq("voucher_id", order.getVoucherId())) > 0) return;
            throw new IllegalStateException("Order lock busy; keep pending");
        }
        try { orders.createVoucherOrder(order); }
        finally { lock.unlock(); }
    }

    private void ensureGroup() {
        try {
            redis.execute((RedisCallback<Object>) connection -> connection.execute("XGROUP",
                    bytes("CREATE"), bytes(STREAM), bytes(GROUP), bytes("0"), bytes("MKSTREAM")));
        } catch (RuntimeException failure) {
            Throwable cause = failure;
            while (cause.getCause() != null) cause = cause.getCause();
            if (cause.getMessage() == null || !cause.getMessage().contains("BUSYGROUP")) throw failure;
        }
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    @PreDestroy
    public void stop() {
        running = false;
        executor.shutdownNow();
        try { executor.awaitTermination(6, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }
}
