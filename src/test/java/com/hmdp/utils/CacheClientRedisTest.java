package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Real Redis Lua/lease/cache contracts. Run only against the isolated test Redis. */
@EnabledIfSystemProperty(named = "hmdp.test.redis.port", matches = "[0-9]+")
class CacheClientRedisTest {
    private LettuceConnectionFactory connection;
    private StringRedisTemplate redis;
    private CacheClient cache;
    private String prefix;

    @BeforeEach
    void setup() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("127.0.0.1",
                Integer.parseInt(System.getProperty("hmdp.test.redis.port")));
        config.setPassword(System.getProperty("hmdp.test.redis.password", "hmdp-test"));
        config.setDatabase(14);
        connection = new LettuceConnectionFactory(config);
        connection.afterPropertiesSet();
        redis = new StringRedisTemplate(connection);
        cache = new CacheClient(redis);
        prefix = "cache-unit:" + UUID.randomUUID() + ":";
    }

    @AfterEach
    void cleanup() {
        cache.shutdown();
        redis.delete(prefix + 1);
        redis.delete("lock:cache:" + prefix + 1);
        connection.destroy();
    }

    private Shop shop(String name) {
        Shop value = new Shop();
        value.setId(1L);
        value.setName(name);
        return value;
    }

    @Test
    void concurrentColdMissesCallDatabaseOnceAndStoreEnvelopeWithPhysicalTtl() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(32);
        AtomicInteger databaseCalls = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Shop>> requests = new ArrayList<>();
        try {
            for (int i = 0; i < 64; i++) {
                requests.add(workers.submit(() -> {
                    start.await();
                    return cache.queryWithLogicalExpire(prefix, 1L, Shop.class, id -> {
                        databaseCalls.incrementAndGet();
                        try { Thread.sleep(100); }
                        catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
                        return shop("loaded");
                    }, 30L, TimeUnit.MINUTES);
                }));
            }
            start.countDown();
            for (Future<Shop> request : requests) assertEquals("loaded", request.get(15, TimeUnit.SECONDS).getName());
            assertEquals(1, databaseCalls.get());
            assertFalse(Boolean.TRUE.equals(redis.hasKey("lock:cache:" + prefix + 1)));
            Long ttl = redis.getExpire(prefix + 1, TimeUnit.MILLISECONDS);
            assertNotNull(ttl);
            assertTrue(ttl >= 3590000 && ttl <= 4320000, "physical TTL is 60..72 minutes");
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    void negativeCacheUsesLiteralEmptyValueAndDoesNotRequeryDatabase() {
        AtomicInteger databaseCalls = new AtomicInteger();
        for (int i = 0; i < 2; i++) assertNull(cache.queryWithLogicalExpire(prefix, 1L,
                Shop.class, id -> { databaseCalls.incrementAndGet(); return null; }, 30L, TimeUnit.MINUTES));
        assertEquals(1, databaseCalls.get());
        assertEquals("", redis.opsForValue().get(prefix + 1));
        Long ttl = redis.getExpire(prefix + 1, TimeUnit.MILLISECONDS);
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 2400);
    }

    @Test
    void staleWorkerCannotOverwriteOrUnlockReplacementLease() throws Exception {
        RedisData stale = new RedisData();
        stale.setData(shop("old"));
        stale.setExpireTime(LocalDateTime.now().minusMinutes(1));
        redis.opsForValue().set(prefix + 1, JSONUtil.toJsonStr(stale));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        try {
            assertEquals("old", cache.queryWithLogicalExpire(prefix, 1L, Shop.class, id -> {
                entered.countDown();
                try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException ex) { throw new RuntimeException(ex); }
                finally { finished.countDown(); }
                return shop("stale-worker-new-value");
            }, 30L, TimeUnit.MINUTES).getName());
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            String lock = "lock:cache:" + prefix + 1;
            // Simulates the old lease expiring and a different worker obtaining its replacement.
            redis.opsForValue().set(lock, "replacement-owner", 10L, TimeUnit.SECONDS);
            release.countDown();
            assertTrue(finished.await(5, TimeUnit.SECONDS));
            // Wait until the submitted task (including its Lua finally block) has completed.
            java.lang.reflect.Field executorField = CacheClient.class.getDeclaredField("rebuildExecutor");
            executorField.setAccessible(true);
            ExecutorService executor = (ExecutorService) executorField.get(cache);
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals("replacement-owner", redis.opsForValue().get(lock));
            RedisData stored = JSONUtil.toBean(redis.opsForValue().get(prefix + 1), RedisData.class);
            Shop storedShop = JSONUtil.toBean(JSONUtil.toJsonStr(stored.getData()), Shop.class);
            assertEquals("old", storedShop.getName());
        } finally {
            release.countDown();
        }
    }
}
