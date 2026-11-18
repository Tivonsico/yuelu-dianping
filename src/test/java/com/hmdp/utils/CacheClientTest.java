package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Unit contracts; Redis operations are mocked, so these are not throughput measurements. */
class CacheClientTest {
    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private CacheClient cache;
    private final Map<String, String> storage = new ConcurrentHashMap<>();
    private final Map<String, Long> ttls = new ConcurrentHashMap<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(anyString())).thenAnswer(inv -> storage.get(inv.getArgument(0)));
        when(values.setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(inv -> storage.putIfAbsent(inv.getArgument(0), inv.getArgument(1)) == null);
        doAnswer(inv -> {
            storage.put(inv.getArgument(0), inv.getArgument(1));
            ttls.put(inv.getArgument(0), inv.<TimeUnit>getArgument(3).toMillis(inv.getArgument(2)));
            return null;
        }).when(values).set(anyString(), anyString(), anyLong(), any(TimeUnit.class));
        // A small fake for script boundaries, not a substitute for actual Redis Lua integration.
        when(redis.execute(any(RedisScript.class), anyList(), anyString())).thenAnswer(inv -> {
            List<String> keys = inv.getArgument(1);
            if (!inv.getArgument(2).equals(storage.get(keys.get(0)))) return 0L;
            storage.remove(keys.get(0));
            return 1L;
        });
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString()))
                .thenAnswer(inv -> {
            List<String> keys = inv.getArgument(1);
            if (!inv.getArgument(2).equals(storage.get(keys.get(0)))) return 0L;
            storage.put(keys.get(1), inv.getArgument(3));
            ttls.put(keys.get(1), Long.parseLong(inv.getArgument(4)));
            return 1L;
        });        cache = new CacheClient(redis);
    }

    @AfterEach
    void cleanup() { cache.shutdown(); }

    private Shop shop(String name) {
        Shop shop = new Shop();
        shop.setId(1L);
        shop.setName(name);
        return shop;
    }

    @Test
    void coldExistingShopLoadsDatabaseThenReusesCache() {
        AtomicInteger calls = new AtomicInteger();
        Shop first = cache.queryWithLogicalExpire("test:", 1L, Shop.class, id -> {
            calls.incrementAndGet();
            return shop("existing");
        }, 30L, TimeUnit.MINUTES);
        assertNotNull(first, "cold cache must not hide a real shop");
        assertEquals("existing", first.getName());
        Shop second = cache.queryWithLogicalExpire("test:", 1L, Shop.class, id -> {
            fail("warm lookup must not query MySQL");
            return null;
        }, 30L, TimeUnit.MINUTES);
        assertEquals("existing", second.getName());
        assertEquals(1, calls.get());
        assertTrue(ttls.get("test:1") >= TimeUnit.MINUTES.toMillis(60));
        assertTrue(ttls.get("test:1") <= TimeUnit.MINUTES.toMillis(72));
    }

    @Test
    void coldMissingShopStoresLiteralNegativeSentinel() {
        AtomicInteger calls = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            assertNull(cache.queryWithLogicalExpire("test:", 99L, Shop.class, id -> {
                calls.incrementAndGet();
                return null;
            }, 30L, TimeUnit.MINUTES));
        }
        assertEquals(1, calls.get());
        assertEquals("", storage.get("test:99"));
        assertTrue(ttls.get("test:99") >= 2000 && ttls.get("test:99") <= 2400);
    }

    @Test
    void passThroughNegativeSentinelAvoidsSecondDatabaseRead() {
        AtomicInteger calls = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            assertNull(cache.queryWithPassThrough("plain:", 9L, Shop.class, id -> {
                calls.incrementAndGet();
                return null;
            }, 30L, TimeUnit.MINUTES));
        }
        assertEquals(1, calls.get());
        assertEquals("", storage.get("plain:9"));
    }

    @Test
    void staleShopReturnsImmediatelyAndOneWorkerRefreshesIt() throws Exception {
        RedisData stale = new RedisData();
        stale.setData(shop("old"));
        stale.setExpireTime(LocalDateTime.now().minusMinutes(1));
        storage.put("test:1", JSONUtil.toJsonStr(stale));
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        java.util.function.Function<Long, Shop> fallback = id -> {
            calls.incrementAndGet();
            entered.countDown();
            try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
            catch (InterruptedException ex) { throw new RuntimeException(ex); }
            return shop("new");
        };
        assertEquals("old", cache.queryWithLogicalExpire("test:", 1L, Shop.class, fallback,
                30L, TimeUnit.MINUTES).getName());
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertEquals("old", cache.queryWithLogicalExpire("test:", 1L, Shop.class, fallback,
                30L, TimeUnit.MINUTES).getName());
        release.countDown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (storage.containsKey("lock:cache:test:1") && System.nanoTime() < deadline) Thread.sleep(5);
        assertEquals(1, calls.get());
        assertEquals("new", cache.queryWithLogicalExpire("test:", 1L, Shop.class, fallback,
                30L, TimeUnit.MINUTES).getName());
    }

    @Test
    void plainHistoricalPrewarmValueRemainsReadable() {
        storage.put("test:1", JSONUtil.toJsonStr(shop("legacy")));
        assertEquals("legacy", cache.queryWithLogicalExpire("test:", 1L, Shop.class, id -> {
            fail("legacy prewarm is a cache hit");
            return null;
        }, 30L, TimeUnit.MINUTES).getName());
        assertTrue(storage.get("test:1").contains("expireTime"));
    }

    @Test
    void randomPhysicalTtlStaysInDocumentedBoundsAndVaries() {
        java.util.Set<Long> observed = new java.util.HashSet<>();
        for (int i = 0; i < 40; i++) {
            cache.set("jitter:" + i, shop("jitter"), 30L, TimeUnit.MINUTES);
            long ttl = ttls.get("jitter:" + i);
            assertTrue(ttl >= 1800000 && ttl <= 2160000);
            observed.add(ttl);
        }
        assertTrue(observed.size() > 1, "jitter should spread expiry times");
    }
}
