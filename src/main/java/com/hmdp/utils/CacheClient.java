package com.hmdp.utils;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/** Cache-aside, negative caching and stale-while-revalidate for shop lookups. */
@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private final ExecutorService rebuildExecutor = new ThreadPoolExecutor(
            4, 4, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(64),
            runnable -> {
                Thread thread = new Thread(runnable, "cache-rebuild");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());
    private static final DefaultRedisScript<Long> UNLOCK = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end return 0", Long.class);
    private static final DefaultRedisScript<Long> WRITE_IF_OWNER = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then redis.call('set', KEYS[2], ARGV[2], 'PX', ARGV[3]); return 1 end return 0", Long.class);

    @Autowired
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // Add 0..20% jitter, expressed in milliseconds so short TTLs also vary.
    private long ttlMillis(Long time, TimeUnit unit) {
        long base = Math.max(1L, unit.toMillis(time));
        return base + ThreadLocalRandom.current().nextLong(Math.max(1L, base / 5) + 1L);
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, value == null || "".equals(value)
                ? "" : JSONUtil.toJsonStr(value), ttlMillis(time, unit), TimeUnit.MILLISECONDS);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        if (value == null) {
            set(key, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
            return;
        }
        long ttl = ttlMillis(time, unit);
        stringRedisTemplate.opsForValue().set(key, logicalJson(value, ttl), ttl * 2, TimeUnit.MILLISECONDS);
    }

    private String logicalJson(Object value, long ttlMillis) {
        RedisData data = new RedisData();
        data.setData(value);
        data.setExpireTime(LocalDateTime.now().plusNanos(ttlMillis * 1_000_000L));
        return JSONUtil.toJsonStr(data);
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if ("".equals(json)) return null;
        if (json != null) return readCached(json, type);
        R value = dbFallback.apply(id);
        set(key, value, value == null ? CACHE_NULL_TTL : time,
                value == null ? TimeUnit.SECONDS : unit);
        return value;
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
                                            Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if ("".equals(json)) return null;
        if (json == null) return loadCold(key, id, type, dbFallback, time, unit);
        JSONObject object = JSONUtil.parseObj(json);
        // Historical prewarm helpers stored plain shop JSON under this same prefix.
        if (!object.containsKey("expireTime")) {
            R value = JSONUtil.toBean(object, type);
            setWithLogicalExpire(key, value, time, unit);
            return value;
        }
        RedisData data = JSONUtil.toBean(object, RedisData.class);
        R value = JSONUtil.toBean(JSONUtil.toJsonStr(data.getData()), type);
        if (data.getExpireTime().isAfter(LocalDateTime.now())) return value;
        String lockKey = "lock:cache:" + key;
        String token = tryLock(lockKey);
        if (token != null) {
            try {
                rebuildExecutor.submit(() -> {
                    try {
                        writeIfOwner(key, dbFallback.apply(id), time, unit, lockKey, token);
                    } catch (Exception ex) {
                        log.warn("Cache rebuild failed for {}", key, ex);
                    } finally {
                        unlock(lockKey, token);
                    }
                });
            } catch (RejectedExecutionException ex) {
                unlock(lockKey, token);
                log.warn("Cache rebuild queue full for {}", key);
            }
        }
        return value;
    }

    private <R, ID> R loadCold(String key, ID id, Class<R> type, Function<ID, R> fallback,
                                Long time, TimeUnit unit) {
        String lockKey = "lock:cache:" + key;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(LOCK_SHOP_TTL + 1);
        do {
            String token = tryLock(lockKey);
            if (token != null) {
                try {
                    String json = stringRedisTemplate.opsForValue().get(key);
                    if (json != null) return readCached(json, type);
                    R value = fallback.apply(id);
                    writeIfOwner(key, value, time, unit, lockKey, token);
                    return value;
                } finally {
                    unlock(lockKey, token);
                }
            }
            String json = stringRedisTemplate.opsForValue().get(key);
            if (json != null) return readCached(json, type);
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for cache rebuild", ex);
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Timed out waiting for cache rebuild: " + key);
    }

    private <R> R readCached(String json, Class<R> type) {
        if (json.isEmpty()) return null;
        JSONObject object = JSONUtil.parseObj(json);
        return JSONUtil.toBean(object.containsKey("expireTime")
                ? object.getJSONObject("data") : object, type);
    }

    private void writeIfOwner(String key, Object value, Long time, TimeUnit unit,
                              String lockKey, String token) {
        long ttl = value == null ? ttlMillis(CACHE_NULL_TTL, TimeUnit.SECONDS) : ttlMillis(time, unit);
        String json = value == null ? "" : logicalJson(value, ttl);
        stringRedisTemplate.execute(WRITE_IF_OWNER, Arrays.asList(lockKey, key), token, json,
                Long.toString(value == null ? ttl : ttl * 2));
    }

    private String tryLock(String key) {
        String token = UUID.randomUUID().toString();
        return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(
                key, token, LOCK_SHOP_TTL, TimeUnit.SECONDS)) ? token : null;
    }

    private void unlock(String key, String token) {
        stringRedisTemplate.execute(UNLOCK, Collections.singletonList(key), token);
    }

    @PreDestroy
    public void shutdown() {
        rebuildExecutor.shutdownNow();
    }
}
