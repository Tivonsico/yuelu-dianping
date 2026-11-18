package com.hmdp.stream;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderStreamConsumerTest {
    @Test void transactionFailureIsRetriedAndLockReleased() {
        IVoucherOrderService orders = mock(IVoucherOrderService.class);
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(redisson.getLock("lock:order:2")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        VoucherOrder order = new VoucherOrder().setId(1L).setUserId(2L).setVoucherId(3L);
        doThrow(new IllegalStateException("DB down")).when(orders).createVoucherOrder(order);
        OrderStreamConsumer consumer = new OrderStreamConsumer(mock(StringRedisTemplate.class), orders, redisson);
        assertThrows(IllegalStateException.class, () -> consumer.handle(order));
        verify(lock).unlock();
        consumer.stop();
    }

    @Test void uncommittedOrderWithBusyLockMustRemainPending() {
        IVoucherOrderService orders = mock(IVoucherOrderService.class);
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(redisson.getLock("lock:order:2")).thenReturn(lock);
        OrderStreamConsumer consumer = new OrderStreamConsumer(mock(StringRedisTemplate.class), orders, redisson);
        assertThrows(IllegalStateException.class, () -> consumer.handle(new VoucherOrder().setUserId(2L).setVoucherId(3L)));
        verify(orders, never()).createVoucherOrder(any());
        verify(lock, never()).unlock();
        consumer.stop();
    }

    @Test void committedDuplicateCanCompleteWhileAnotherWorkerOwnsLock() {
        IVoucherOrderService orders = mock(IVoucherOrderService.class);
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(redisson.getLock("lock:order:2")).thenReturn(lock);
        when(orders.count(any(Wrapper.class))).thenReturn(1L);
        OrderStreamConsumer consumer = new OrderStreamConsumer(mock(StringRedisTemplate.class), orders, redisson);
        assertDoesNotThrow(() -> consumer.handle(new VoucherOrder().setUserId(2L).setVoucherId(3L)));
        verify(orders, never()).createVoucherOrder(any());
        verify(lock, never()).unlock();
        consumer.stop();
    }
}
