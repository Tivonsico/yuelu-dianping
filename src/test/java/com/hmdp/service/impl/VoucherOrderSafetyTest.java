package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class VoucherOrderSafetyTest {
    private VoucherOrderServiceImpl orders;
    private ISeckillVoucherService stock;
    private VoucherOrder order;

    @BeforeEach
    void setUp() {
        orders = spy(new VoucherOrderServiceImpl());
        stock = mock(ISeckillVoucherService.class);
        ReflectionTestUtils.setField(orders, "seckillVoucherService", stock);
        order = new VoucherOrder().setId(100L).setUserId(5L).setVoucherId(8L);
        doReturn(0L).when(orders).count(any(Wrapper.class));
    }

    @Test
    void duplicateDeliveryDoesNotDecrementOrInsert() {
        doReturn(1L).when(orders).count(any(Wrapper.class));
        orders.createVoucherOrder(order);
        verifyNoInteractions(stock);
        verify(orders, never()).save(any());
    }

    @Test
    void failedConditionalStockUpdateMustNeverCreateOrder() {
        when(stock.update(any(Wrapper.class))).thenReturn(false);
        doReturn(true).when(orders).save(any());
        assertThrows(IllegalStateException.class, () -> orders.createVoucherOrder(order));
        verify(orders, never()).save(any());
    }

    @Test
    void failedInsertMustThrowSoTransactionRollsBack() {
        when(stock.update(any(Wrapper.class))).thenReturn(true);
        doReturn(false).when(orders).save(any());
        assertThrows(IllegalStateException.class, () -> orders.createVoucherOrder(order));
    }

    @Test
    void availableStockCreatesOneOrder() {
        when(stock.update(any(Wrapper.class))).thenReturn(true);
        doReturn(true).when(orders).save(any());
        orders.createVoucherOrder(order);
        verify(stock).update(any(Wrapper.class));
        verify(orders).save(order);
    }
}
