package com.lab.stock.api;

import com.lab.stock.domain.Order;
import com.lab.stock.domain.OrderStatus;

public record OrderStatusResponse(Long orderId, Long productId, Long userId, OrderStatus status) {

    public static OrderStatusResponse from(Order order) {
        return new OrderStatusResponse(order.getId(), order.getProductId(), order.getUserId(), order.getStatus());
    }
}
