package com.lab.stock.api;

import com.lab.stock.domain.OrderStatus;

public record OrderResponse(Long orderId, OrderStatus status, String strategy) {}
