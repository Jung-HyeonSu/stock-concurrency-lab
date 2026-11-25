package com.lab.stock.strategy;

import com.lab.stock.domain.OrderStatus;

/**
 * @param orderId 저장된 주문 id. 동기 전략의 SOLD_OUT 은 주문을 저장하지 않으므로 null.
 * @param status  SUCCESS / SOLD_OUT, kafka 전략은 PENDING
 */
public record OrderResult(Long orderId, OrderStatus status) {

    public static OrderResult success(Long orderId) {
        return new OrderResult(orderId, OrderStatus.SUCCESS);
    }

    public static OrderResult soldOut() {
        return new OrderResult(null, OrderStatus.SOLD_OUT);
    }

    public static OrderResult pending(Long orderId) {
        return new OrderResult(orderId, OrderStatus.PENDING);
    }
}
