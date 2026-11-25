package com.lab.stock.consumer;

/** Kafka 로 흘려보내는 주문 접수 메시지 (JSON 문자열로 직렬화). requestedAtMs 는 end-to-end 시간 측정용. */
public record OrderMessage(Long orderId, Long productId, Long userId, long requestedAtMs) {}
