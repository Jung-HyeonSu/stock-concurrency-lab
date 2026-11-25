package com.lab.stock.strategy;

/** 모든 동시성 제어 전략의 공통 계약. API 는 이름으로 구현체를 찾는다. */
public interface StockStrategy {

    OrderResult order(Long productId, Long userId);

    String name();
}
