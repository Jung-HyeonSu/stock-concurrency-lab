package com.lab.stock.strategy;

/** 전략/API 계층에서 쓰는 비즈니스 예외 모음. HTTP 상태 매핑은 api.GlobalExceptionHandler 가 담당한다. */
public final class StockExceptions {

    private StockExceptions() {}

    public static class ProductNotFoundException extends RuntimeException {
        public ProductNotFoundException(Long productId) {
            super("상품을 찾을 수 없습니다. productId=" + productId);
        }
    }

    public static class UnknownStrategyException extends RuntimeException {
        public UnknownStrategyException(String name) {
            super("알 수 없는 전략입니다: " + name);
        }
    }

    /** 분산 락 획득 실패 (waitTime 초과). */
    public static class LockAcquisitionException extends RuntimeException {
        public LockAcquisitionException(String key) {
            super("락 획득에 실패했습니다: " + key);
        }
    }

    /** 낙관적 락 재시도 횟수 초과. */
    public static class RetryExhaustedException extends RuntimeException {
        public RetryExhaustedException(int attempts, Throwable cause) {
            super("낙관적 락 재시도 횟수를 초과했습니다. attempts=" + attempts, cause);
        }
    }

    /** Redis 재고가 초기화되지 않은 상품. */
    public static class StockNotInitializedException extends RuntimeException {
        public StockNotInitializedException(Long productId) {
            super("Redis 재고가 초기화되지 않았습니다. productId=" + productId);
        }
    }
}
