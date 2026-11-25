package com.lab.stock.config;

/** Redis 재고 캐시 초기화 훅. Redis 가 비활성이면 구현 Bean 이 없어 AdminService 가 건너뛴다. */
public interface RedisStockInitializer {

    /** Redis 의 재고/구매자 키를 모두 지우고 DB 의 현재 재고로 다시 적재한다. */
    void resetAndLoadFromDb();
}
