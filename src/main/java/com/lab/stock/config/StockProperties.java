package com.lab.stock.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 전략별 튜닝 값. 모든 값은 application.yml(app.*)에서 주입한다. */
@ConfigurationProperties(prefix = "app")
public record StockProperties(
        Optimistic optimistic,
        Redisson redisson,
        Redis redis,
        Kafka kafka,
        Simulation simulation) {

    public record Optimistic(int maxRetries, long baseBackoffMs, long maxBackoffMs) {}

    public record Redisson(long waitTimeMs, long leaseTimeMs) {}

    public record Redis(boolean enabled, String host, int port, int connectionPoolSize, boolean loadOnStartup) {}

    public record Kafka(boolean enabled, String topic, int partitions, int concurrency, String groupId) {}

    /** 경합 구간을 넓혀 정합성 붕괴를 재현하기 위한 인위적 지연. 운영/벤치마크에서는 0. */
    public record Simulation(long noLockThinkTimeMs, long redissonWrongPostUnlockDelayMs) {}
}
