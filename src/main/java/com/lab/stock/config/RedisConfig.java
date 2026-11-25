package com.lab.stock.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnRedisEnabled
public class RedisConfig {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(StockProperties properties) {
        StockProperties.Redis redis = properties.redis();
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + redis.host() + ":" + redis.port())
                .setConnectionPoolSize(redis.connectionPoolSize())
                .setConnectionMinimumIdleSize(Math.min(10, redis.connectionPoolSize()));
        return Redisson.create(config);
    }
}
