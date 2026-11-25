package com.lab.stock.config;

import java.lang.annotation.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** app.redis.enabled=true(기본)일 때만 Bean 을 등록한다. Redis 없는 H2 테스트에서 끄기 위함. */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ConditionalOnProperty(prefix = "app.redis", name = "enabled", havingValue = "true", matchIfMissing = true)
public @interface ConditionalOnRedisEnabled {
}
