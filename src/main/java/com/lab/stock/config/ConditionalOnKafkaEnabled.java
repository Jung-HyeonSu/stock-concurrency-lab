package com.lab.stock.config;

import java.lang.annotation.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** app.kafka.enabled=true(기본)일 때만 Bean 을 등록한다. */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public @interface ConditionalOnKafkaEnabled {
}
