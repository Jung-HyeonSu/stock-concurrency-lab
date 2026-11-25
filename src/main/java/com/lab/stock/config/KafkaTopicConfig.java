package com.lab.stock.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConditionalOnKafkaEnabled
public class KafkaTopicConfig {

    /** 파티션 여러 개. 같은 productId(key)는 같은 파티션으로 가므로 한 상품의 주문은 순차 처리된다. */
    @Bean
    public NewTopic ordersTopic(StockProperties properties) {
        return TopicBuilder.name(properties.kafka().topic())
                .partitions(properties.kafka().partitions())
                .replicas(1)
                .build();
    }
}
