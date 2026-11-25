package com.lab.stock.strategy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lab.stock.config.ConditionalOnKafkaEnabled;
import com.lab.stock.config.StockProperties;
import com.lab.stock.consumer.OrderMessage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 5. kafka. orders 에 PENDING 저장(커밋) → Kafka publish(key=productId) → PENDING 반환(API 는 202).
 * 같은 productId 는 같은 파티션이라 Consumer 가 순차 처리하므로 락이 필요 없다.
 * 이 클래스에는 @Transactional 이 없다: 커밋 후에 publish 한다.
 */
@Component
@ConditionalOnKafkaEnabled
public class KafkaStrategy implements StockStrategy {

    private final KafkaOrderService orderService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaStrategy(KafkaOrderService orderService, KafkaTemplate<String, String> kafkaTemplate,
                         ObjectMapper objectMapper, StockProperties properties) {
        this.orderService = orderService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = properties.kafka().topic();
    }

    @Override
    public OrderResult order(Long productId, Long userId) {
        Long orderId = orderService.createPending(productId, userId); // 중복이면 여기서 UNIQUE 위반
        OrderMessage message = new OrderMessage(orderId, productId, userId, System.currentTimeMillis());
        try {
            // 브로커 ack 까지 기다린다: 접수(202)했는데 메시지가 유실되는 일을 막기 위함 (decisions.md D-08)
            kafkaTemplate.send(topic, String.valueOf(productId), objectMapper.writeValueAsString(message))
                    .get(5, TimeUnit.SECONDS);
        } catch (JsonProcessingException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Kafka publish 실패. orderId=" + orderId, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka publish 중 인터럽트. orderId=" + orderId, e);
        }
        return OrderResult.pending(orderId);
    }

    @Override
    public String name() {
        return "kafka";
    }
}
