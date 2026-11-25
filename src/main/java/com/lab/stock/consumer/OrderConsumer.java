package com.lab.stock.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lab.stock.config.ConditionalOnKafkaEnabled;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnKafkaEnabled
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderProcessor processor;
    private final ObjectMapper objectMapper;
    private final Timer endToEndTimer;

    public OrderConsumer(OrderProcessor processor, ObjectMapper objectMapper, MeterRegistry registry) {
        this.processor = processor;
        this.objectMapper = objectMapper;
        this.endToEndTimer = Timer.builder("stock.kafka.e2e")
                .description("kafka 전략의 end-to-end 시간 (요청 접수 → 상태 확정)")
                .publishPercentileHistogram()
                .register(registry);
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${app.kafka.group-id}",
            concurrency = "${app.kafka.concurrency}")
    public void consume(String payload) throws JsonProcessingException {
        OrderMessage message = objectMapper.readValue(payload, OrderMessage.class);
        if (processor.process(message)) {
            long elapsed = System.currentTimeMillis() - message.requestedAtMs();
            endToEndTimer.record(Duration.ofMillis(Math.max(elapsed, 0)));
        } else {
            log.debug("skip orderId={}", message.orderId());
        }
    }
}
