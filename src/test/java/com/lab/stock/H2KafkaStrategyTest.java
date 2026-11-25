package com.lab.stock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lab.stock.consumer.OrderMessage;
import com.lab.stock.consumer.OrderProcessor;
import com.lab.stock.domain.OrderStatus;
import com.lab.stock.support.ConcurrencyTestSupport;
import java.time.Duration;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/** 임베디드 Kafka(KRaft) + H2 로 Docker 없이 kafka 전략을 검증한다. */
@SpringBootTest(properties = "app.kafka.enabled=true")
@ActiveProfiles("h2")
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 6, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class H2KafkaStrategyTest extends ConcurrencyTestSupport {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;
    @Autowired OrderProcessor processor;

    private void awaitAllProcessed() {
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(countOrders(OrderStatus.PENDING)).isZero());
    }

    @Test
    void 재고_100_사용자_1000명_동시_요청이면_Consumer_처리_후_정합성이_유지된다() throws Exception {
        resetStock(STOCK);

        Outcome outcome = runConcurrently("kafka", distinctUsers(USERS));
        awaitAllProcessed();

        assertThat(outcome.error()).isZero();
        assertThat(outcome.pending()).as("API 는 모두 접수(PENDING)로 응답").isEqualTo(USERS);
        assertThat(currentStock()).isZero();
        assertThat(countOrders(OrderStatus.SUCCESS)).isEqualTo(STOCK);
        assertThat(countOrders(OrderStatus.SOLD_OUT)).isEqualTo(USERS - STOCK);
        assertConsistent(STOCK);
    }

    @Test
    void 동일_사용자_중복_요청은_한_번만_접수된다() throws Exception {
        resetStock(STOCK);

        Outcome outcome = runConcurrently("kafka", Collections.nCopies(50, 7L));
        awaitAllProcessed();

        assertThat(outcome.pending()).isEqualTo(1);
        assertThat(outcome.duplicate()).isEqualTo(49);
        assertThat(countOrders(OrderStatus.SUCCESS)).isEqualTo(1);
        assertThat(currentStock()).isEqualTo(STOCK - 1);
    }

    @Test
    void 이미_처리된_주문의_메시지가_다시_와도_건너뛴다() throws Exception {
        resetStock(STOCK);
        runConcurrently("kafka", Collections.singletonList(1L));
        awaitAllProcessed();
        Long orderId = orderRepository.findAll().get(0).getId();

        boolean processed = processor.process(new OrderMessage(orderId, PRODUCT_ID, 1L, System.currentTimeMillis()));

        assertThat(processed).as("멱등성: status != PENDING 이면 skip").isFalse();
        assertThat(currentStock()).isEqualTo(STOCK - 1);
    }

    @Test
    void 주문과_내용이_다른_메시지는_건너뛴다() throws Exception {
        resetStock(STOCK);
        Long orderId = orderRepository.saveAndFlush(com.lab.stock.domain.Order.pending(PRODUCT_ID, 5L)).getId();

        // 같은 orderId 지만 userId 가 다른 잔여 메시지 (DB 초기화 후 id 재사용 상황)
        boolean processed = processor.process(new OrderMessage(orderId, PRODUCT_ID, 999L, System.currentTimeMillis()));

        assertThat(processed).isFalse();
        assertThat(countOrders(OrderStatus.PENDING)).isEqualTo(1);
        assertThat(currentStock()).isEqualTo(STOCK);
    }

    @Test
    void API_는_202_와_orderId_를_반환하고_조회_API_로_최종_상태를_확인할_수_있다() throws Exception {
        resetStock(STOCK);

        String body = mockMvc.perform(post("/api/orders").param("strategy", "kafka")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"productId\":1,\"userId\":42}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn().getResponse().getContentAsString();
        JsonNode json = objectMapper.readTree(body);
        long orderId = json.get("orderId").asLong();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/api/orders/" + orderId))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("SUCCESS")));
    }
}
