package com.lab.stock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.lab.stock.config.RedisStockOps;
import com.lab.stock.domain.Order;
import com.lab.stock.domain.OrderStatus;
import com.lab.stock.support.ConcurrencyTestSupport;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * CLAUDE.md 명세 그대로의 통합 테스트: MySQL 8 + Redis 7 (+ Kafka) 컨테이너 위에서 모든 전략을 같은 조건으로 검증한다.
 * Docker 가 없으면 건너뛴다 (docs/decisions.md D-05).
 */
@SpringBootTest(properties = {
        "app.simulation.no-lock-think-time-ms=5",
        "app.simulation.redisson-wrong-post-unlock-delay-ms=5"
})
@Testcontainers(disabledWithoutDocker = true)
class ContainerStrategyConsistencyTest extends ConcurrencyTestSupport {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7").withExposedPorts(6379);

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.0");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("app.redis.host", REDIS::getHost);
        registry.add("app.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired RedisStockOps redisOps;

    @ParameterizedTest(name = "[{0}] 재고 100, 사용자 1000명 동시 요청이면 정합성이 유지된다")
    @ValueSource(strings = {"pessimistic", "optimistic", "redisson", "redis-atomic"})
    void 정합성이_유지된다(String strategy) throws Exception {
        resetStock(STOCK);

        Outcome outcome = runConcurrently(strategy, distinctUsers(USERS));

        assertThat(outcome.error()).as("에러 응답 없음").isZero();
        assertThat(currentStock()).isZero();
        assertThat(countOrders(OrderStatus.SUCCESS)).isEqualTo(STOCK);
        assertThat(outcome.success()).isEqualTo(STOCK);
        assertConsistent(STOCK);
    }

    @ParameterizedTest(name = "[{0}] 같은 사용자가 동시에 50번 요청해도 한 번만 구매된다")
    @ValueSource(strings = {"pessimistic", "optimistic", "redisson", "redis-atomic"})
    void 동일_사용자_중복_요청은_한_번만_성공한다(String strategy) throws Exception {
        resetStock(STOCK);
        List<Long> sameUser = Collections.nCopies(50, 7L);

        Outcome outcome = runConcurrently(strategy, sameUser);

        assertThat(outcome.success()).isEqualTo(1);
        assertThat(outcome.duplicate()).isEqualTo(49);
        assertThat(countOrders(OrderStatus.SUCCESS)).isEqualTo(1);
        assertThat(currentStock()).isEqualTo(STOCK - 1);
    }

    @Test
    void no_lock_은_정합성이_깨진다() throws Exception {
        resetStock(STOCK);

        runConcurrently("no-lock", distinctUsers(USERS));

        assertThat(isInconsistent(STOCK))
                .as("재고=%d, 성공 주문=%d", currentStock(), countOrders(OrderStatus.SUCCESS))
                .isTrue();
    }

    @Test
    void redisson_wrong_은_커밋_전에_락이_풀려_정합성이_깨진다() throws Exception {
        resetStock(STOCK);

        runConcurrently("redisson-wrong", distinctUsers(USERS));

        assertThat(isInconsistent(STOCK))
                .as("재고=%d, 성공 주문=%d", currentStock(), countOrders(OrderStatus.SUCCESS))
                .isTrue();
    }

    @Test
    void redis_atomic_은_DB_저장이_실패하면_Redis_재고와_구매자를_복구한다() {
        resetStock(STOCK);
        // Redis 는 모르는 주문이 DB 에만 있는 상태를 만들어 DB 저장을 UNIQUE 위반으로 실패시킨다
        orderRepository.saveAndFlush(Order.success(PRODUCT_ID, 9L));

        assertThatThrownBy(() -> registry.get("redis-atomic").order(PRODUCT_ID, 9L))
                .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(redisOps.remainingStock(PRODUCT_ID)).as("INCR 로 재고 복구").isEqualTo(STOCK);
        assertThat(redisOps.buyerCount(PRODUCT_ID)).as("SREM 으로 구매자 제거").isZero();
    }

    @Test
    void kafka_는_Consumer_처리가_끝난_뒤_정합성이_유지된다() throws Exception {
        resetStock(STOCK);

        Outcome outcome = runConcurrently("kafka", distinctUsers(USERS));
        await().atMost(Duration.ofSeconds(120)).untilAsserted(
                () -> assertThat(countOrders(OrderStatus.PENDING)).isZero());

        assertThat(outcome.pending()).isEqualTo(USERS);
        assertThat(currentStock()).isZero();
        assertThat(countOrders(OrderStatus.SUCCESS)).isEqualTo(STOCK);
        assertThat(countOrders(OrderStatus.SOLD_OUT)).isEqualTo(USERS - STOCK);
        assertConsistent(STOCK);
    }
}
