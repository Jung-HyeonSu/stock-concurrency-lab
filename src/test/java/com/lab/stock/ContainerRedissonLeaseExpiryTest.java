package com.lab.stock;

import static org.assertj.core.api.Assertions.assertThat;

import com.lab.stock.support.ConcurrencyTestSupport;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 올바른 redisson 전략(락 → TX → 커밋 → 해제)도 "트랜잭션이 leaseTime 보다 오래 걸리면" 깨진다는 것을 재현한다.
 * leaseTime 100ms, 트랜잭션 지연 400ms 로 만료를 강제한다. 락이 풀린 뒤 다른 요청이 같은 임계 구역에 들어와
 * Lost Update 가 발생하고, 이는 stock.redisson.lease.expired Counter 로도 관측된다.
 */
@SpringBootTest(properties = {
        "app.kafka.enabled=false",
        "app.redisson.lease-time-ms=100",
        "app.redisson.wait-time-ms=60000",
        "app.simulation.redisson-tx-delay-ms=400"
})
@Testcontainers(disabledWithoutDocker = true)
class ContainerRedissonLeaseExpiryTest extends ConcurrencyTestSupport {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7").withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("app.redis.host", REDIS::getHost);
        registry.add("app.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired MeterRegistry meterRegistry;

    @Test
    void 락_leaseTime_이_트랜잭션보다_짧으면_redisson_도_정합성이_깨진다() throws Exception {
        int stock = 10;
        resetStock(stock);
        double before = meterRegistry.counter("stock.redisson.lease.expired").count();

        runConcurrently("redisson", distinctUsers(40));

        double expired = meterRegistry.counter("stock.redisson.lease.expired").count() - before;
        assertThat(expired).as("락 만료가 관측되어야 한다").isGreaterThan(0);
        assertThat(isInconsistent(stock))
                .as("재고=%d, 성공 주문=%d, 락 만료=%.0f", currentStock(), countOrders(com.lab.stock.domain.OrderStatus.SUCCESS), expired)
                .isTrue();
    }
}
