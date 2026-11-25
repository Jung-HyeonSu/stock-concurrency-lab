package com.lab.stock;

import static org.assertj.core.api.Assertions.assertThat;

import com.lab.stock.domain.OrderStatus;
import com.lab.stock.support.ConcurrencyTestSupport;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Docker 없이 H2 로 도는 정합성 테스트. DB 만 쓰는 전략(no-lock, pessimistic, optimistic)을 검증한다.
 * Redis/Kafka 가 필요한 전략은 Testcontainers 기반 {@code ContainerStrategyConsistencyTest} 가 담당한다.
 */
@SpringBootTest(properties = "app.simulation.no-lock-think-time-ms=5")
@ActiveProfiles("h2")
class H2StrategyConsistencyTest extends ConcurrencyTestSupport {

    @ParameterizedTest(name = "[{0}] 재고 100, 사용자 1000명 동시 요청이면 정합성이 유지된다")
    @ValueSource(strings = {"pessimistic", "optimistic"})
    void 정합성이_유지된다(String strategy) throws Exception {
        resetStock(STOCK);

        Outcome outcome = runConcurrently(strategy, distinctUsers(USERS));

        assertThat(outcome.error()).as("에러 응답 없음").isZero();
        assertThat(currentStock()).isZero();
        assertThat(countOrders(OrderStatus.SUCCESS)).isEqualTo(STOCK);
        assertThat(outcome.success()).isEqualTo(STOCK);
        assertThat(outcome.soldOut()).isEqualTo(USERS - STOCK);
        assertConsistent(STOCK);
    }

    @ParameterizedTest(name = "[{0}] 같은 사용자가 동시에 50번 요청해도 한 번만 구매된다")
    @ValueSource(strings = {"pessimistic", "optimistic"})
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
    void no_lock_은_Lost_Update_로_정합성이_깨진다() throws Exception {
        resetStock(STOCK);

        runConcurrently("no-lock", distinctUsers(USERS));

        // 이 테스트는 "깨짐"을 검증한다. 재고 차감 수와 성공 주문 수가 어긋나거나 초과 판매가 발생한다.
        assertThat(isInconsistent(STOCK))
                .as("재고=%d, 성공 주문=%d", currentStock(), countOrders(OrderStatus.SUCCESS))
                .isTrue();
    }
}
