package com.lab.stock.facade;

import com.lab.stock.config.StockProperties;
import com.lab.stock.strategy.OptimisticStockService;
import com.lab.stock.strategy.OrderResult;
import com.lab.stock.strategy.StockExceptions.RetryExhaustedException;
import com.lab.stock.strategy.StockStrategy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;

/**
 * 2. optimistic. 트랜잭션 바깥에서 재시도한다. 지수 백오프 + full jitter, 최대 횟수는 설정값.
 * 이 클래스에는 @Transactional 이 없다 (있으면 재시도가 같은 트랜잭션에 갇힌다).
 */
@Component
public class OptimisticFacade implements StockStrategy {

    private final OptimisticStockService stockService;
    private final StockProperties.Optimistic props;
    private final Counter retryCounter;

    public OptimisticFacade(OptimisticStockService stockService, StockProperties properties, MeterRegistry registry) {
        this.stockService = stockService;
        this.props = properties.optimistic();
        this.retryCounter = Counter.builder("stock.optimistic.retry")
                .description("낙관적 락 충돌로 인한 재시도 횟수")
                .register(registry);
    }

    @Override
    public OrderResult order(Long productId, Long userId) {
        int attempt = 0;
        while (true) {
            try {
                return stockService.tryOrder(productId, userId);
            } catch (OptimisticLockingFailureException e) {
                if (attempt >= props.maxRetries()) {
                    throw new RetryExhaustedException(attempt + 1, e);
                }
                retryCounter.increment();
                sleep(backoffMs(attempt));
                attempt++;
            }
        }
    }

    /** full jitter: [0, min(max, base * 2^attempt)] 균등 분포. */
    long backoffMs(int attempt) {
        long ceiling = Math.min(props.maxBackoffMs(), props.baseBackoffMs() * (1L << Math.min(attempt, 20)));
        return ceiling <= 0 ? 0 : ThreadLocalRandom.current().nextLong(ceiling + 1);
    }

    private void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryExhaustedException(0, e);
        }
    }

    @Override
    public String name() {
        return "optimistic";
    }
}
