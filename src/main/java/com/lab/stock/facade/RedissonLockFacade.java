package com.lab.stock.facade;

import com.lab.stock.config.ConditionalOnRedisEnabled;
import com.lab.stock.config.StockProperties;
import com.lab.stock.strategy.OrderResult;
import com.lab.stock.strategy.RedissonStockService;
import com.lab.stock.strategy.StockExceptions.LockAcquisitionException;
import com.lab.stock.strategy.StockStrategy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

/**
 * 3. redisson. 순서: 락 획득 → 트랜잭션 시작 → 커밋 → 락 해제.
 * Facade 에는 @Transactional 이 없고, 트랜잭션은 RedissonStockService 의 메서드 경계에서 열리고 닫힌다.
 */
@Component
@ConditionalOnRedisEnabled
public class RedissonLockFacade implements StockStrategy {

    private static final Logger log = LoggerFactory.getLogger(RedissonLockFacade.class);

    private final RedissonClient redisson;
    private final Counter leaseExpired;
    private final RedissonStockService stockService;
    private final StockProperties.Redisson props;

    public RedissonLockFacade(RedissonClient redisson, RedissonStockService stockService, StockProperties properties,
                              MeterRegistry registry) {
        this.leaseExpired = Counter.builder("stock.redisson.lease.expired")
                .description("트랜잭션이 끝나기 전에 락 leaseTime 이 만료된 횟수 (정합성 위험 신호)")
                .register(registry);
        this.redisson = redisson;
        this.stockService = stockService;
        this.props = properties.redisson();
    }

    @Override
    public OrderResult order(Long productId, Long userId) {
        String key = "lock:product:" + productId;
        RLock lock = redisson.getLock(key);
        boolean acquired;
        try {
            acquired = lock.tryLock(props.waitTimeMs(), props.leaseTimeMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockAcquisitionException(key);
        }
        if (!acquired) {
            throw new LockAcquisitionException(key);
        }
        try {
            return stockService.order(productId, userId); // 이 호출이 반환될 때 이미 커밋 완료
        } finally {
            // leaseTime 이 지나 락이 만료됐다면 unlock() 이 예외를 던지므로 보유 여부를 확인한다
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            } else {
                // 락이 이미 만료됐다면 다른 요청이 동시에 임계 구역에 들어왔을 수 있다
                leaseExpired.increment();
                log.warn("락 leaseTime 이 트랜잭션 종료 전에 만료됐습니다. key={}", key);
            }
        }
    }

    @Override
    public String name() {
        return "redisson";
    }
}
