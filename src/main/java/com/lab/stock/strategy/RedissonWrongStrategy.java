package com.lab.stock.strategy;

import com.lab.stock.config.ConditionalOnRedisEnabled;
import com.lab.stock.config.StockProperties;
import com.lab.stock.domain.*;
import com.lab.stock.strategy.StockExceptions.LockAcquisitionException;
import com.lab.stock.strategy.StockExceptions.ProductNotFoundException;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 비교용 버그 재현: @Transactional 메서드 안에서 락을 해제한다.
 * 락이 풀리는 시점은 메서드 return 직전이지만 커밋은 메서드가 끝난 뒤(프록시)에 일어난다.
 * 그 사이 다른 요청이 락을 얻어 커밋 전의 옛 재고를 읽으므로 Lost Update / 초과 판매가 발생한다.
 * 절대 이렇게 구현하면 안 된다. 올바른 구현은 RedissonLockFacade.
 */
@Component
@ConditionalOnRedisEnabled
public class RedissonWrongStrategy implements StockStrategy {

    private final RedissonClient redisson;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final StockProperties.Redisson props;
    private final long postUnlockDelayMs;

    public RedissonWrongStrategy(RedissonClient redisson, ProductRepository productRepository,
                                 OrderRepository orderRepository, StockProperties properties) {
        this.redisson = redisson;
        this.productRepository = productRepository;
        this.orderRepository = orderRepository;
        this.props = properties.redisson();
        this.postUnlockDelayMs = properties.simulation().redissonWrongPostUnlockDelayMs();
    }

    @Override
    @Transactional
    public OrderResult order(Long productId, Long userId) {
        String key = "lock:product:" + productId;
        RLock lock = redisson.getLock(key);
        try {
            if (!lock.tryLock(props.waitTimeMs(), props.leaseTimeMs(), TimeUnit.MILLISECONDS)) {
                throw new LockAcquisitionException(key);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockAcquisitionException(key);
        }
        try {
            Product product = productRepository.findById(productId)
                    .orElseThrow(() -> new ProductNotFoundException(productId));
            if (!product.hasStock()) {
                return OrderResult.soldOut();
            }
            product.decrease();
        } finally {
            lock.unlock(); // 버그: 아직 커밋 전이다
        }
        NoLockStrategy.sleepQuietly(postUnlockDelayMs);
        Order order = orderRepository.saveAndFlush(Order.success(productId, userId));
        return OrderResult.success(order.getId());
    }

    @Override
    public String name() {
        return "redisson-wrong";
    }
}
