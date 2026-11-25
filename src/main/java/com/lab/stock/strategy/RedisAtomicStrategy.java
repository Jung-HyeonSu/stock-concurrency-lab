package com.lab.stock.strategy;

import com.lab.stock.config.ConditionalOnRedisEnabled;
import com.lab.stock.config.RedisStockOps;
import com.lab.stock.strategy.StockExceptions.StockNotInitializedException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

/**
 * 4. redis-atomic (락 없는 방식). Lua 한 번으로 [중복 체크 → 재고 확인 → DECR] 을 원자 처리하고,
 * 성공한 요청만 DB 에 저장한다. DB 저장이 실패하면 Redis 재고를 복구(INCR + SREM)한다.
 * 이 클래스에는 @Transactional 이 없다. 보상은 DB 트랜잭션이 끝난 뒤에 수행해야 하기 때문이다.
 */
@Component
@ConditionalOnRedisEnabled
public class RedisAtomicStrategy implements StockStrategy {

    private final RedisStockOps redisOps;
    private final RedisAtomicOrderService orderService;

    public RedisAtomicStrategy(RedisStockOps redisOps, RedisAtomicOrderService orderService) {
        this.redisOps = redisOps;
        this.orderService = orderService;
    }

    @Override
    public OrderResult order(Long productId, Long userId) {
        int result = redisOps.acquire(productId, userId);
        if (result == RedisStockOps.NOT_INITIALIZED) {
            throw new StockNotInitializedException(productId);
        }
        if (result == RedisStockOps.DUPLICATE) {
            throw new DataIntegrityViolationException(
                    "이미 주문한 사용자입니다 (Redis SADD). productId=" + productId + ", userId=" + userId);
        }
        if (result == RedisStockOps.SOLD_OUT) {
            return OrderResult.soldOut();
        }
        try {
            return OrderResult.success(orderService.persist(productId, userId));
        } catch (RuntimeException e) {
            redisOps.compensate(productId, userId);
            throw e;
        }
    }

    @Override
    public String name() {
        return "redis-atomic";
    }
}
