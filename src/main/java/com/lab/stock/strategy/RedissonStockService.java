package com.lab.stock.strategy;

import com.lab.stock.config.ConditionalOnRedisEnabled;
import com.lab.stock.config.StockProperties;
import com.lab.stock.domain.*;
import com.lab.stock.strategy.StockExceptions.ProductNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 분산 락 안쪽에서 실행되는 트랜잭션 단위. 락은 이 메서드가 커밋된 뒤에 Facade 가 해제한다. */
@Service
@ConditionalOnRedisEnabled
public class RedissonStockService {

    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final long txDelayMs;

    public RedissonStockService(ProductRepository productRepository, OrderRepository orderRepository,
                                StockProperties properties) {
        this.txDelayMs = properties.simulation().redissonTxDelayMs();
        this.productRepository = productRepository;
        this.orderRepository = orderRepository;
    }

    @Transactional
    public OrderResult order(Long productId, Long userId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));
        if (!product.hasStock()) {
            return OrderResult.soldOut();
        }
        // 트랜잭션이 락 leaseTime 보다 오래 걸리는 상황(부하로 DB/커넥션 대기)을 재현하기 위한 지연. 기본 0
        NoLockStrategy.sleepQuietly(txDelayMs);
        product.decrease();
        Order order = orderRepository.saveAndFlush(Order.success(productId, userId));
        return OrderResult.success(order.getId());
    }
}
