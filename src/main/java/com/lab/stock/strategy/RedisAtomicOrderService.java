package com.lab.stock.strategy;

import com.lab.stock.config.ConditionalOnRedisEnabled;
import com.lab.stock.domain.Order;
import com.lab.stock.domain.OrderRepository;
import com.lab.stock.domain.ProductRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** redis-atomic 의 DB 반영 단위. Redis 에서 구매권을 얻은 요청만 도달하므로 DB 경합이 재고 수만큼으로 줄어든다. */
@Service
@ConditionalOnRedisEnabled
public class RedisAtomicOrderService {

    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;

    public RedisAtomicOrderService(ProductRepository productRepository, OrderRepository orderRepository) {
        this.productRepository = productRepository;
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Long persist(Long productId, Long userId) {
        if (productRepository.decreaseStock(productId) == 0) {
            throw new IllegalStateException("DB 재고가 부족합니다. Redis 와 DB 재고가 어긋났습니다. productId=" + productId);
        }
        return orderRepository.saveAndFlush(Order.success(productId, userId)).getId();
    }
}
