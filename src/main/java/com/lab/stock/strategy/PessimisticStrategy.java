package com.lab.stock.strategy;

import com.lab.stock.domain.*;
import com.lab.stock.strategy.StockExceptions.ProductNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 1. pessimistic. SELECT ... FOR UPDATE 로 행 락을 잡고 차감한다 (락 대기 3000ms).
 * 락 대기 동안 DB 커넥션을 점유하므로 HikariCP 풀 크기가 처리량과 에러율을 좌우한다.
 */
@Component
public class PessimisticStrategy implements StockStrategy {

    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;

    public PessimisticStrategy(ProductRepository productRepository, OrderRepository orderRepository) {
        this.productRepository = productRepository;
        this.orderRepository = orderRepository;
    }

    @Override
    @Transactional
    public OrderResult order(Long productId, Long userId) {
        Product product = productRepository.findByIdForUpdate(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));
        if (!product.hasStock()) {
            return OrderResult.soldOut();
        }
        product.decrease();
        Order order = orderRepository.saveAndFlush(Order.success(productId, userId));
        return OrderResult.success(order.getId());
    }

    @Override
    public String name() {
        return "pessimistic";
    }
}
