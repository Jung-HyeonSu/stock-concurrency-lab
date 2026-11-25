package com.lab.stock.strategy;

import com.lab.stock.config.StockProperties;
import com.lab.stock.domain.*;
import com.lab.stock.strategy.StockExceptions.ProductNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 0. no-lock (베이스라인). findById → 재고 감소 → save.
 * 읽기와 쓰기 사이에 아무 보호가 없어 Lost Update 로 재고/주문이 어긋난다.
 */
@Component
public class NoLockStrategy implements StockStrategy {

    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final long thinkTimeMs;

    public NoLockStrategy(ProductRepository productRepository, OrderRepository orderRepository,
                          StockProperties properties) {
        this.productRepository = productRepository;
        this.orderRepository = orderRepository;
        this.thinkTimeMs = properties.simulation().noLockThinkTimeMs();
    }

    @Override
    @Transactional
    public OrderResult order(Long productId, Long userId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));
        if (!product.hasStock()) {
            return OrderResult.soldOut();
        }
        sleepQuietly(thinkTimeMs);
        product.decrease();
        productRepository.saveAndFlush(product);
        Order order = orderRepository.saveAndFlush(Order.success(productId, userId));
        return OrderResult.success(order.getId());
    }

    @Override
    public String name() {
        return "no-lock";
    }

    static void sleepQuietly(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
