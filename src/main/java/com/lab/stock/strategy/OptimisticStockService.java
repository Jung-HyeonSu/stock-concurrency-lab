package com.lab.stock.strategy;

import com.lab.stock.domain.*;
import com.lab.stock.strategy.StockExceptions.ProductNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 낙관적 락 1회 시도 단위. 충돌 시 OptimisticLockingFailureException 이 이 트랜잭션 밖으로 전파되고,
 * 재시도는 반드시 트랜잭션 바깥의 {@code OptimisticFacade} 가 새 트랜잭션으로 수행한다.
 * (같은 트랜잭션 안에서 재시도하면 이미 오염된 영속성 컨텍스트를 재사용하게 된다)
 */
@Service
public class OptimisticStockService {

    private final VersionedProductRepository productRepository;
    private final OrderRepository orderRepository;

    public OptimisticStockService(VersionedProductRepository productRepository, OrderRepository orderRepository) {
        this.productRepository = productRepository;
        this.orderRepository = orderRepository;
    }

    @Transactional
    public OrderResult tryOrder(Long productId, Long userId) {
        VersionedProduct product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));
        if (!product.hasStock()) {
            return OrderResult.soldOut();
        }
        product.decrease();
        // flush 시점에 UPDATE ... WHERE version = ? 로 충돌이 감지된다
        productRepository.flush();
        Order order = orderRepository.saveAndFlush(Order.success(productId, userId));
        return OrderResult.success(order.getId());
    }
}
