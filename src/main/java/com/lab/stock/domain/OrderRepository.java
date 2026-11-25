package com.lab.stock.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
    long countByProductIdAndStatus(Long productId, OrderStatus status);
    long countByProductId(Long productId);
}
