package com.lab.stock.domain;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/** UNIQUE(product_id, user_id) 가 중복 주문의 최후 방어선이다 (schema.sql). */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    protected Order() {}

    public Order(Long productId, Long userId, OrderStatus status) {
        this.productId = productId;
        this.userId = userId;
        this.status = status;
        this.createdAt = LocalDateTime.now();
    }

    public static Order success(Long productId, Long userId) {
        return new Order(productId, userId, OrderStatus.SUCCESS);
    }

    public static Order pending(Long productId, Long userId) {
        return new Order(productId, userId, OrderStatus.PENDING);
    }

    public void markSuccess() { this.status = OrderStatus.SUCCESS; }
    public void markSoldOut() { this.status = OrderStatus.SOLD_OUT; }

    public Long getId() { return id; }
    public Long getProductId() { return productId; }
    public Long getUserId() { return userId; }
    public OrderStatus getStatus() { return status; }
    public LocalDateTime getCreatedAt() { return createdAt; }
}
