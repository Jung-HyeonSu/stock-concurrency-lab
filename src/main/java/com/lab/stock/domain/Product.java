package com.lab.stock.domain;

import jakarta.persistence.*;

/**
 * 버전 컬럼을 매핑하지 않은 상품 엔티티. no-lock / pessimistic / redisson / kafka 전략이 사용한다.
 * (@Version 이 붙으면 no-lock 에서 Lost Update 대신 낙관적 락 예외가 나므로 분리했다. docs/decisions.md 참고)
 */
@Entity
@Table(name = "product")
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private int stock;

    protected Product() {}

    public Product(String name, int stock) {
        this.name = name;
        this.stock = stock;
    }

    public boolean hasStock() {
        return stock > 0;
    }

    public void decrease() {
        if (stock <= 0) {
            throw new IllegalStateException("재고가 없습니다. productId=" + id);
        }
        stock--;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public int getStock() { return stock; }
}
