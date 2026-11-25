package com.lab.stock.domain;

import jakarta.persistence.*;

/** 같은 product 테이블을 @Version 으로 매핑한 엔티티. optimistic 전략 전용. */
@Entity
@Table(name = "product")
public class VersionedProduct {

    @Id
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private int stock;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    protected VersionedProduct() {}

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
    public int getStock() { return stock; }
    public Long getVersion() { return version; }
}
