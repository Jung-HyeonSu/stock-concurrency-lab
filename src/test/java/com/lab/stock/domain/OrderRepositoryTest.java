package com.lab.stock.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;

/** schema.sql 과 엔티티 매핑, 유니크 제약(최후 방어선)을 H2 로 빠르게 검증한다. */
@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=none")
@AutoConfigureTestDatabase
class OrderRepositoryTest {

    @Autowired ProductRepository productRepository;
    @Autowired VersionedProductRepository versionedProductRepository;
    @Autowired OrderRepository orderRepository;

    @Test
    void 같은_상품에_같은_사용자의_주문은_유니크_제약으로_거부된다() {
        orderRepository.saveAndFlush(Order.success(1L, 10L));

        assertThatThrownBy(() -> orderRepository.saveAndFlush(Order.success(1L, 10L)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void 다른_상품이면_같은_사용자도_주문할_수_있다() {
        orderRepository.saveAndFlush(Order.success(1L, 10L));
        orderRepository.saveAndFlush(Order.success(2L, 10L));

        assertThat(orderRepository.count()).isEqualTo(2);
    }

    @Test
    void 같은_테이블을_두_엔티티로_매핑해_읽을_수_있다() {
        Product saved = productRepository.saveAndFlush(new Product("p", 5));

        VersionedProduct versioned = versionedProductRepository.findById(saved.getId()).orElseThrow();

        assertThat(versioned.getStock()).isEqualTo(5);
        assertThat(versioned.getVersion()).isZero();
    }
}
