package com.lab.stock.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ProductTest {

    @Test
    void 재고를_하나씩_차감한다() {
        Product product = new Product("limited", 2);

        product.decrease();

        assertThat(product.getStock()).isEqualTo(1);
        assertThat(product.hasStock()).isTrue();
    }

    @Test
    void 재고가_없으면_차감할_수_없다() {
        Product product = new Product("limited", 0);

        assertThat(product.hasStock()).isFalse();
        assertThatThrownBy(product::decrease).isInstanceOf(IllegalStateException.class);
        assertThat(product.getStock()).isZero();
    }
}
