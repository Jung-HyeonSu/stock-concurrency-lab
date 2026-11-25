package com.lab.stock.domain;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, Long> {

    /** 비관적 락. 락 대기 타임아웃 3000ms (jakarta.persistence.lock.timeout). */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000"))
    @Query("select p from Product p where p.id = :id")
    Optional<Product> findByIdForUpdate(@Param("id") Long id);

    /** 조건부 원자 차감. redis-atomic 의 DB 반영에 사용. 영향 행 수 0 이면 DB 재고 부족. */
    @Modifying
    @Query("update Product p set p.stock = p.stock - 1 where p.id = :id and p.stock > 0")
    int decreaseStock(@Param("id") Long id);
}
