package com.lab.stock.api;

import com.lab.stock.config.RedisStockInitializer;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/** 벤치마크/테스트 회차마다 DB 와 Redis 를 같은 초기 상태로 되돌린다. */
@Service
public class AdminService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectProvider<RedisStockInitializer> redisInitializer;

    public AdminService(JdbcTemplate jdbcTemplate, ObjectProvider<RedisStockInitializer> redisInitializer) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisInitializer = redisInitializer;
    }

    /** 주문/상품 테이블을 비우고 id 1..products 상품을 재고 stock 으로 다시 만든다. */
    public void reset(int stock, int products) {
        jdbcTemplate.execute("TRUNCATE TABLE orders");
        jdbcTemplate.execute("TRUNCATE TABLE product");
        List<Object[]> rows = new ArrayList<>();
        for (long id = 1; id <= products; id++) {
            rows.add(new Object[]{id, "product-" + id, stock});
        }
        jdbcTemplate.batchUpdate("INSERT INTO product (id, name, stock, version) VALUES (?, ?, ?, 0)", rows);
        redisInitializer.ifAvailable(RedisStockInitializer::resetAndLoadFromDb);
    }
}
