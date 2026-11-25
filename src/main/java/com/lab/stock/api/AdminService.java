package com.lab.stock.api;

import com.lab.stock.config.RedisStockInitializer;
import com.lab.stock.config.RedisStockOps;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/** 벤치마크/테스트 회차마다 DB 와 Redis 를 같은 초기 상태로 되돌린다. */
@Service
public class AdminService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectProvider<RedisStockInitializer> redisInitializer;
    private final ObjectProvider<RedisStockOps> redisOps;

    public AdminService(JdbcTemplate jdbcTemplate, ObjectProvider<RedisStockInitializer> redisInitializer,
                        ObjectProvider<RedisStockOps> redisOps) {
        this.redisOps = redisOps;
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

    /**
     * 부하 실행 후 정합성 검증. 상품별로 (재고 >= 0) 와 (성공 주문 수 == 초기 재고 - 현재 재고) 를 확인한다.
     * kafka 전략은 pending 이 0 이 될 때까지 호출자가 폴링한다. 중복 사용자는 UNIQUE 제약이 막으므로 별도 검사하지 않는다.
     */
    public Map<String, Object> verify(int initialStock) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT p.id AS id, p.stock AS stock, "
                        + "(SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id AND o.status = 'SUCCESS') AS success "
                        + "FROM product p");
        int negative = 0;
        int mismatched = 0;
        int redisMismatched = 0;
        long successTotal = 0;
        RedisStockOps redis = redisOps.getIfAvailable();
        for (Map<String, Object> row : rows) {
            long id = ((Number) row.get("id")).longValue();
            int stock = ((Number) row.get("stock")).intValue();
            long success = ((Number) row.get("success")).longValue();
            successTotal += success;
            if (stock < 0) {
                negative++;
            }
            if (success != initialStock - stock) {
                mismatched++;
            }
            if (redis != null) {
                Integer redisStock = redis.remainingStock(id);
                if (redisStock == null || redisStock != stock) {
                    redisMismatched++;
                }
            }
        }
        Long pending = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders WHERE status = 'PENDING'", Long.class);
        Long soldOut = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders WHERE status = 'SOLD_OUT'", Long.class);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("products", rows.size());
        result.put("initialStock", initialStock);
        result.put("successOrders", successTotal);
        result.put("soldOutOrders", soldOut);
        result.put("pendingOrders", pending);
        result.put("negativeStockProducts", negative);
        result.put("mismatchedProducts", mismatched);
        result.put("redisChecked", redis != null);
        result.put("redisMismatchedProducts", redisMismatched);
        result.put("consistent", negative == 0 && mismatched == 0 && pending == 0);
        return result;
    }
}
