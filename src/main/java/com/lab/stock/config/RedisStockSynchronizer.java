package com.lab.stock.config;

import com.lab.stock.domain.Order;
import com.lab.stock.domain.OrderRepository;
import com.lab.stock.domain.OrderStatus;
import com.lab.stock.domain.Product;
import com.lab.stock.domain.ProductRepository;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * DB 를 기준(source of truth)으로 Redis 재고/구매자 키를 재적재한다.
 * 기동 시 로더(app.redis.load-on-startup)와 /admin/reset 이 사용한다.
 */
@Component
@ConditionalOnRedisEnabled
public class RedisStockSynchronizer implements RedisStockInitializer, ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RedisStockSynchronizer.class);

    private final RedisStockOps ops;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final StockProperties properties;

    public RedisStockSynchronizer(RedisStockOps ops, ProductRepository productRepository,
                                  OrderRepository orderRepository, StockProperties properties) {
        this.ops = ops;
        this.productRepository = productRepository;
        this.orderRepository = orderRepository;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (properties.redis().loadOnStartup()) {
            resetAndLoadFromDb();
        }
    }

    @Override
    public void resetAndLoadFromDb() {
        ops.deleteAll();
        List<Order> successOrders = orderRepository.findAll().stream()
                .filter(o -> o.getStatus() == OrderStatus.SUCCESS).toList();
        for (Product product : productRepository.findAll()) {
            Set<Long> buyers = successOrders.stream()
                    .filter(o -> o.getProductId().equals(product.getId()))
                    .map(Order::getUserId).collect(Collectors.toSet());
            ops.load(product.getId(), product.getStock(), buyers);
        }
        log.info("Redis 재고를 DB 기준으로 적재했습니다.");
    }
}
