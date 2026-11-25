package com.lab.stock.support;

import static org.assertj.core.api.Assertions.assertThat;

import com.lab.stock.api.AdminService;
import com.lab.stock.domain.Order;
import com.lab.stock.domain.OrderRepository;
import com.lab.stock.domain.OrderStatus;
import com.lab.stock.domain.ProductRepository;
import com.lab.stock.strategy.OrderResult;
import com.lab.stock.strategy.StockStrategy;
import com.lab.stock.strategy.StrategyRegistry;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.springframework.beans.factory.annotation.Autowired;

/** 모든 전략에 같은 시나리오(ExecutorService + CountDownLatch)를 적용하는 테스트 공통부. */
public abstract class ConcurrencyTestSupport {

    protected static final long PRODUCT_ID = 1L;
    protected static final int STOCK = 100;
    protected static final int USERS = 1000;
    protected static final int THREADS = 100;

    @Autowired protected AdminService adminService;
    @Autowired protected StrategyRegistry registry;
    @Autowired protected OrderRepository orderRepository;
    @Autowired protected ProductRepository productRepository;

    /** 클라이언트가 응답으로 관측한 결과 집계. */
    public record Outcome(int success, int soldOut, int pending, int duplicate, int error, String firstError) {}

    protected void resetStock(int stock) {
        adminService.reset(stock, 1);
    }

    protected List<Long> distinctUsers(int count) {
        return LongStream.rangeClosed(1, count).boxed().collect(Collectors.toList());
    }

    /** userIds 각각에 대해 동시에 주문 요청을 보내고 응답 결과를 집계한다. */
    protected Outcome runConcurrently(String strategyName, List<Long> userIds) throws Exception {
        StockStrategy strategy = registry.get(strategyName);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger soldOut = new AtomicInteger();
        AtomicInteger pending = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        AtomicInteger error = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<String> firstError = new java.util.concurrent.atomic.AtomicReference<>();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(userIds.size());
        for (Long userId : userIds) {
            pool.submit(() -> {
                try {
                    start.await();
                    OrderResult result = strategy.order(PRODUCT_ID, userId);
                    switch (result.status()) {
                        case SUCCESS -> success.incrementAndGet();
                        case SOLD_OUT -> soldOut.incrementAndGet();
                        case PENDING -> pending.incrementAndGet();
                    }
                } catch (org.springframework.dao.DataIntegrityViolationException e) {
                    duplicate.incrementAndGet();
                } catch (Exception e) {
                    error.incrementAndGet();
                    firstError.compareAndSet(null, e.toString());
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(120, TimeUnit.SECONDS)).as("모든 요청이 제한 시간 안에 끝나야 한다").isTrue();
        pool.shutdownNow();
        return new Outcome(success.get(), soldOut.get(), pending.get(), duplicate.get(), error.get(), firstError.get());
    }

    protected int currentStock() {
        return productRepository.findById(PRODUCT_ID).orElseThrow().getStock();
    }

    protected long countOrders(OrderStatus status) {
        return orderRepository.countByProductIdAndStatus(PRODUCT_ID, status);
    }

    protected long distinctSuccessUsers() {
        return orderRepository.findAll().stream()
                .filter(o -> o.getStatus() == OrderStatus.SUCCESS)
                .map(Order::getUserId).distinct().count();
    }

    /** 정합성 3조건: 재고 ≥ 0, 성공 주문 수 == 차감된 재고 수, 중복 사용자 없음. */
    protected void assertConsistent(int initialStock) {
        int stock = currentStock();
        long successOrders = countOrders(OrderStatus.SUCCESS);
        assertThat(stock).as("재고는 음수가 될 수 없다").isGreaterThanOrEqualTo(0);
        assertThat(successOrders).as("성공 주문 수 == 차감된 재고 수").isEqualTo(initialStock - stock);
        assertThat(distinctSuccessUsers()).as("1인 1회 구매").isEqualTo(successOrders);
    }

    /** 정합성이 깨졌는지 여부 (no-lock, redisson-wrong 증명용). */
    protected boolean isInconsistent(int initialStock) {
        int stock = currentStock();
        long successOrders = countOrders(OrderStatus.SUCCESS);
        return stock < 0 || successOrders != initialStock - stock || successOrders > initialStock
                || distinctSuccessUsers() != successOrders;
    }
}
