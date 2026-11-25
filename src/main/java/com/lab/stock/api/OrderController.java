package com.lab.stock.api;

import com.lab.stock.domain.Order;
import com.lab.stock.domain.OrderRepository;
import com.lab.stock.domain.OrderStatus;
import com.lab.stock.strategy.OrderResult;
import com.lab.stock.strategy.StockStrategy;
import com.lab.stock.strategy.StrategyRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final StrategyRegistry registry;
    private final OrderRepository orderRepository;
    private final MeterRegistry meterRegistry;

    public OrderController(StrategyRegistry registry, OrderRepository orderRepository, MeterRegistry meterRegistry) {
        this.registry = registry;
        this.orderRepository = orderRepository;
        this.meterRegistry = meterRegistry;
    }

    /** 성공/품절은 200, kafka 전략의 접수(PENDING)는 202, 중복 409 는 예외 핸들러가 처리한다. */
    @PostMapping
    public ResponseEntity<OrderResponse> order(@RequestParam String strategy, @Valid @RequestBody OrderRequest request) {
        StockStrategy stockStrategy = registry.get(strategy);
        Timer.Sample sample = Timer.start(meterRegistry);
        String outcome = "error";
        try {
            OrderResult result = stockStrategy.order(request.productId(), request.userId());
            outcome = result.status().name().toLowerCase();
            HttpStatus status = result.status() == OrderStatus.PENDING ? HttpStatus.ACCEPTED : HttpStatus.OK;
            return ResponseEntity.status(status)
                    .body(new OrderResponse(result.orderId(), result.status(), stockStrategy.name()));
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            outcome = "duplicate";
            throw e;
        } finally {
            sample.stop(Timer.builder("stock.order")
                    .description("전략별 주문 API 처리 시간 (응답 기준)")
                    .tag("strategy", stockStrategy.name())
                    .tag("outcome", outcome)
                    .publishPercentileHistogram()
                    .register(meterRegistry));
        }
    }

    @GetMapping("/{id}")
    public OrderStatusResponse get(@PathVariable Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "주문을 찾을 수 없습니다. id=" + id));
        return OrderStatusResponse.from(order);
    }
}
