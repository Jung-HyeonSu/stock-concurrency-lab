package com.lab.stock.consumer;

import com.lab.stock.config.ConditionalOnKafkaEnabled;
import com.lab.stock.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 재고 차감과 상태 갱신. 같은 파티션 순차 처리가 직렬화를 보장하므로 락을 쓰지 않는다. */
@Service
@ConditionalOnKafkaEnabled
public class OrderProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessor.class);

    private final OrderRepository orderRepository;
    private final ProductRepository productRepository;

    public OrderProcessor(OrderRepository orderRepository, ProductRepository productRepository) {
        this.orderRepository = orderRepository;
        this.productRepository = productRepository;
    }

    /** @return 이번 호출이 실제로 처리했으면 true, 멱등성 때문에 건너뛰었으면 false */
    @Transactional
    public boolean process(OrderMessage message) {
        Order order = orderRepository.findById(message.orderId()).orElse(null);
        if (order == null) {
            log.warn("주문을 찾을 수 없어 건너뜁니다 (DB 초기화 이전의 잔여 메시지). orderId={}", message.orderId());
            return false;
        }
        if (order.getStatus() != OrderStatus.PENDING) {
            return false; // 멱등성: 이미 처리된 주문(재전달/중복 메시지)
        }
        Product product = productRepository.findById(message.productId()).orElse(null);
        if (product != null && product.hasStock()) {
            product.decrease();
            order.markSuccess();
        } else {
            order.markSoldOut();
        }
        return true;
    }
}
