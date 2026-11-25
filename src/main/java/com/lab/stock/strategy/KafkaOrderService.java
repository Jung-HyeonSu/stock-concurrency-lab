package com.lab.stock.strategy;

import com.lab.stock.config.ConditionalOnKafkaEnabled;
import com.lab.stock.domain.Order;
import com.lab.stock.domain.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** PENDING 주문 저장 단위. 커밋이 끝난 뒤에야 publish 해야 Consumer 가 못 찾는 일이 없다. */
@Service
@ConditionalOnKafkaEnabled
public class KafkaOrderService {

    private final OrderRepository orderRepository;

    public KafkaOrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Long createPending(Long productId, Long userId) {
        return orderRepository.saveAndFlush(Order.pending(productId, userId)).getId();
    }
}
