package com.lab.stock.strategy;

import com.lab.stock.strategy.StockExceptions.UnknownStrategyException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** 이름 → 전략 Map. 조건부로 등록되는 전략(redis/kafka)이 비활성이면 목록에서 빠진다. */
@Component
public class StrategyRegistry {

    private final Map<String, StockStrategy> strategies;

    public StrategyRegistry(List<StockStrategy> all) {
        this.strategies = all.stream().collect(Collectors.toUnmodifiableMap(StockStrategy::name, Function.identity()));
    }

    public StockStrategy get(String name) {
        StockStrategy strategy = strategies.get(name);
        if (strategy == null) {
            throw new UnknownStrategyException(name);
        }
        return strategy;
    }

    public java.util.Set<String> names() {
        return strategies.keySet();
    }
}
