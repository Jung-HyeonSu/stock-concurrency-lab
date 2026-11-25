package com.lab.stock.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.List;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Component;

/**
 * redis-atomic 전략이 쓰는 Redis 재고 연산 모음.
 * 키: stock:{productId} (남은 재고, 문자열 정수), buyers:{productId} (구매자 SET).
 */
@Component
@ConditionalOnRedisEnabled
public class RedisStockOps {

    /** 결과 코드 */
    public static final int ACQUIRED = 1;
    public static final int SOLD_OUT = 0;
    public static final int DUPLICATE = -1;
    public static final int NOT_INITIALIZED = -2;

    /** [중복 체크(SADD) → 재고 확인 → DECR] 을 한 번에 원자 처리한다. 품절이면 SADD 를 되돌린다. */
    static final String ACQUIRE_LUA = """
            local stock = redis.call('GET', KEYS[1])
            if not stock then return -2 end
            if redis.call('SADD', KEYS[2], ARGV[1]) == 0 then return -1 end
            if tonumber(stock) <= 0 then
              redis.call('SREM', KEYS[2], ARGV[1])
              return 0
            end
            redis.call('DECR', KEYS[1])
            return 1
            """;

    /** DB 저장 실패 보상: 재고 복구(INCR) + 구매자 제거(SREM). */
    static final String COMPENSATE_LUA = """
            redis.call('INCR', KEYS[1])
            redis.call('SREM', KEYS[2], ARGV[1])
            return 1
            """;

    private final RedissonClient redisson;
    private final Counter acquireOps;
    private final Counter compensateOps;
    private final Counter loadOps;

    public RedisStockOps(RedissonClient redisson, MeterRegistry registry) {
        this.redisson = redisson;
        this.acquireOps = counter(registry, "acquire");
        this.compensateOps = counter(registry, "compensate");
        this.loadOps = counter(registry, "load");
    }

    private static Counter counter(MeterRegistry registry, String op) {
        return Counter.builder("stock.redis.ops").tag("op", op).register(registry);
    }

    public static String stockKey(long productId) {
        return "stock:" + productId;
    }

    public static String buyersKey(long productId) {
        return "buyers:" + productId;
    }

    public int acquire(long productId, long userId) {
        acquireOps.increment();
        Long result = script().eval(RScript.Mode.READ_WRITE, ACQUIRE_LUA, RScript.ReturnType.INTEGER,
                List.<Object>of(stockKey(productId), buyersKey(productId)), String.valueOf(userId));
        return result.intValue();
    }

    public void compensate(long productId, long userId) {
        compensateOps.increment();
        script().eval(RScript.Mode.READ_WRITE, COMPENSATE_LUA, RScript.ReturnType.INTEGER,
                List.<Object>of(stockKey(productId), buyersKey(productId)), String.valueOf(userId));
    }

    /** 재고와 구매자 집합을 통째로 덮어쓴다. */
    public void load(long productId, int stock, Collection<Long> buyers) {
        loadOps.increment();
        redisson.getBucket(stockKey(productId), StringCodec.INSTANCE).set(String.valueOf(stock));
        var set = redisson.getSet(buyersKey(productId), StringCodec.INSTANCE);
        set.delete();
        if (!buyers.isEmpty()) {
            set.addAll(buyers.stream().map(String::valueOf).toList());
        }
    }

    public void deleteAll() {
        redisson.getKeys().deleteByPattern("stock:*");
        redisson.getKeys().deleteByPattern("buyers:*");
    }

    public Integer remainingStock(long productId) {
        Object value = redisson.getBucket(stockKey(productId), StringCodec.INSTANCE).get();
        return value == null ? null : Integer.valueOf(value.toString());
    }

    public long buyerCount(long productId) {
        return redisson.getSet(buyersKey(productId), StringCodec.INSTANCE).size();
    }

    private RScript script() {
        return redisson.getScript(StringCodec.INSTANCE);
    }
}
