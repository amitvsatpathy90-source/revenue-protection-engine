package io.rpe.consumer;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.List;

/**
 * Distributed token-bucket rate limiter backed by an atomic Redis Lua script (ADR-24).
 *
 * One round-trip per acquire returns {@link Decision}{@code (allowed, waitMs)}. This is the
 * <em>primary</em> path; {@link RateLimiterService} owns the throttle-wait loop, the
 * classification-driven rate, and the degrade-to-local fallback when this boundary is
 * unavailable.
 *
 * <b>Per-boundary Resilience4j</b> (no global defaults — Architecture Spec): its OWN {@code ratelimit}
 * CircuitBreaker + Bulkhead, distinct from the gate's {@code redis} instances. Sharing the
 * gate's CB would conflate a rate-limit hiccup with detection-core health and could open the
 * gate CB (buffering events) for a non-detection reason. When the {@code ratelimit} CB is open,
 * {@code CallNotPermittedException} short-circuits each call with no Redis round-trip — the
 * caller degrades to the local limiter immediately (fast-fail, no per-event timeout).
 */
@Component
public class DistributedRateLimiter {

    /** Bounded wait for a single token-bucket round-trip — a hung Redis must not wedge the lane. */
    private static final Duration CALL_TIMEOUT = Duration.ofMillis(500);

    private final ReactiveStringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> rateLimitScript;
    private final Scheduler laneScheduler;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;

    @SuppressWarnings("rawtypes")
    public DistributedRateLimiter(
            ReactiveStringRedisTemplate redis,
            DefaultRedisScript<List> rateLimitScript,
            Scheduler laneScheduler,
            CircuitBreakerRegistry cbRegistry,
            BulkheadRegistry bulkheadRegistry) {
        this.redis           = redis;
        this.rateLimitScript = rateLimitScript;
        this.laneScheduler   = laneScheduler;
        this.circuitBreaker  = cbRegistry.circuitBreaker("ratelimit");
        this.bulkhead        = bulkheadRegistry.bulkhead("ratelimit");
    }

    /**
     * One token-bucket evaluation. Blocks on the lane VT (which parks without pinning a carrier,
     * the same model as {@link RedisGate} and {@code AccountClassificationService}).
     *
     * @throws io.github.resilience4j.circuitbreaker.CallNotPermittedException when the
     *         {@code ratelimit} CB is open — the caller treats this as "distributed unavailable"
     *         and degrades to the local limiter.
     * @throws RuntimeException on timeout / Redis error — also a degrade-to-local signal.
     */
    @SuppressWarnings("unchecked")
    public Decision tryConsume(String accountId, double ratePerSec, long keyTtlMs) {
        // No caller clock: rate_limit.lua reads redis.call('TIME') so every replica refills off
        // ONE server clock (ADR-24 R2). Passing System.currentTimeMillis() here reintroduced
        // inter-replica skew into the shared bucket — removed.
        List<String> keys = List.of("ratelimit:" + accountId);
        List<String> args = List.of(
                String.valueOf(ratePerSec),
                String.valueOf(keyTtlMs));

        Mono<Decision> call = redis.execute(rateLimitScript, keys, args)
                .next()
                // MANDATORY: hop off the Lettuce I/O thread before mapping (reactive-pipeline.md).
                .publishOn(laneScheduler)
                .map(raw -> toDecision((List<Object>) raw))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .transformDeferred(BulkheadOperator.of(bulkhead));

        // block() surfaces CallNotPermittedException (CB open) and timeout/Redis errors to the
        // caller as the degrade-to-local signal. A null reply (script returned nothing) is also
        // treated as a failure rather than silently allowing/denying.
        Decision decision = call.block(CALL_TIMEOUT);
        if (decision == null) {
            throw new IllegalStateException("rate-limit script returned no reply");
        }
        return decision;
    }

    private Decision toDecision(List<Object> raw) {
        // Lua integer replies map to Long. allowed ∈ {0,1}; waitMs ≥ 0.
        long allowed = ((Number) raw.get(0)).longValue();
        long waitMs  = ((Number) raw.get(1)).longValue();
        return new Decision(allowed == 1L, waitMs);
    }

    /** Outcome of one token-bucket evaluation. {@code waitMs} is 0 when {@code allowed}. */
    public record Decision(boolean allowed, long waitMs) {}
}
