package io.rpe.redis;

import io.rpe.detection.LuaGateResult;
import io.rpe.util.Pii;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.math.BigDecimal;
import java.util.List;

@Component
public class RedisGate {

    private final ReactiveStringRedisTemplate redis;
    private final DefaultRedisScript<List> gateScript;
    private final Scheduler laneScheduler;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;
    private final MeterRegistry meterRegistry;

    @SuppressWarnings("rawtypes")
    public RedisGate(
            ReactiveStringRedisTemplate redis,
            DefaultRedisScript<List> gateScript,
            Scheduler laneScheduler,
            CircuitBreakerRegistry cbRegistry,
            BulkheadRegistry bulkheadRegistry,
            MeterRegistry meterRegistry) {
        this.redis          = redis;
        this.gateScript     = gateScript;
        this.laneScheduler  = laneScheduler;
        this.circuitBreaker = cbRegistry.circuitBreaker("redis");
        this.bulkhead       = bulkheadRegistry.bulkhead("redis");
        this.meterRegistry  = meterRegistry;
    }

    /**
     * Executes the unified Lua gate atomically:
     *   velocity → z-score → geo → dedup (step ordering is a correctness invariant).
     *
     * The returned Mono executes non-blocking on the Lettuce I/O thread, then hops to
     * {@code laneScheduler} via {@code publishOn} before the caller processes the result.
     *
     * A reply that violates the gate contract (ADR-14) — partial geo state, out-of-range
     * coordinates, malformed shape — fails the Mono with {@link GateContractViolation}.
     * The violation is deterministic per event: callers must route it to DLT without retry.
     * This catch site owns the {@code rpe.gate.invalid_state} counter increment; downstream
     * handlers treat the exception as pre-reported and log at debug only.
     *
     * @param brokerIngestMs  {@code ConsumerRecord.timestamp()} — NOT the payload timestamp.
     *                        Payload clocks drift; broker ingest time is authoritative for geo.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Mono<LuaGateResult> execute(
            String eventId, String accountId,
            BigDecimal amount, BigDecimal lat, BigDecimal lon,
            long brokerIngestMs,
            long windowMs, long velTtlMs, long geoTtlMs, int welfordSampleCap,
            int dedupTtlSeconds, long statsTtlMs) {

        List<String> keys = List.of(
                "vel:"   + accountId,
                "stats:" + accountId,
                "geo:"   + accountId,
                "dedup:" + eventId);

        List<String> args = List.of(
                eventId,
                String.valueOf(brokerIngestMs),
                amount.toPlainString(),
                lat.toPlainString(),
                lon.toPlainString(),
                String.valueOf(windowMs),
                String.valueOf(velTtlMs),
                String.valueOf(geoTtlMs),
                String.valueOf(welfordSampleCap),
                String.valueOf(dedupTtlSeconds),
                // ARGV[11] — appended last (additive; existing indexes frozen): sliding idle TTL
                // for stats:{account} (ADR-27, supersedes ADR-10's no-TTL sub-decision).
                String.valueOf(statsTtlMs));

        return redis.execute(gateScript, keys, args)
                .next()
                // MANDATORY: hop off the Lettuce I/O thread BEFORE processing the result.
                // A missing publishOn (or publishOn placed after map) is a silent deadlock
                // risk: any downstream work executing on the Lettuce I/O thread can stall
                // ALL other Redis operations sharing that event loop.
                .publishOn(laneScheduler)
                .map(raw -> mapReply((List<Object>) raw, eventId))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .transformDeferred(BulkheadOperator.of(bulkhead));
    }

    /**
     * Single mapping site for the raw Lua reply. IllegalArgumentException = geo contract
     * violation; any other RuntimeException = reply shape violation (see
     * {@link LuaGateResult#from}). Never coerces partial state to a clean result —
     * a Lua regression must fail loudly, not become invisible missed detections.
     */
    private LuaGateResult mapReply(List<Object> raw, String eventId) {
        try {
            return LuaGateResult.from(raw);
        } catch (RuntimeException e) {
            String reason = e instanceof IllegalArgumentException ? "geo" : "shape";
            meterRegistry.counter("rpe.gate.invalid_state", "reason", reason).increment();
            // eventId is already in MDC at the lane entry; the message carries the masked
            // id only. No log here — the counter is the signal and the DLT path reports.
            throw new GateContractViolation(Pii.maskEventId(eventId), reason, e);
        }
    }
}
