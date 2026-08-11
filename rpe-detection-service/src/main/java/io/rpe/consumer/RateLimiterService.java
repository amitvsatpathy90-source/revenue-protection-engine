package io.rpe.consumer;

import io.rpe.config.RpeProperties;
import io.rpe.util.Pii;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Per-account rate limiting. Public contract is unchanged — {@link #tryAcquire(String)} returns
 * {@code true} when a permit was acquired (possibly after a bounded throttle wait) and
 * {@code false} only on a sustained breach (→ DLT). Called INSIDE the lane task, never on the
 * Kafka consumer thread (ADR-03).
 *
 * <b>Distributed primary, local degraded fallback (ADR-24):</b>
 * <ul>
 *   <li><b>Primary:</b> {@link DistributedRateLimiter} — an atomic Redis token bucket shared by
 *       all replicas, so the global limit is {@code rate/s} regardless of replica count N. This
 *       closes ADR-03's {@code global = N × per_instance}.</li>
 *   <li><b>Degraded:</b> when the {@code ratelimit} CB is open or Redis errors, falls back to the
 *       per-instance Guava {@link RateLimiter} (today's behaviour) — bounded {@code N × rate/s},
 *       self-healing. Never fails closed: dropping an unrepeatable payment event over a Redis blip
 *       is exactly what ADR-03 forbids.</li>
 * </ul>
 *
 * <b>Throttle, not drop:</b> on a distributed deny the lane VT parks for the bucket-reported
 * {@code waitMs} (bounded by {@code acquireTimeoutMs}) and retries — replicating Guava's blocking
 * {@code tryAcquire(5s)}. The lane is single-threaded per account, so the park preserves ordering.
 */
@Service
@SuppressWarnings("UnstableApiUsage")
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final RpeProperties props;
    private final AccountClassificationService classificationService;
    private final DistributedRateLimiter distributedRateLimiter;
    private final MeterRegistry meterRegistry;

    /** Per-instance limiter map — the degraded fallback when the distributed limiter is down. */
    private final Cache<String, RateLimiter> localLimiterCache;

    public RateLimiterService(
            RpeProperties props,
            AccountClassificationService classificationService,
            DistributedRateLimiter distributedRateLimiter,
            MeterRegistry meterRegistry) {
        this.props                  = props;
        this.classificationService  = classificationService;
        this.distributedRateLimiter = distributedRateLimiter;
        this.meterRegistry          = meterRegistry;
        this.localLimiterCache = Caffeine.newBuilder()
                .maximumSize(50_000)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build();
    }

    /**
     * Acquires a rate-limit permit for the given account, blocking up to
     * {@code rpe.rate-limiting.acquire-timeout-ms} (default 5s). Returns {@code false} on a
     * sustained breach → route to DLT. MUST be called inside the lane task (ADR-03).
     */
    public boolean tryAcquire(String accountId) {
        double rate = rateFor(accountId);
        long keyTtlMs = props.rateLimiting().keyTtlMs();
        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(props.rateLimiting().acquireTimeoutMs());
        Timer.Sample sample = Timer.start(meterRegistry);

        while (true) {
            DistributedRateLimiter.Decision decision;
            try {
                decision = distributedRateLimiter.tryConsume(accountId, rate, keyTtlMs);
            } catch (RuntimeException e) {
                // CallNotPermittedException (CB open) or Redis error/timeout — both are
                // RuntimeException — ⇒ degrade to the per-instance limiter for the remaining
                // budget. Never fail closed: that would drop the event (ADR-03).
                return acquireLocalDegraded(accountId, rate, deadlineNanos, sample);
            }

            if (decision.allowed()) {
                record(sample, "allowed", "distributed");
                return true;
            }

            long remainingMs = remainingMs(deadlineNanos);
            if (remainingMs <= 0) {
                record(sample, "breach", "distributed");
                log.warn("Rate limit sustained breach for account ...{} — routing to DLT",
                        Pii.maskAccountId(accountId));
                return false;
            }
            // Park the lane VT until a token accrues (bounded by the remaining budget). VT park
            // does not pin a carrier — same model as RedisGate/AccountClassificationService.
            parkBounded(Math.min(Math.max(1, decision.waitMs()), remainingMs));
        }
    }

    /**
     * Degraded path: per-instance Guava limiter (the pre-ADR-24 behaviour). Honours the same
     * acquire budget so throttle-not-drop still holds; {@code rpe.ratelimit.degraded} is the
     * alert signal that the distributed limiter is unavailable.
     */
    private boolean acquireLocalDegraded(
            String accountId, double rate, long deadlineNanos, Timer.Sample sample) {
        meterRegistry.counter("rpe.ratelimit.degraded").increment();

        RateLimiter limiter = localLimiterCache.get(accountId, k -> RateLimiter.create(rate));
        if (limiter.getRate() != rate) {
            limiter.setRate(rate);
        }
        long remainingMs = Math.max(0, remainingMs(deadlineNanos));
        boolean acquired = limiter.tryAcquire((int) remainingMs, TimeUnit.MILLISECONDS);
        record(sample, acquired ? "allowed" : "breach", "local");
        if (!acquired) {
            log.warn("Rate limit sustained breach (degraded/local) for account ...{} — routing to DLT",
                    Pii.maskAccountId(accountId));
        }
        return acquired;
    }

    private double rateFor(String accountId) {
        return classificationService.isElevated(accountId)
                ? props.rateLimiting().elevatedLimitPerSec()
                : props.rateLimiting().defaultLimitPerSec();
    }

    private static long remainingMs(long deadlineNanos) {
        return (deadlineNanos - System.nanoTime()) / 1_000_000;
    }

    private void parkBounded(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // Forced drain (ADR-22 shutdownNow) interrupted the throttle wait. Restore the flag
            // and abandon this acquire; the generic boundary catch in PaymentEventConsumer routes
            // the event to DLT (deterministic id + dedup make replay safe).
            Thread.currentThread().interrupt();
            throw new RuntimeException("rate-limit throttle wait interrupted", e);
        }
    }

    private void record(Timer.Sample sample, String outcome, String mode) {
        meterRegistry.counter("rpe.ratelimit.decision", "outcome", outcome, "mode", mode).increment();
        sample.stop(meterRegistry.timer("rpe.ratelimit.wait", "outcome", outcome));
    }
}
