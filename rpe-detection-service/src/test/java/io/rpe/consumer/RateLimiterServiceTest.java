package io.rpe.consumer;

import io.rpe.config.RpeProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the DEGRADED (local Guava) path of {@link RateLimiterService} — the fallback
 * taken when the distributed limiter is unavailable (ADR-24). The distributed token-bucket
 * primary and the multi-instance global-cap property are covered against a real Redis in
 * {@code DistributedRateLimiterIntegrationTest}.
 */
class RateLimiterServiceTest {

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    private RateLimiterService newService(
            AccountClassificationService cls, DistributedRateLimiter distributed) {
        RpeProperties props = new RpeProperties(
                null,
                // default 1000/s, elevated 5000/s, 60s key TTL, 5s acquire budget
                new RpeProperties.RateLimitingProperties(1000, 5000, 60_000, 5_000),
                null);
        return new RateLimiterService(props, cls, distributed, meters);
    }

    /** Distributed limiter down ⇒ degrade to local; an account under ample rate still acquires. */
    @Test
    void degradesToLocalAndAcquiresUnderAmpleRate() {
        AccountClassificationService cls = mock(AccountClassificationService.class);
        when(cls.isElevated(anyString())).thenReturn(false);
        DistributedRateLimiter distributed = mock(DistributedRateLimiter.class);
        when(distributed.tryConsume(anyString(), anyDouble(), anyLong()))
                .thenThrow(new RuntimeException("redis unavailable"));

        RateLimiterService svc = newService(cls, distributed);

        assertThat(svc.tryAcquire("acct")).isTrue();
        assertThat(svc.tryAcquire("acct")).isTrue();
        // Every degraded acquire is observable — the alert signal that the distributed limiter is down.
        assertThat(meters.counter("rpe.ratelimit.degraded").count()).isEqualTo(2.0);
        // Outcome recorded with mode=local.
        assertThat(meters.counter("rpe.ratelimit.decision", "outcome", "allowed", "mode", "local")
                .count()).isEqualTo(2.0);
    }

    /** Distributed allow ⇒ never touches the local limiter; outcome recorded as mode=distributed. */
    @Test
    void usesDistributedWhenAvailable_noDegrade() {
        AccountClassificationService cls = mock(AccountClassificationService.class);
        when(cls.isElevated(anyString())).thenReturn(false);
        DistributedRateLimiter distributed = mock(DistributedRateLimiter.class);
        when(distributed.tryConsume(anyString(), anyDouble(), anyLong()))
                .thenReturn(new DistributedRateLimiter.Decision(true, 0));

        RateLimiterService svc = newService(cls, distributed);

        assertThat(svc.tryAcquire("acct")).isTrue();
        assertThat(meters.counter("rpe.ratelimit.degraded").count()).isEqualTo(0.0);
        assertThat(meters.counter("rpe.ratelimit.decision", "outcome", "allowed", "mode", "distributed")
                .count()).isEqualTo(1.0);
    }

    /**
     * Classification is consulted on EVERY acquire (rate can flip default↔elevated within the
     * limiter's lifetime) — the regression guarded is UNDER-consultation (classify once).
     * Lower-bound assertion: see the original ADR-04 staleness-fix rationale.
     */
    @Test
    void reclassifiesOnEveryAcquire() {
        AccountClassificationService cls = mock(AccountClassificationService.class);
        when(cls.isElevated(anyString())).thenReturn(false);
        DistributedRateLimiter distributed = mock(DistributedRateLimiter.class);
        when(distributed.tryConsume(anyString(), anyDouble(), anyLong()))
                .thenReturn(new DistributedRateLimiter.Decision(true, 0));

        RateLimiterService svc = newService(cls, distributed);

        svc.tryAcquire("acct");
        verify(cls, atLeastOnce()).isElevated("acct");
        clearInvocations(cls);
        svc.tryAcquire("acct");
        verify(cls, atLeastOnce()).isElevated("acct");
    }
}
