package io.rpe.consumer;

import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the ADR-24 fix against a real Redis: the token bucket is GLOBAL, so two limiter
 * instances sharing one Redis enforce a combined cap of ~{@code rate}, not {@code 2 × rate}
 * (the per-instance behaviour ADR-03 left as a known limitation).
 */
@Testcontainers(disabledWithoutDocker = true)
class DistributedRateLimiterIntegrationTest {

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2.5"))
                    .withExposedPorts(6379)
                    .withCommand("--appendonly yes --appendfsync everysec "
                            + "--maxmemory 96mb --maxmemory-policy volatile-lru --save \"\"");

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redis;

    @BeforeAll
    static void setUp() {
        var cfg = new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory = new LettuceConnectionFactory(cfg);
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redis = new ReactiveStringRedisTemplate(connectionFactory);
    }

    @AfterAll
    static void tearDown() {
        if (connectionFactory != null) connectionFactory.destroy();
    }

    @SuppressWarnings("rawtypes")
    private DistributedRateLimiter newLimiter() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/rate_limit.lua"));
        script.setResultType(List.class);
        // Each limiter gets its own R4j registry — proves the cap is enforced in Redis, not by
        // any shared in-process state.
        return new DistributedRateLimiter(
                redis, script, Schedulers.boundedElastic(),
                CircuitBreakerRegistry.ofDefaults(), BulkheadRegistry.ofDefaults());
    }

    /** Interleaves {@code attempts} acquires for one account across the given instances. */
    private int burstAcross(String account, int rate, int attempts, DistributedRateLimiter... instances) {
        int allowed = 0;
        for (int i = 0; i < attempts; i++) {
            if (instances[i % instances.length].tryConsume(account, rate, 60_000).allowed()) allowed++;
        }
        return allowed;
    }

    /** A rapid burst is limited well below the number of attempts (the bucket throttles). */
    @Test
    void singleInstanceBurstIsLimited() {
        DistributedRateLimiter limiter = newLimiter();
        int rate = 5;
        int attempts = 50;

        int allowed = burstAcross("acct-" + UUID.randomUUID(), rate, attempts, limiter);

        // At least the initial capacity is admitted; far fewer than the attempts get through (a
        // token bucket also admits rate×burst-duration in refill, so we assert a robust band, not
        // an exact count — see twoInstances test for the latency-cancelling proof).
        assertThat(allowed).isGreaterThanOrEqualTo(rate);
        assertThat(allowed).isLessThan(attempts / 2);
    }

    /**
     * THE fix (latency-robust): a second replica on the SAME account shares ONE global bucket, so
     * it does NOT raise the admitted count. Two independent per-instance Guava limiters (the
     * pre-ADR-24 world) would have admitted ~2× — this asserts we stay near 1×.
     *
     * Measured back-to-back on fresh accounts so the refill term (rate × burst-duration) is the
     * same in both runs and cancels out of the comparison.
     */
    @Test
    void addingReplicasDoesNotRaiseTheGlobalLimit() {
        DistributedRateLimiter instanceA = newLimiter();
        DistributedRateLimiter instanceB = newLimiter();
        int rate = 8;
        int attempts = 40;   // > capacity, so the bucket (not the attempt count) is the limiter

        int oneInstance = burstAcross("acct-" + UUID.randomUUID(), rate, attempts, instanceA);
        int twoShared   = burstAcross("acct-" + UUID.randomUUID(), rate, attempts, instanceA, instanceB);

        assertThat(oneInstance).isGreaterThanOrEqualTo(rate).isLessThan(attempts);
        assertThat(twoShared).isGreaterThanOrEqualTo(rate);
        // Shared bucket ⇒ two replicas admit ~the same as one (ratio ≈ 1). Per-instance limiters
        // would admit ~2×; 1.5× cleanly separates the two regimes despite latency jitter.
        assertThat(twoShared).isLessThanOrEqualTo((oneInstance * 3) / 2);
    }

    /** On denial the bucket reports a positive wait — the input to the throttle-not-drop loop. */
    @Test
    void deniedDecisionReportsPositiveWait() {
        String account = "acct-" + UUID.randomUUID();
        DistributedRateLimiter limiter = newLimiter();
        int rate = 3;

        // Fire well past capacity so the bucket is guaranteed to deplete despite refill — then
        // assert that every denial carries a positive wait (what the throttle loop parks on).
        boolean sawDeniedWithPositiveWait = false;
        for (int i = 0; i < 40; i++) {
            DistributedRateLimiter.Decision d = limiter.tryConsume(account, rate, 60_000);
            if (!d.allowed()) {
                assertThat(d.waitMs()).isPositive();
                sawDeniedWithPositiveWait = true;
            }
        }
        assertThat(sawDeniedWithPositiveWait).isTrue();
    }
}
