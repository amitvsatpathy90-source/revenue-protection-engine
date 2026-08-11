package io.rpe.redis;

import io.rpe.detection.LuaGateResult;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.rpe.observability.RedisMemoryMetrics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the ADR-27 stats-key lifecycle against a real Redis.
 *
 * ADR-10 left {@code stats:{account}} with no TTL — invisible to {@code volatile-lru}, so under
 * account churn the unevictable set grows until it squeezes out the TTL'd dedup/vel/geo working
 * set (silent detection decay) and terminally wedges Redis in noeviction write errors. ADR-27
 * supersedes that sub-decision with a sliding idle TTL refreshed by every gate execution. The
 * regression this test guards: someone removing the {@code PEXPIRE} from gate.lua step 2 (the
 * key would silently revert to immortal — nothing else fails).
 *
 * Also pins the ADR-27 memory watermark: {@code RedisMemoryMetrics} populates the
 * {@code rpe.redis.*memory.bytes} gauges from a live INFO reply.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisStatsLifecycleIntegrationTest {

    private static final long STATS_TTL_MS = 2_592_000_000L; // 30d — the application.yml default

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2.5"))
                    .withExposedPorts(6379)
                    .withCommand("--appendonly yes --appendfsync everysec "
                            + "--maxmemory 96mb --maxmemory-policy volatile-lru --save \"\"");

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redis;
    private static RedisGate gate;

    @BeforeAll
    @SuppressWarnings("rawtypes")
    static void setUp() {
        var cfg = new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory = new LettuceConnectionFactory(cfg);
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redis = new ReactiveStringRedisTemplate(connectionFactory);

        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/gate.lua"));
        script.setResultType(List.class);
        gate = new RedisGate(redis, script, Schedulers.boundedElastic(),
                CircuitBreakerRegistry.ofDefaults(), BulkheadRegistry.ofDefaults(),
                new SimpleMeterRegistry());
    }

    @AfterAll
    static void tearDown() {
        if (connectionFactory != null) connectionFactory.destroy();
    }

    private static LuaGateResult runGate(String account) {
        return runGate(account, new BigDecimal("42.00"), 10_000);
    }

    /** Sample cap is an ARGV parameter, so a tiny cap drives the past-cap regime in a few hundred
     *  events instead of the 10,001 the production default would need. */
    private static LuaGateResult runGate(String account, BigDecimal amount, int welfordSampleCap) {
        return gate.execute(
                UUID.randomUUID().toString(), account,
                amount, new BigDecimal("48.8566"), new BigDecimal("2.3522"),
                System.currentTimeMillis(),
                60_000, 120_000, 86_400_000, welfordSampleCap, 300, STATS_TTL_MS
        ).block(Duration.ofSeconds(5));
    }

    @Test
    void statsKeyCarriesSlidingTtlRefreshedByEveryEvent() {
        String account = "acct-" + UUID.randomUUID();
        String statsKey = "stats:" + account;

        LuaGateResult first = runGate(account);
        assertThat(first.dedupBlocked()).isFalse();

        Duration ttlAfterFirst = redis.getExpire(statsKey).block(Duration.ofSeconds(5));
        assertThat(ttlAfterFirst)
                .as("gate step 2 must PEXPIRE the stats key (ADR-27) — a zero/negative TTL "
                        + "means the key is immortal again and the noeviction wedge is back")
                .isNotNull()
                .isGreaterThan(Duration.ZERO)
                .isLessThanOrEqualTo(Duration.ofMillis(STATS_TTL_MS));

        // Sliding, not one-shot: shrink the remaining TTL, run a second event for the same
        // account, and the TTL must snap back to the full window (a hot account never expires).
        redis.expire(statsKey, Duration.ofSeconds(5)).block(Duration.ofSeconds(5));
        LuaGateResult second = runGate(account);

        Duration ttlAfterSecond = redis.getExpire(statsKey).block(Duration.ofSeconds(5));
        assertThat(ttlAfterSecond)
                .as("every gate execution refreshes the idle TTL")
                .isNotNull()
                .isGreaterThan(Duration.ofSeconds(5));
        assertThat(second.welfordCount())
                .as("TTL refresh must not reset the Welford state — same key, count accumulates")
                .isEqualTo(2);
    }

    @Test
    void welfordM2StopsAccumulatingPastTheSampleCap() {
        // ADR-08 amendment (arch-audit): the Lua half of a matched pair with ZScoreDetector.
        //
        // Before the amendment the cap applied to the MEAN only (an EWMA over the last `cap` samples)
        // while M2 accumulated over the true, unbounded n. ZScoreDetector divided that lifetime M2 by
        // the lifetime n, so after a regime shift in an account's amounts the mean re-centred within
        // ~cap events but the variance stayed inflated by the old regime for ~n events — suppressed
        // z-scores, silently missed alerts, no counter moving.
        //
        // Stationary input: amounts alternate 90/110 → mean 100, variance 100. With cap=50 the decayed
        // M2 converges to ~cap*variance = 5,000 and STAYS there as n grows. Un-decayed it reaches
        // ~n*variance = 50,000 by n=500. The bound below sits an order of magnitude from the
        // un-decayed value, so it separates the two implementations rather than fitting this one.
        String account = "acct-" + UUID.randomUUID();
        int cap = 50;
        int events = 500;

        LuaGateResult last = null;
        for (int i = 1; i <= events; i++) {
            last = runGate(account, BigDecimal.valueOf(i % 2 == 0 ? 90.0 : 110.0), cap);
        }

        assertThat(last).isNotNull();
        assertThat(last.welfordCount())
                .as("the TRUE count is still stored and returned uncapped — the >=30 z-score gate "
                        + "reads it, so capping the stored count would break that boundary")
                .isEqualTo(events);
        assertThat(last.welfordM2())
                .as("M2 must stay scaled to ~cap samples (~%d), not grow with n (~%d un-decayed). "
                        + "An un-decayed M2 divided by the true n is the silent missed-alert path.",
                        cap * 100, events * 100)
                .isGreaterThan(0.0)
                .isLessThan(15_000.0);
    }

    @Test
    void memoryWatermarkGaugesPopulateFromInfo() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RedisMemoryMetrics metrics = new RedisMemoryMetrics(redis, registry);

        metrics.refresh();

        double used = registry.get("rpe.redis.used_memory.bytes").gauge().value();
        double max = registry.get("rpe.redis.maxmemory.bytes").gauge().value();
        assertThat(used).as("used_memory parsed from INFO").isGreaterThan(0);
        assertThat(max).as("maxmemory 96mb from the container command").isEqualTo(96 * 1024 * 1024);
        assertThat(registry.get("rpe.redis.info.poll_failures").counter().count()).isZero();
    }
}
