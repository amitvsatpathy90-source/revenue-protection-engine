package io.rpe.observability;

import io.rpe.config.BoundaryHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code rpe.redis.used_memory.bytes} / {@code rpe.redis.maxmemory.bytes} — the ADR-27 memory
 * watermark. Detection is the sole Redis owner (microservices.md §3), so it owns this gauge.
 *
 * Why it exists: under {@code volatile-lru}, memory pressure evicts the TTL'd dedup/vel/geo
 * working set FIRST — detection quality decays silently (missed velocity/geo alerts look like a
 * quiet day) long before Redis errors. The {@code RpeRedisMemoryHigh} rule on these gauges is the
 * only early signal for that decay; by the time the gate throws OOM write errors and the {@code
 * redis} CB opens, the damage is already hours old. ADR-27's sliding stats TTL removes the
 * unbounded-growth wedge, but the watermark is still the operational tripwire for sizing.
 *
 * Poll failures increment {@code rpe.redis.info.poll_failures} (the Bundle-1 false-healthy
 * lesson: a frozen gauge must never look healthy) — {@code RpeRedisInfoPollFailing} alerts on it.
 */
@Component
@BoundaryHandler("observability-poll-must-not-throw into the scheduling thread; a metric refresh failure logs and the next scheduled run retries")
public class RedisMemoryMetrics {

    private static final Logger log = LoggerFactory.getLogger(RedisMemoryMetrics.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final ReactiveStringRedisTemplate redis;
    private final AtomicLong usedMemoryBytes = new AtomicLong(0);
    private final AtomicLong maxMemoryBytes = new AtomicLong(0);
    private final Counter pollFailures;

    public RedisMemoryMetrics(ReactiveStringRedisTemplate redis, MeterRegistry meterRegistry) {
        this.redis = redis;
        Gauge.builder("rpe.redis.used_memory.bytes", usedMemoryBytes, AtomicLong::get)
                .description("Redis used_memory from INFO memory")
                .register(meterRegistry);
        Gauge.builder("rpe.redis.maxmemory.bytes", maxMemoryBytes, AtomicLong::get)
                .description("Redis maxmemory from INFO memory (0 = unlimited)")
                .register(meterRegistry);
        this.pollFailures = Counter.builder("rpe.redis.info.poll_failures")
                .description("Failed INFO polls; while rising, the rpe.redis.*memory gauges are stale")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 25_000)
    public void refresh() {
        try {
            // No publishOn here, deliberately: the Mono is materialized by block() on THIS
            // scheduled platform thread and has no downstream operators, so nothing user-supplied
            // executes on the Lettuce I/O thread — the hazard reactive-pipeline.md's publishOn
            // rule guards against on the hot path. A publishOn would be dead code in this shape.
            Properties info = redis.execute(conn -> conn.serverCommands().info("memory"))
                    .next()
                    .block(POLL_TIMEOUT);
            if (info == null) {
                pollFailures.increment();
                log.warn("Redis INFO memory poll returned no reply");
                return;
            }
            usedMemoryBytes.set(Long.parseLong(info.getProperty("used_memory", "0")));
            maxMemoryBytes.set(Long.parseLong(info.getProperty("maxmemory", "0")));
        } catch (Exception e) {
            pollFailures.increment();
            log.warn("Redis INFO memory poll failed: {}", e.getMessage());
        }
    }
}
