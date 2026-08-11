package io.rpe.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes outbox health metrics to Micrometer → Prometheus.
 *
 * {@code rpe.outbox.pending.count} — number of PENDING rows in outbox.
 * {@code rpe.outbox.pending.age_seconds} — age of oldest PENDING row (seconds).
 *
 * Log WARN thresholds (relay health signals):
 *   pending.count > 100 OR pending.age_seconds > 30 indicate a stuck relay.
 *   A stuck relay is invisible without these signals; you discover it when alerts stop arriving.
 */
@Component
public class OutboxMetrics {

    private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);

    private final DataSource    dataSource;
    private final Scheduler     jdbcScheduler;
    private final MeterRegistry meterRegistry;

    private final AtomicLong pendingCount      = new AtomicLong(0);
    private final AtomicLong pendingAgeSeconds = new AtomicLong(0);
    private final AtomicLong failedCount       = new AtomicLong(0);

    public OutboxMetrics(DataSource dataSource, Scheduler jdbcScheduler, MeterRegistry meterRegistry) {
        this.dataSource    = dataSource;
        this.jdbcScheduler = jdbcScheduler;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void registerGauges() {
        Gauge.builder("rpe.outbox.pending.count", pendingCount, AtomicLong::get)
                .description("Number of PENDING outbox rows")
                .register(meterRegistry);
        Gauge.builder("rpe.outbox.pending.age_seconds", pendingAgeSeconds, AtomicLong::get)
                .description("Age in seconds of the oldest PENDING outbox row")
                .register(meterRegistry);
        Gauge.builder("rpe.outbox.failed.count", failedCount, AtomicLong::get)
                .description("Outbox rows that exhausted relay attempts (dead-lettered, operator review)")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelay = 10_000, initialDelay = 15_000)
    public void refresh() {
        Mono.fromCallable(this::queryOutboxStats)
                .subscribeOn(jdbcScheduler)
                .subscribe(
                        stats -> {
                            pendingCount.set(stats[0]);
                            pendingAgeSeconds.set(stats[1]);
                            failedCount.set(stats[2]);
                            if (stats[0] > 100) {
                                log.warn("Outbox pending count={} exceeds threshold 100 — relay may be stuck", stats[0]);
                            }
                            if (stats[1] > 30) {
                                log.warn("Outbox oldest pending age={}s exceeds threshold 30s — relay may be stuck", stats[1]);
                            }
                        },
                        err -> log.error("Outbox metrics query failed", err));
    }

    private long[] queryOutboxStats() throws Exception {
        String sql =
                "SELECT COUNT(*) FILTER (WHERE status='PENDING'), "
              + "COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at) FILTER (WHERE status='PENDING'))), 0), "
              + "COUNT(*) FILTER (WHERE status='FAILED') "
              + "FROM outbox WHERE status IN ('PENDING','FAILED')";
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.setQueryTimeout(5);
            try (var rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return new long[]{rs.getLong(1), rs.getLong(2), rs.getLong(3)};
                }
            }
        }
        return new long[]{0L, 0L, 0L};
    }
}
