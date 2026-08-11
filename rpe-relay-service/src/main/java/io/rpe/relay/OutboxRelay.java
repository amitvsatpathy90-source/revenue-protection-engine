package io.rpe.relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rpe.relay.config.BoundaryHandler;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Outbox relay: polls PENDING rows and publishes them to {@code payment.alerts}.
 *
 * Concurrency: {@code FOR UPDATE SKIP LOCKED} allows safe concurrent relay instances.
 * Each relay instance has a unique {@code transactional.id} (RELAY_INSTANCE_ID, ADR-06).
 *
 * Adaptive polling: processed > 0 → tight (minPollMs); no signal + no rows → exponential
 * backoff to maxPollMs. Result: 40× fewer idle DB queries vs fixed 200ms polling (ADR-05).
 *
 * <b>Bounded attempts → dead-letter status:</b> rows that exhaust {@code maxAttempts}
 * transition PENDING → FAILED in the same batch transaction. FAILED rows are never
 * re-picked, never purged by the 7-day job, and are surfaced by the
 * {@code rpe.outbox.failed.count} gauge — an operator decision, not a retry loop.
 *
 * All JDBC runs on {@code jdbcScheduler} (dedicated platform-thread pool).
 * PgJDBC {@code synchronized} internals WILL pin VT carrier threads if run on VT lanes.
 */
@Component
@BoundaryHandler("relay-loop-must-survive any failure; the background loop logs and continues polling the outbox")
public class OutboxRelay implements SmartLifecycle {

    private static final Logger log   = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int    LIMIT = 50;

    /**
     * Graceful-shutdown phase (ADR-22). The relay has no Kafka CONSUMER container, but it does
     * own a Kafka-TRANSACTIONAL producer (a {@code DisposableBean} destroyed during bean
     * destruction, AFTER all SmartLifecycle.stop()). Stopping the loop in a lifecycle phase
     * therefore guarantees it ceases BEFORE the producer factory is torn down — the loop can
     * never be mid-{@code executeInTransaction} when its producer is closed. Phase value mirrors
     * detection's drain rung for symmetry.
     */
    static final int PHASE = AbstractMessageListenerContainer.DEFAULT_PHASE - 10;

    /** Total bound on stopping the loop: a short grace for an in-flight batch to finish, then an
     *  interrupt to break an idle poll-park, then the remainder of the join. */
    private static final Duration STOP_TIMEOUT      = Duration.ofSeconds(10);
    private static final long     BATCH_GRACE_MILLIS = 2_000;

    private final RelayProperties props;
    private final PostgresNotifyRelayTrigger relayTrigger;
    private final AlertPublisher alertPublisher;
    private final DataSource     dataSource;
    private final ObjectMapper   objectMapper;
    private final Scheduler      jdbcScheduler;
    private final MeterRegistry  meterRegistry;

    private volatile boolean running;
    private volatile Thread  relayThread;

    public OutboxRelay(
            RelayProperties props,
            PostgresNotifyRelayTrigger relayTrigger,
            AlertPublisher alertPublisher,
            DataSource dataSource,
            ObjectMapper objectMapper,
            Scheduler jdbcScheduler,
            MeterRegistry meterRegistry) {
        this.props          = props;
        this.relayTrigger   = relayTrigger;
        this.alertPublisher = alertPublisher;
        this.dataSource     = dataSource;
        this.objectMapper   = objectMapper;
        this.jdbcScheduler  = jdbcScheduler;
        this.meterRegistry  = meterRegistry;
    }

    @Override
    public void start() {
        running     = true;
        relayThread = Thread.ofVirtual().name("rpe-relay").start(this::relayLoop);
    }

    /**
     * Graceful shutdown (ADR-22), DRAIN phase. Two-phase stop so an in-flight outbox batch
     * completes cleanly rather than being torn in half:
     *   1. clear {@code running} and join briefly — a batch already in progress finishes and the
     *      loop exits at its next guard check;
     *   2. if still parked in an idle poll-wait, interrupt and join the remainder.
     * On a forced timeout the rows simply stay PENDING and are re-sent on restart — the idempotent
     * producer + {@code processed_alerts ON CONFLICT} keep that exactly-once on the topic.
     */
    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        Thread t = relayThread;
        if (t == null) {
            return;
        }

        log.info("Graceful shutdown: stopping relay loop, awaiting in-flight batch (≤{}s)",
                STOP_TIMEOUT.toSeconds());
        long startNanos = System.nanoTime();
        try {
            t.join(BATCH_GRACE_MILLIS);                 // let a running batch finish, loop re-check running
            if (t.isAlive()) {
                t.interrupt();                          // break an idle awaitSignal() park
                long remainingMs = STOP_TIMEOUT.toMillis() - BATCH_GRACE_MILLIS;
                t.join(Math.max(remainingMs, 0));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        meterRegistry.timer("rpe.shutdown.drain.duration", "component", "relay")
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        if (t.isAlive()) {
            meterRegistry.counter("rpe.shutdown.forced", "component", "relay").increment();
            log.warn("Relay loop did not stop within {}s — outbox rows stay PENDING and re-send "
                    + "on restart (idempotent producer + ON CONFLICT keep exactly-once)", STOP_TIMEOUT.toSeconds());
        } else {
            log.info("Relay loop stopped cleanly; in-flight outbox batch completed before producer teardown");
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    private void relayLoop() {
        long pollMs = props.minPollMs();

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                boolean signaled = relayTrigger.awaitSignal(pollMs);
                Integer processed = Mono.fromCallable(this::processOutboxBatch)
                        .subscribeOn(jdbcScheduler)
                        .block();

                if (processed != null && processed > 0) {
                    pollMs = props.minPollMs();
                } else if (!signaled) {
                    pollMs = Math.min(pollMs * 2, props.maxPollMs());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Relay loop error", e);
                pollMs = Math.min(pollMs * 2, props.maxPollMs());
            }
        }
    }

    /**
     * Runs entirely on the jdbcScheduler platform thread.
     * Returns the number of rows processed in this batch.
     */
    private int processOutboxBatch() throws Exception {
        int processed = 0;
        String selectSql =
                "SELECT id, account_id, payload, traceparent, tracestate FROM outbox "
              + "WHERE status='PENDING' AND attempts < ? "
              + "FOR UPDATE SKIP LOCKED LIMIT " + LIMIT;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            List<OutboxRow> rows = new ArrayList<>();

            try (var ps = conn.prepareStatement(selectSql)) {
                ps.setQueryTimeout(10);
                ps.setInt(1, props.maxAttempts());
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new OutboxRow(
                                UUID.fromString(rs.getString("id")),
                                rs.getString("account_id"),
                                objectMapper.readTree(rs.getString("payload")),
                                rs.getString("traceparent"),
                                rs.getString("tracestate")));
                    }
                }
            }

            for (OutboxRow row : rows) {
                boolean ok = alertPublisher.publish(
                        conn, row.id(), row.accountId(), row.payload(),
                        row.traceparent(), row.tracestate());
                if (!ok) {
                    incrementAttempts(conn, row.id());
                    // alertPublisher already logged the error
                }
                processed++;
            }

            // Bounded attempts: rows that just exhausted the budget become FAILED —
            // dead-lettered for operator inspection, never silently retried forever.
            markExhaustedAsFailed(conn);

            conn.commit();
        }
        return processed;
    }

    private void incrementAttempts(Connection conn, UUID id) throws Exception {
        try (var ps = conn.prepareStatement(
                "UPDATE outbox SET attempts=attempts+1 WHERE id=?::uuid")) {
            ps.setQueryTimeout(10);
            ps.setString(1, id.toString());
            ps.executeUpdate();
        }
    }

    private void markExhaustedAsFailed(Connection conn) throws Exception {
        // SKIP LOCKED subselect: never block on rows another relay instance holds.
        // Rows locked by THIS transaction (just attempted above) are not skipped.
        try (var ps = conn.prepareStatement(
                "UPDATE outbox SET status='FAILED' WHERE id IN ("
              + "SELECT id FROM outbox WHERE status='PENDING' AND attempts >= ? "
              + "FOR UPDATE SKIP LOCKED)")) {
            ps.setQueryTimeout(10);
            ps.setInt(1, props.maxAttempts());
            int failed = ps.executeUpdate();
            if (failed > 0) {
                log.error("{} outbox row(s) exhausted {} attempts — marked FAILED for operator review",
                        failed, props.maxAttempts());
            }
        }
    }

    // Stage 6: purge_old_records() was removed — it violated one-writer-per-table by
    // deleting from both outbox (detection's table) and processed_alerts (alert's table)
    // from the relay role (ADR-17 §3.4). Each owning service now runs its own purge:
    //   outbox          → OutboxBatchWriter.purgeOutbox()       in rpe-detection-service
    //   processed_alerts → ProcessedAlertsPurge.purge()         in rpe-alert-service
    // relay_role has no DELETE privilege on either table.

    private record OutboxRow(UUID id, String accountId, JsonNode payload,
                             String traceparent, String tracestate) {}
}
