package io.rpe.outbox;

import io.rpe.config.BoundaryHandler;
import io.rpe.domain.AlertIntent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Async batched outbox writer.
 *
 * Flushes every 50ms (timer) or at 100 rows (count trigger), whichever comes first.
 * All JDBC executes on a dedicated platform thread — PgJDBC synchronized internals
 * pin VT carrier threads if run on the VT scheduler.
 *
 * <b>Failure semantics — never a silent drop:</b>
 * <ul>
 *   <li>A failed flush re-queues the whole batch (idempotent: {@code ON CONFLICT DO
 *       NOTHING} on deterministic alert_id makes a partial-batch re-insert safe).
 *       Postgres recovers → the queue drains.
 *   <li>The queue is bounded ({@value #DEFAULT_QUEUE_CAPACITY}). When full — sustained Postgres
 *       outage — intents are dropped LOUDLY: {@code rpe.outbox.dropped} counter +
 *       error log. Bounded memory beats unbounded heap growth during an outage.
 *   <li>Accepted crash window stays as designed: up to 50ms of un-flushed intents
 *       (ADR-12). Re-queue covers failures, not process death.
 * </ul>
 *
 * {@code SET LOCAL synchronous_commit=off} per batch transaction: bounded 200ms WAL
 * flush window vs UNLOGGED table's unbounded checkpoint-interval loss (ADR-12).
 */
@Service
public class OutboxBatchWriter implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxBatchWriter.class);

    private static final int DEFAULT_FLUSH_THRESHOLD = 100;
    /** ~1KB per intent → bounded at ~10MB heap worst-case during a Postgres outage. */
    private static final int DEFAULT_QUEUE_CAPACITY  = 10_000;

    /**
     * Two rungs below the Kafka container (ADR-22). The lane drain runs at
     * {@code DEFAULT_PHASE - 10} and this final flush at {@code DEFAULT_PHASE - 20}, so flush
     * runs strictly AFTER the lanes have finished submitting their intents — closing the
     * flush-before-drain race that {@code @PreDestroy} ordering did not guarantee.
     */
    static final int PHASE = AbstractMessageListenerContainer.DEFAULT_PHASE - 20;

    /** Bound on the final drain-flush at shutdown. Postgres-down is detected by no-progress
     *  (re-queue keeps the size flat) and breaks early rather than spinning to this deadline. */
    private static final Duration SHUTDOWN_FLUSH_TIMEOUT = Duration.ofSeconds(5);

    private volatile boolean running;

    private final DataSource     dataSource;
    /** All JDBC in this class hops here — the dedicated platform-thread pool (see purgeOutbox). */
    private final Scheduler      jdbcScheduler;
    private final MeterRegistry  meterRegistry;

    private final int flushThreshold;
    private final int queueCapacity;
    private final LinkedBlockingQueue<AlertIntent> queue;
    private final Semaphore flushPermit = new Semaphore(1);
    /** Volatile: written by start()/stop(), read by submitting lane/jdbc threads in wakeFlusher(). */
    private volatile ScheduledExecutorService scheduler;

    @Autowired
    public OutboxBatchWriter(DataSource dataSource, Scheduler jdbcScheduler, MeterRegistry meterRegistry) {
        this(dataSource, jdbcScheduler, meterRegistry, DEFAULT_QUEUE_CAPACITY, DEFAULT_FLUSH_THRESHOLD);
    }

    /** Package-private: lets tests use a tiny capacity/threshold to drive the drop and
     *  flush paths deterministically without filling 10,000 entries. */
    OutboxBatchWriter(DataSource dataSource, Scheduler jdbcScheduler, MeterRegistry meterRegistry,
                      int queueCapacity, int flushThreshold) {
        this.dataSource     = dataSource;
        this.jdbcScheduler  = jdbcScheduler;
        this.meterRegistry  = meterRegistry;
        this.queueCapacity  = queueCapacity;
        this.flushThreshold = flushThreshold;
        this.queue          = new LinkedBlockingQueue<>(queueCapacity);
        Gauge.builder("rpe.outbox.queue.size", queue, LinkedBlockingQueue::size)
                .description("Alert intents awaiting batch flush to the outbox table")
                .register(meterRegistry);
    }

    @Override
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "rpe-outbox-flush");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::flushIfPending, 50, 50, TimeUnit.MILLISECONDS);
        running = true;
    }

    /**
     * Graceful shutdown (ADR-22), FLUSH phase — runs after the lane drain, so every intent the
     * hot path produced is already enqueued. Stops the periodic flusher, then drives a final
     * drain-flush of the whole queue against a bounded deadline.
     *
     * No-progress guard: a failed flush re-queues its batch (idempotent {@code ON CONFLICT}),
     * so under a Postgres outage the queue size stays flat — we detect that and break early
     * rather than busy-spinning to the deadline. Anything still queued at exit is the existing
     * ADR-12 loss window (table intact; bounded), now reported via
     * {@code rpe.shutdown.forced{component=outbox}} + {@code rpe.outbox.dropped{reason=shutdown_timeout}}.
     */
    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }

        log.info("Graceful shutdown: final outbox flush ({} intent(s) pending, ≤{}s)",
                queue.size(), SHUTDOWN_FLUSH_TIMEOUT.toSeconds());
        long startNanos    = System.nanoTime();
        long deadlineNanos = startNanos + SHUTDOWN_FLUSH_TIMEOUT.toNanos();
        int before;
        do {
            before = queue.size();
            flushIfPending();
        } while (!queue.isEmpty() && queue.size() < before && System.nanoTime() < deadlineNanos);

        meterRegistry.timer("rpe.shutdown.drain.duration", "component", "outbox")
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        int abandoned = queue.size();
        if (abandoned > 0) {
            meterRegistry.counter("rpe.shutdown.forced", "component", "outbox").increment();
            meterRegistry.counter("rpe.outbox.dropped", "reason", "shutdown_timeout").increment(abandoned);
            log.warn("Outbox flush incomplete at shutdown: {} intent(s) un-flushed (Postgres "
                    + "unavailable or deadline hit) — within the ADR-12 loss window; table intact", abandoned);
        } else {
            log.info("Outbox flush complete: queue fully drained at shutdown");
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

    /**
     * 7-day rolling purge of PUBLISHED outbox rows (Stage 6: detection owns outbox, so
     * it owns the purge — relay_role has no DELETE privilege). FAILED rows are
     * intentionally excluded: they require operator review before removal.
     *
     * Runs hourly with a 5-minute startup delay. The DELETE is dispatched to {@code jdbcScheduler}
     * — the dedicated platform-thread pool — and NOT run on the scheduling thread: with
     * {@code spring.threads.virtual.enabled=true} (application.yml) Boot auto-configures a
     * VT-backed {@code SimpleAsyncTaskScheduler} for {@code @Scheduled}, and PgJDBC's
     * {@code synchronized} internals pin a VT carrier for the statement's whole duration (here up
     * to the 60s query timeout, hourly, against a table sized for 7 days of alerts). JDBC on a
     * dedicated platform pool is a JVM-level constraint, not a preference (Architecture Spec Immutable
     * Constraints); this method previously claimed to honour it while doing the opposite
     * (arch-audit). {@code block()} parks the scheduling VT without pinning. Same shape as
     * {@code OutboxMetrics.refresh}.
     */
    @org.springframework.scheduling.annotation.Scheduled(
            fixedDelay = 3_600_000,
            initialDelay = 300_000)
    @BoundaryHandler("scheduled-purge-must-survive any failure; logs and the next fixedDelay run retries")
    public void purgeOutbox() {
        try {
            // block(), not subscribe(): fixedDelay measures from completion, so the next run must
            // not overlap a still-running purge.
            Mono.fromCallable(this::executePurge).subscribeOn(jdbcScheduler).block();
        } catch (Exception e) {
            log.error("Outbox purge failed", e);
        }
    }

    /** The purge statement itself — only ever invoked on {@code jdbcScheduler} (see purgeOutbox). */
    private int executePurge() throws Exception {
        String sql = "DELETE FROM outbox WHERE status = 'PUBLISHED' "
                   + "AND created_at < now() - interval '7 days'";
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(sql)) {
            stmt.setQueryTimeout(60);
            int deleted = stmt.executeUpdate();
            if (deleted > 0) {
                log.info("Outbox purge completed: {} PUBLISHED rows removed", deleted);
            }
            return deleted;
        }
    }

    /** Bounded backpressure before an intent is declared undeliverable: when the queue is full the
     *  submitting thread waits this long for the flusher to drain a slot. Steady-state (queue not full)
     *  this is a non-blocking O(1) offer, so the ADR-12 fast path is unchanged; only a backed-up queue
     *  (Postgres slow/down) blocks — and the return value then lets the CALLER preserve the event on the
     *  DLT instead of acking a lost alert (arch-audit HIGH-1). */
    private static final long OFFER_BACKPRESSURE_MS = 250;

    /**
     * Enqueues an alert intent for batched outbox write. Thread-safe. Applies bounded backpressure when
     * the queue is full, then triggers an immediate flush at the batch threshold.
     *
     * @return {@code true} if the intent was durably enqueued (will be flushed to the outbox table);
     *         {@code false} if the queue stayed full past {@link #OFFER_BACKPRESSURE_MS} — the caller
     *         MUST then route the event to the DLT (not ack), so a Postgres outage never turns into a
     *         silently-dropped alert on a committed offset.
     *
     * Called from a lane VT via:
     *   Boolean enqueued = Mono.fromCallable(() -> outboxWriter.submit(intent))
     *       .subscribeOn(jdbcScheduler).block();
     */
    public boolean submit(AlertIntent intent) {
        boolean enqueued;
        try {
            enqueued = queue.offer(intent, OFFER_BACKPRESSURE_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            enqueued = false;
        }
        if (!enqueued) {
            // Queue full past the backpressure window = sustained Postgres outage. Loud, and the caller
            // preserves the event on the DLT for redrive — never a silent drop on a committed offset.
            meterRegistry.counter("rpe.outbox.dropped", "reason", "queue_full").increment();
            log.error("Outbox queue full ({}) after {}ms backpressure: alert NOT enqueued, alertId={} "
                    + "— caller routes event to DLT", queueCapacity, OFFER_BACKPRESSURE_MS, intent.alertId());
            return false;
        }
        // Confirmation half of the in-flight gap metric: the intent is now safely held in the
        // volatile queue. delta(rpe.hotpath.alerts.submitted - rpe.hotpath.alerts.buffered),
        // measured against the consumer-side submitted counter, is the count of alerts still
        // in the async hop (dispatched, not yet buffered) at any instant. Dropped intents
        // (queue_full above) are intentionally NOT counted here — they never reach safety.
        meterRegistry.counter("rpe.hotpath.alerts.buffered").increment();
        if (queue.size() >= flushThreshold) {
            wakeFlusher();
        }
        return true;
    }

    /**
     * Hands the threshold-triggered flush to the flusher thread instead of running it on the caller.
     *
     * <p>This call MUST stay O(1). It used to be a direct {@code flushIfPending()}, which ran the whole
     * JDBC batch — {@code getConnection}, {@code SET LOCAL}, {@code executeBatch}, {@code commit}, two
     * 10s query timeouts — on the submitting thread. That thread is a lane VT parked on
     * {@code submitAlert}'s {@code block()}, so a slow Postgres stalled the account's lane for the whole
     * flush; the lane queue (200) then filled, {@code BoundedBlockingSubmitPolicy} parked the listener,
     * and the consumer stopped polling every partition. It also falsified Architecture Spec's hot-path contract
     * ("ack gated on a bounded O(1) enqueue … lane never waits for the Postgres write itself"), which is
     * true again only because the flush now happens off-caller (arch-audit).
     *
     * <p>{@code flushIfPending}'s {@code flushPermit} already makes flushing single-flight, so this
     * wakeup and the 50ms timer cannot collide. A skipped wakeup is harmless: the timer flushes within
     * 50ms regardless, and {@code stop()} drives its own bounded final drain.
     */
    private void wakeFlusher() {
        ScheduledExecutorService s = this.scheduler;
        if (!running || s == null) {
            return;   // pre-start or post-stop: the timer / stop()'s final flush covers the queue.
        }
        try {
            s.execute(this::flushIfPending);
        } catch (RejectedExecutionException e) {
            // stop() raced this wakeup — its bounded final flush drains the queue. Never fall back to
            // flushing on the caller: that reintroduces the lane stall this method exists to remove.
        }
    }

    /**
     * Single-flight flush: the permit serialises the scheduler thread and threshold-
     * triggered submitters. A skipped flush is harmless — the in-flight one drains the
     * same queue and the 50ms timer retries.
     */
    private void flushIfPending() {
        if (queue.isEmpty() || !flushPermit.tryAcquire()) {
            return;
        }
        try {
            flush();
        } finally {
            flushPermit.release();
        }
    }

    @BoundaryHandler("outbox-flush-must-re-queue on ANY failure; the broad catch protects the whole batch from being lost")
    private void flush() {
        List<AlertIntent> batch = new ArrayList<>();
        queue.drainTo(batch, flushThreshold * 2);
        if (batch.isEmpty()) return;

        String sql = "INSERT INTO outbox(id, account_id, payload, status, attempts, traceparent, tracestate) "
                   + "VALUES (?::uuid, ?, ?::jsonb, 'PENDING', 0, ?, ?) ON CONFLICT DO NOTHING";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // SET LOCAL synchronous_commit=off: bounded 200ms WAL flush loss window.
            // Production upgrade: remove this statement for full synchronous durability (ADR-12).
            try (var stmt = conn.createStatement()) {
                stmt.setQueryTimeout(10);
                stmt.execute("SET LOCAL synchronous_commit=off");
            }
            try (var pstmt = conn.prepareStatement(sql)) {
                pstmt.setQueryTimeout(10);
                for (AlertIntent intent : batch) {
                    pstmt.setString(1, intent.alertId().toString());
                    pstmt.setString(2, intent.accountId());
                    pstmt.setString(3, intent.payload().toString());
                    // Nullable W3C trace context (ADR-25): setString(null) binds SQL NULL.
                    pstmt.setString(4, intent.traceparent());
                    pstmt.setString(5, intent.tracestate());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            conn.commit();
            meterRegistry.counter("rpe.outbox.flush.success").increment(batch.size());
            // Relay wakeup is cross-process: the V4 outbox AFTER-INSERT trigger fires
            // pg_notify('outbox_ready') once per batch, which the (separately deployed)
            // rpe-relay-service LISTENs on. The former in-process RelayTrigger.signal()
            // was the co-located fast-path only — a no-op once the relay is extracted
            // (ADR-05 / ADR-17 §7 Stage 1). Signal is an optimisation, not correctness:
            // the relay re-queries the outbox on every poll regardless.
        } catch (Exception e) {
            meterRegistry.counter("rpe.outbox.flush.failure").increment(batch.size());
            // Re-queue for the next flush cycle — deterministic alert_id + ON CONFLICT
            // DO NOTHING make re-inserting a partially-committed batch idempotent.
            int requeued = 0;
            for (AlertIntent intent : batch) {
                if (queue.offer(intent)) {
                    requeued++;
                } else {
                    meterRegistry.counter("rpe.outbox.dropped", "reason", "requeue_full").increment();
                    log.error("Outbox re-queue full: dropping alert intent alertId={}", intent.alertId());
                }
            }
            log.error("Outbox batch flush failed for {} intents ({} re-queued): {}",
                    batch.size(), requeued, e.getMessage(), e);
        }
    }
}
