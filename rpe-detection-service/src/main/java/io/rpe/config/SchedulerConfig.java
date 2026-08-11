package io.rpe.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class SchedulerConfig {

    private static final Logger log = LoggerFactory.getLogger(SchedulerConfig.class);

    static final String JDBC_THREAD_PREFIX = "rpe-jdbc-";

    // Pool references captured so they can be shut down at context close. Bean destruction runs
    // AFTER all SmartLifecycle.stop() — i.e. after the lane drain and the final outbox flush
    // (ADR-22 POOL phase) — so closing them here never cuts off in-flight drain/flush work.
    private ExecutorService lanePool;
    private ExecutorService jdbcPool;

    @PostConstruct
    public void configureReactor() {
        // Propagate MDC and Reactor Context across reactive thread hops (Spring Boot 3.2+)
        Hooks.enableAutomaticContextPropagation();
    }

    /**
     * Used in {@code .publishOn(laneScheduler)} after every reactive Redis op.
     *
     * Mandatory after ReactiveRedisTemplate.execute() — keeps result handlers off the
     * Lettuce I/O thread. A missing publishOn is a silent deadlock risk under high concurrency:
     * any downstream blocking call on the I/O thread deadlocks ALL Redis operations.
     */
    @Bean
    public Scheduler laneScheduler() {
        this.lanePool = Executors.newVirtualThreadPerTaskExecutor();
        return Schedulers.fromExecutor(lanePool);
    }

    /**
     * Dedicated BOUNDED platform-thread pool for all JDBC work (outbox batch writer, relay).
     *
     * PgJDBC uses {@code synchronized} internally. Running JDBC on virtual-thread lanes
     * pins carrier threads and degrades ALL VT concurrency across the JVM. This is a
     * JVM-level constraint, not a config choice (System Invariants).
     *
     * {@link BoundedBlockingJdbcSubmitPolicy} provides back-pressure when the pool is saturated:
     * the submitting thread (lane VT, scheduler VT) parks until a slot frees, throttling Postgres
     * load naturally — without ever executing JDBC on the submitter.
     */
    @Bean
    public Scheduler jdbcScheduler() {
        var pool = new ThreadPoolExecutor(
                20, 20, 0L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(500),
                r -> {
                    var t = new Thread(r);
                    t.setName(JDBC_THREAD_PREFIX + t.threadId());
                    t.setDaemon(true);
                    return t;
                },
                new BoundedBlockingJdbcSubmitPolicy());
        this.jdbcPool = pool;
        return Schedulers.fromExecutor(pool);
    }

    /**
     * Saturation policy for {@link #jdbcScheduler()}: park the submitter until a queue slot frees;
     * NEVER execute the task on it.
     *
     * <p>Supersedes {@code CallerRunsPolicy} (arch-audit). CallerRuns *executed* the rejected task on
     * the calling thread — which on the outbox hot path is a lane virtual thread — so under exactly
     * the Postgres backpressure the policy exists to absorb, PgJDBC's {@code synchronized} internals
     * pinned a VT carrier. That inverted the JDBC-on-dedicated-platform-pool constraint precisely when
     * it mattered most. The bean javadoc already claimed the submitter "blocks"; this makes it true.
     *
     * <p>Why not a JDK-native policy: none park. Abort/Discard/DiscardOldest reject or silently lose
     * an alert intent, and CallerRuns is the defect itself — so a custom handler is the only option
     * that preserves both the backpressure semantics and the threading constraint. Same shape as
     * {@code LaneExecutorService.BoundedBlockingSubmitPolicy}, minus the lane's inline escape hatch:
     * no pool thread submits back into this pool, so parking cannot self-deadlock. That precondition
     * is guarded below rather than assumed — a future self-submit fails loudly instead of wedging the
     * pool in silence.
     */
    private static final class BoundedBlockingJdbcSubmitPolicy implements RejectedExecutionHandler {

        /** Park granularity; each slice re-checks shutdown and stays interruptible. */
        private static final long OFFER_SLICE_MS = 500;
        /** WARN cadence while parked: every N slices (~5s), so a saturated pool is ops-visible. */
        private static final int SLICES_PER_WARN = 10;

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if (Thread.currentThread().getName().startsWith(JDBC_THREAD_PREFIX)) {
                // A pool thread submitting into its own full pool would park on a queue only it can
                // drain. No such path exists today; if one is ever added, fail loudly here rather
                // than deadlock the whole JDBC pool invisibly.
                throw new RejectedExecutionException(
                        "jdbcScheduler self-submit from pool thread would deadlock the pool");
            }
            try {
                int slices = 0;
                while (!executor.isShutdown()) {
                    if (executor.getQueue().offer(task, OFFER_SLICE_MS, TimeUnit.MILLISECONDS)) {
                        if (slices >= SLICES_PER_WARN) {
                            log.info("jdbcScheduler capacity freed after ~{}ms park",
                                    slices * OFFER_SLICE_MS);
                        }
                        return;
                    }
                    if (++slices % SLICES_PER_WARN == 0) {
                        log.warn("jdbcScheduler saturated for ~{}ms — submitter '{}' parked "
                                        + "(backpressure: Postgres is not keeping up)",
                                slices * OFFER_SLICE_MS, Thread.currentThread().getName());
                    }
                }
                // Shutdown raced the park. For an alert intent this surfaces to submitAlert as a
                // throw → processEvent's boundary catch → DLT; the offset is never acked silently.
                throw new RejectedExecutionException("jdbcScheduler shut down while awaiting capacity");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("interrupted while awaiting jdbcScheduler capacity", e);
            }
        }
    }

    /**
     * Graceful shutdown (ADR-22), POOL phase. Runs at bean destruction — after the lane drain
     * ({@code LaneDrainLifecycle}) and the final outbox flush ({@code OutboxBatchWriter.stop()}),
     * both of which complete during the earlier SmartLifecycle phases. {@code shutdown()} (not
     * {@code shutdownNow()}) lets any trailing task finish rather than interrupting JDBC
     * mid-statement; by this point the drain/flush work is already done, so there is no
     * meaningful backlog to await.
     */
    @PreDestroy
    public void shutdownPools() {
        if (jdbcPool != null) {
            jdbcPool.shutdown();
        }
        if (lanePool != null) {
            lanePool.shutdown();
        }
    }
}
