package io.rpe.consumer;

import io.rpe.util.Pii;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Provides per-account single-threaded virtual-thread executors (lanes).
 *
 * Each account's events execute on a dedicated single-threaded lane:
 *   - Enforces per-account ordering (atomicity and ordering are different properties)
 *   - Required for geo read-before-write correctness
 *   - Caffeine bounds memory: evicts idle account lanes after 10 minutes
 *
 * {@code ArrayBlockingQueue(200)} + {@link BoundedBlockingSubmitPolicy} is the back-pressure
 * mechanism (ADR-07, amended 2026-07-06). When a lane's queue fills, the submitting thread
 * PARKS on the queue until capacity frees — the Kafka consumer thread cannot poll while
 * parked, so consumer lag rises visibly; ops team sees it. No external pause/resume needed.
 * The original {@code CallerRunsPolicy} is superseded: inline execution ran the task
 * CONCURRENTLY with the lane worker, breaking the single-writer invariant this class exists
 * to provide (two threads in the Redis gate for one account) and processing the inline event
 * out of offset order.
 *
 * Lane eviction is zero-cost: all account state lives in Redis, not in the lane.
 */
@Service
public class LaneExecutorService {

    private static final Logger log = LoggerFactory.getLogger(LaneExecutorService.class);

    private final Cache<String, ExecutorService> lanes;
    private final AtomicLong laneSequence = new AtomicLong();

    /**
     * Invoked with the account id whenever a lane leaves the cache (idle expiry, size
     * eviction, or explicit invalidate). Lets the owner prune any per-account bookkeeping
     * (e.g. the consumer's partition→account index) so it never outgrows this bounded cache.
     * Defaults to a no-op so removals occurring before registration are harmless.
     */
    private volatile Consumer<String> evictionListener = accountId -> {};

    public LaneExecutorService() {
        this.lanes = Caffeine.<String, ExecutorService>newBuilder()
                .maximumSize(50_000)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .removalListener((accountId, executor, cause) -> {
                    if (executor instanceof ExecutorService es) {
                        es.shutdown();
                        log.debug("Lane evicted for account ...{}", Pii.maskAccountId((String) accountId));
                    }
                    if (accountId instanceof String id) {
                        // Prune owner-side bookkeeping on the same eviction that frees the lane.
                        // Must never throw back into Caffeine maintenance — swallow + log.
                        try {
                            evictionListener.accept(id);
                        } catch (RuntimeException e) {
                            log.warn("Lane eviction listener failed for account ...{}",
                                    Pii.maskAccountId(id), e);
                        }
                    }
                })
                .build();
    }

    /**
     * Registers a callback fired when a lane leaves the cache. Single listener; the most
     * recent registration wins. Wired once at startup by {@code PaymentEventConsumer} to
     * keep its partition→account index bounded by this cache.
     */
    public void onEviction(Consumer<String> listener) {
        this.evictionListener = listener;
    }

    public ExecutorService getOrCreate(String accountId) {
        return lanes.get(accountId, id -> createLane());
    }

    /**
     * Removes the lane for {@code accountId} from the Caffeine cache so that the next
     * {@link #getOrCreate} call creates a fresh executor. Called after partition-scoped
     * drain on rebalance — without invalidation, the shut-down executor remains in the
     * cache and tasks for a rebalanced-back partition are silently dropped.
     */
    public void invalidate(String accountId) {
        lanes.invalidate(accountId);
    }

    /** Outcome of a global drain: total live lanes, how many terminated in time, and whether
     *  the bounded deadline was hit (remaining tasks abandoned). */
    public record DrainResult(int lanes, int drained, boolean forced) {}

    /**
     * Drains EVERY live lane within a bounded deadline. Called once at shutdown by
     * {@code LaneDrainLifecycle}, which runs AFTER the Kafka container has stopped — so no new
     * lane task can be submitted while this runs (the precondition for a clean drain).
     *
     * Per-account ordering is preserved across the drain: each lane is a single-threaded
     * executor, so {@code shutdown()} lets it finish its already-queued tasks in order before
     * terminating; only NEW submissions are rejected. Lanes are shut down first (all at once),
     * then awaited against a shared deadline, so one slow account cannot starve the others'
     * drain budget. The cache is invalidated at the end so a restarted instance rebuilds fresh
     * lanes (all account state lives in Redis — eviction is zero-cost).
     */
    public DrainResult drainAll(Duration timeout) {
        lanes.cleanUp();
        List<ExecutorService> live = new ArrayList<>(lanes.asMap().values());
        for (ExecutorService es : live) {
            es.shutdown();
        }
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        int drained = 0;
        boolean forced = false;
        for (ExecutorService es : live) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                forced = true;
                break;
            }
            try {
                if (es.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                    drained++;
                } else {
                    forced = true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                forced = true;
                break;
            }
        }
        lanes.invalidateAll();
        return new DrainResult(live.size(), drained, forced);
    }

    private ExecutorService createLane() {
        // Per-lane sequence — NOT the creator's threadId, which is the same Kafka
        // consumer thread for every lane and would name all lanes identically.
        // Account id stays out of thread names (PII rule; thread names reach dumps).
        String laneName = "rpe-lane-" + laneSequence.incrementAndGet();
        return new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(LANE_QUEUE_CAPACITY),
                r -> Thread.ofVirtual().name(laneName).unstarted(r),
                new BoundedBlockingSubmitPolicy(laneName));
    }

    static final int LANE_QUEUE_CAPACITY = 200;

    /** Park granularity while awaiting lane capacity; each slice is interruptible. */
    private static final long OFFER_SLICE_MS = 500;
    /** WARN cadence while parked: every N slices (~5s), so a wedged lane is ops-visible. */
    private static final int SLICES_PER_WARN = 10;

    /**
     * Saturation policy replacing {@code CallerRunsPolicy} (ADR-07 amendment, 2026-07-06).
     *
     * Inline execution on the submitting thread broke the per-account single-writer
     * invariant two ways: (a) the listener thread ran the Redis gate CONCURRENTLY with the
     * lane worker for the same account — geo read-before-write races (both read the same
     * prev location); (b) the inline event processed out of offset order. It also parked
     * the listener up to 5s inside RateLimiterService.tryAcquire — the exact anti-pattern
     * kafka-consumer.md marks WRONG.
     *
     * This policy parks the SUBMITTER on the queue until capacity frees instead. The
     * backpressure signature is identical (a parked listener cannot poll; consumer lag
     * rises), but the lane worker stays the only thread that ever executes account tasks,
     * so ordering and single-writer hold under saturation. The park never gives up on its
     * own: abandoning the task would surface as RejectedExecutionException → error-handler
     * retries → DLT of a valid, unrepeatable event (the exact reason ADR-07 rejected
     * AbortPolicy). It ends only when capacity frees, the lane is shut down (rebalance
     * drain — reject; asyncAcks holds the watermark and Kafka redelivers), or the thread
     * is interrupted (container shutdown — same redelivery safety net).
     *
     * One legitimate inline execution remains: the lane's OWN worker re-submitting to its
     * full lane (the CallNotPermittedException retry in processEvent racing the CB
     * NORMAL→BUFFERING flip). Parking there would deadlock the queue's only consumer.
     * Running inline is correct — same thread is still the single writer, and the retried
     * event is the account's oldest in flight, so it precedes everything queued.
     */
    private static final class BoundedBlockingSubmitPolicy implements RejectedExecutionHandler {

        private final String laneName;

        BoundedBlockingSubmitPolicy(String laneName) {
            this.laneName = laneName;
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if (laneName.equals(Thread.currentThread().getName())) {
                // Lane worker → own full lane: inline is the only non-deadlocking option
                // and preserves both single-writer (same thread) and offset order.
                task.run();
                return;
            }
            try {
                int slices = 0;
                while (!executor.isShutdown()) {
                    if (executor.getQueue().offer(task, OFFER_SLICE_MS, TimeUnit.MILLISECONDS)) {
                        if (slices >= SLICES_PER_WARN) {
                            log.info("Lane {} capacity freed after ~{}ms park", laneName,
                                    slices * OFFER_SLICE_MS);
                        }
                        return;
                    }
                    if (++slices % SLICES_PER_WARN == 0) {
                        log.warn("Lane {} queue full for ~{}ms — submitter '{}' parked "
                                        + "(backpressure: consumer cannot poll while parked)",
                                laneName, slices * OFFER_SLICE_MS,
                                Thread.currentThread().getName());
                    }
                }
                // Rebalance drain shut this lane down mid-park. The offset stays
                // unacknowledged; asyncAcks holds the watermark — Kafka redelivers.
                throw new RejectedExecutionException(
                        "lane " + laneName + " shut down while awaiting capacity");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException(
                        "interrupted while awaiting capacity on lane " + laneName, e);
            }
        }
    }
}
