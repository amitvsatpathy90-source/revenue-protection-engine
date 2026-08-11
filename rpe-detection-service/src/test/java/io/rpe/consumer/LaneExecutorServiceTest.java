package io.rpe.consumer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the lane cache, its eviction callback, and the bounded-blocking saturation
 * policy (ADR-07 amendment): under queue saturation the submitter parks — it never executes
 * the task inline, so the lane worker stays the account's single writer and offset order
 * holds. The pre-amendment {@code CallerRunsPolicy} fails these tests by design: it ran the
 * overflow task on the submitting thread, concurrently and out of order.
 */
class LaneExecutorServiceTest {

    @Test
    void getOrCreateReturnsSameLaneForSameAccount() {
        LaneExecutorService svc = new LaneExecutorService();
        var lane1 = svc.getOrCreate("acct-1");
        var lane2 = svc.getOrCreate("acct-1");
        assertThat(lane1).isSameAs(lane2);
    }

    @Test
    void evictionListenerFiresOnExplicitInvalidate() {
        LaneExecutorService svc = new LaneExecutorService();
        Set<String> evicted = ConcurrentHashMap.newKeySet();
        svc.onEviction(evicted::add);

        svc.getOrCreate("acct-1");
        svc.invalidate("acct-1");

        // Caffeine runs removal listeners asynchronously after maintenance.
        Awaitility.await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(evicted).contains("acct-1"));
    }

    @Test
    void evictionListenerThatThrowsDoesNotBreakTheCache() {
        LaneExecutorService svc = new LaneExecutorService();
        svc.onEviction(id -> { throw new RuntimeException("boom"); });

        svc.getOrCreate("acct-1");
        svc.invalidate("acct-1");

        // A throwing listener must be swallowed; the cache stays usable for new lanes.
        Awaitility.await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(svc.getOrCreate("acct-2")).isNotNull());
    }

    // ── Bounded-blocking saturation policy (ADR-07 amendment) ─────────────────

    /**
     * The invariant test: saturate one account's lane past queue capacity and prove the
     * overflow submit PARKS the submitter instead of running inline. CallerRunsPolicy fails
     * all three assertions: the submitter keeps advancing past capacity+1, two threads run
     * account tasks concurrently (max concurrency 2), and the overflow task executes before
     * the 200 queued ones (offset order inverted).
     */
    @Test
    void saturatedLaneParksSubmitterAndNeverRunsSameAccountTasksConcurrently() throws Exception {
        int capacity = LaneExecutorService.LANE_QUEUE_CAPACITY;
        int total = capacity + 5;                    // 1 running + 200 queued + 4 overflow

        LaneExecutorService svc = new LaneExecutorService();
        ExecutorService lane = svc.getOrCreate("acct-hot");

        AtomicInteger active        = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        AtomicInteger submitted     = new AtomicInteger();
        List<Integer> executionOrder = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);

        Thread submitter = new Thread(() -> IntStream.range(0, total).forEach(i -> {
            lane.submit(() -> {
                maxConcurrent.accumulateAndGet(active.incrementAndGet(), Math::max);
                executionOrder.add(i);
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                active.decrementAndGet();
            });
            submitted.incrementAndGet();
        }), "test-listener-thread");
        submitter.start();

        // Task 0 occupies the worker; 1..200 fill the queue; submit #202 must park.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(submitted.get()).isEqualTo(capacity + 1));

        // Hold longer than one offer slice: the submitter must STAY parked (no inline run,
        // no sneak-through) while the lane is saturated.
        Thread.sleep(700);
        assertThat(submitted.get())
                .as("submitter must park on the full queue, not execute inline and move on")
                .isEqualTo(capacity + 1);
        assertThat(active.get())
                .as("only the lane worker may run account tasks while saturated")
                .isEqualTo(1);

        release.countDown();
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(executionOrder).hasSize(total));
        submitter.join(5_000);

        assertThat(maxConcurrent.get())
                .as("two threads ran tasks for one account — single-writer invariant broken")
                .isEqualTo(1);
        assertThat(executionOrder)
                .as("overflow tasks must run after the queued ones (offset order)")
                .isEqualTo(IntStream.range(0, total).boxed().toList());
    }

    /**
     * The one legitimate inline execution: the lane's own worker re-submitting to its full
     * lane (the CB-open retry path racing the BUFFERING flip). Parking there would deadlock
     * the queue's only consumer — the task must run inline on the SAME lane thread, ahead
     * of the queued tasks (it is the account's oldest in-flight event).
     */
    @Test
    void laneWorkerSubmittingToItsOwnFullLaneRunsInlineWithoutDeadlock() throws Exception {
        int capacity = LaneExecutorService.LANE_QUEUE_CAPACITY;

        LaneExecutorService svc = new LaneExecutorService();
        ExecutorService lane = svc.getOrCreate("acct-self");

        List<String> executionOrder = new CopyOnWriteArrayList<>();
        AtomicReference<String> markerThread = new AtomicReference<>();
        CountDownLatch workerRunning = new CountDownLatch(1);
        CountDownLatch queueFilled   = new CountDownLatch(1);

        lane.submit(() -> {
            workerRunning.countDown();
            try {
                queueFilled.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Queue is now full: this self-submit must run inline, not park (deadlock).
            lane.submit(() -> {
                markerThread.set(Thread.currentThread().getName());
                executionOrder.add("marker");
            });
        });

        assertThat(workerRunning.await(5, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < capacity; i++) {
            int idx = i;
            lane.submit(() -> executionOrder.add("filler-" + idx));
        }
        queueFilled.countDown();

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(executionOrder).hasSize(capacity + 1));
        assertThat(markerThread.get())
                .as("self-submit must execute on the lane's own worker thread")
                .startsWith("rpe-lane-");
        assertThat(executionOrder.get(0))
                .as("the retried (oldest) event must precede everything queued")
                .isEqualTo("marker");
    }

    /**
     * A submitter parked on a saturated lane must exit via RejectedExecutionException with
     * the interrupt flag restored (container shutdown path — the uncommitted offset is the
     * redelivery safety net), never swallow the interrupt or park forever.
     */
    @Test
    void interruptWhileParkedThrowsRejectedAndRestoresInterruptFlag() throws Exception {
        int capacity = LaneExecutorService.LANE_QUEUE_CAPACITY;

        LaneExecutorService svc = new LaneExecutorService();
        ExecutorService lane = svc.getOrCreate("acct-parked");

        CountDownLatch release = new CountDownLatch(1);
        Runnable hold = () -> {
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        for (int i = 0; i < capacity + 1; i++) {
            lane.submit(hold);
        }

        AtomicReference<Boolean> rejected            = new AtomicReference<>(false);
        AtomicReference<Boolean> interruptRestored   = new AtomicReference<>(false);
        Thread parked = new Thread(() -> {
            try {
                lane.submit(() -> {});
            } catch (java.util.concurrent.RejectedExecutionException e) {
                rejected.set(true);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            }
        }, "test-parked-submitter");
        parked.start();

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> parked.getState() == Thread.State.TIMED_WAITING);
        parked.interrupt();
        parked.join(5_000);

        assertThat(rejected.get()).as("interrupt must surface as RejectedExecutionException").isTrue();
        assertThat(interruptRestored.get()).as("interrupt flag must be restored").isTrue();
        release.countDown();
    }
}
