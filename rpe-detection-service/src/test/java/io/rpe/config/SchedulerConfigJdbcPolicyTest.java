package io.rpe.config;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the real jdbcScheduler pool + saturation policy; other outbox tests use Schedulers.immediate(). */
class SchedulerConfigJdbcPolicyTest {

    // Mirrors SchedulerConfig: 20 workers + 500 queue slots.
    private static final int POOL = 20;
    private static final int QUEUE = 500;

    private SchedulerConfig cfg;
    private Scheduler jdbc;

    // Holds workers to force saturation.
    private CountDownLatch hold;

    @BeforeEach
    void setUp() {
        cfg = new SchedulerConfig();
        jdbc = cfg.jdbcScheduler();
        hold = new CountDownLatch(1);
    }

    @AfterEach
    void tearDown() {
        hold.countDown();
        cfg.shutdownPools();
    }

    private static void await(CountDownLatch l) {
        try {
            l.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void saturate() {
        // Fill workers + queue so the next submit hits the rejection policy.
        for (int i = 0; i < POOL + QUEUE; i++) {
            jdbc.schedule(() -> await(hold));
        }
    }

    private static boolean parked(Thread t) {
        return t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING;
    }

    @Test
    void saturatedSubmitterParksThenRunsOnPoolThread() throws Exception {
        saturate();
        var ranOn = new AtomicReference<String>();
        var returned = new CountDownLatch(1);
        Thread submitter = Thread.ofVirtual().name("lane-vt").start(() -> {
            jdbc.schedule(() -> ranOn.set(Thread.currentThread().getName()));
            returned.countDown();
        });

        // parked, not rejected
        assertThat(returned.await(700, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(parked(submitter)).isTrue();

        // capacity frees
        hold.countDown();

        // Task must run on the dedicated JDBC pool.
        assertThat(returned.await(5, TimeUnit.SECONDS)).isTrue();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
                () -> assertThat(ranOn.get()).startsWith(SchedulerConfig.JDBC_THREAD_PREFIX));
    }

    @Test
    void shutdownWhileParkedThrowsRejected() throws Exception {
        saturate();
        var err = new AtomicReference<Throwable>();
        var done = new CountDownLatch(1);
        Thread submitter = Thread.ofVirtual().start(() -> {
            try {
                jdbc.schedule(() -> { });
            } catch (Throwable t) {
                err.set(t);
            }
            done.countDown();
        });
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> parked(submitter));

        // Parked submit must fail promptly on shutdown.
        cfg.shutdownPools();

        assertThat(done.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(err.get()).isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    void interruptWhileParkedThrowsAndRestoresFlag() throws Exception {
        saturate();
        var err = new AtomicReference<Throwable>();
        var flag = new AtomicBoolean();
        var done = new CountDownLatch(1);
        Thread submitter = Thread.ofVirtual().start(() -> {
            try {
                jdbc.schedule(() -> { });
            } catch (Throwable t) {
                err.set(t);
                flag.set(Thread.currentThread().isInterrupted());
            }
            done.countDown();
        });
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> parked(submitter));

        submitter.interrupt();

        assertThat(done.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(err.get()).isInstanceOf(RejectedExecutionException.class);
        assertThat(flag.get()).isTrue();
    }

    @Test
    void selfSubmitFromPoolThreadFailsLoudly() throws Exception {
        var go = new CountDownLatch(1);
        var err = new AtomicReference<Throwable>();
        var done = new CountDownLatch(1);
        for (int i = 0; i < POOL - 1; i++) {
            jdbc.schedule(() -> await(hold));
        }
        // 20th thread submits into its own full pool
        jdbc.schedule(() -> {
            await(go);
            try {
                jdbc.schedule(() -> { });
            } catch (Throwable t) {
                err.set(t);
            }
            done.countDown();
        });
        for (int i = 0; i < QUEUE; i++) {
            jdbc.schedule(() -> await(hold));
        }

        go.countDown();

        assertThat(done.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(err.get()).isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    void parksPastManySlicesWithoutDeadline() throws Exception {
        saturate();
        var err = new AtomicReference<Throwable>();
        var returned = new CountDownLatch(1);
        Thread s = Thread.ofVirtual().start(() -> {
            try {
                jdbc.schedule(() -> { });
                returned.countDown();
            } catch (Throwable t) {
                err.set(t);
            }
        });
        assertThat(returned.await(5_500, TimeUnit.MILLISECONDS)).isFalse(); // >10 slices
        assertThat(err.get()).isNull();    // not rejected
        assertThat(s.isAlive()).isTrue();  // still waiting, no flaky state check
    }
}
