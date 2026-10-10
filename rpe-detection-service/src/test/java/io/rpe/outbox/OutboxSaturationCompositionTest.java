package io.rpe.outbox;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.rpe.config.SchedulerConfig;
import io.rpe.domain.AlertIntent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Pool wait then outbox wait, in that order, with the real pool and a real writer. */
class OutboxSaturationCompositionTest {

    private static final int POOL = 20, QUEUE = 500;

    private SchedulerConfig cfg;
    private Scheduler jdbc;
    private CountDownLatch hold;
    private OutboxBatchWriter writer;

    @BeforeEach
    void setUp() {
        cfg = new SchedulerConfig();
        jdbc = cfg.jdbcScheduler();
        hold = new CountDownLatch(1);
        writer = new OutboxBatchWriter(null, jdbc, new SimpleMeterRegistry(), 1, 100); // capacity 1, unstarted
    }

    @AfterEach
    void tearDown() {
        hold.countDown();
        cfg.shutdownPools();
    }

    private void saturatePool() {
        for (int i = 0; i < POOL + QUEUE; i++) {
            jdbc.schedule(() -> {
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    private static AlertIntent intent() {
        return new AlertIntent(UUID.randomUUID(), "acc", JsonNodeFactory.instance.objectNode(),
                Instant.now(), null, null);
    }

    @Test
    void bothFull_poolParkFirst_thenOutbox250ms() throws Exception {
        assertThat(writer.submit(intent())).isTrue();   // outbox now full
        saturatePool();

        var out = new AtomicReference<Boolean>();
        var failure = new AtomicReference<Throwable>();
        var startedAt = new AtomicLong();               // set when the callable starts on a pool thread
        var endedAt = new AtomicLong();
        var done = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {                // same call shape as submitAlert
            try {
                out.set(Mono.fromCallable(() -> {
                    startedAt.set(System.nanoTime());
                    try {
                        return writer.submit(intent());
                    } finally {
                        endedAt.set(System.nanoTime());
                    }
                }).subscribeOn(jdbc).block());
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                done.countDown();
            }
        });

        assertThat(done.await(1_500, TimeUnit.MILLISECONDS)).isFalse(); // parked past 250ms
        assertThat(startedAt.get()).isZero();                           // callable not started during park
        hold.countDown();

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull();
        assertThat(out.get()).isFalse();
        assertThat(TimeUnit.NANOSECONDS.toMillis(endedAt.get() - startedAt.get()))
                .isGreaterThanOrEqualTo(240);                           // the 250ms offer, isolated
    }
}
