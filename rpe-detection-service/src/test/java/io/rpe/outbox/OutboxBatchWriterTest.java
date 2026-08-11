package io.rpe.outbox;

import io.rpe.domain.AlertIntent;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the outbox batch writer's failure semantics — the data-loss-adjacent paths
 * (loud drop on queue-full, idempotent re-queue on flush failure) and the buffered counter.
 */
class OutboxBatchWriterTest {

    private static AlertIntent intent() {
        return new AlertIntent(UUID.randomUUID(), "acct",
                JsonNodeFactory.instance.objectNode(), Instant.now(),
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", null);
    }

    private static double count(MeterRegistry reg, String name, String... tags) {
        var c = reg.find(name).tags(tags).counter();
        return c == null ? 0.0 : c.count();
    }

    @Test
    void submitBuffersThenDropsWhenQueueFull() {
        // capacity 2, threshold above capacity so submit() never triggers a flush → fully
        // deterministic, no DataSource interaction.
        MeterRegistry reg = new SimpleMeterRegistry();
        DataSource ds = mock(DataSource.class);
        OutboxBatchWriter w = new OutboxBatchWriter(ds, Schedulers.immediate(), reg, 2, 100);

        w.submit(intent()); // buffered = 1
        w.submit(intent()); // buffered = 2
        w.submit(intent()); // queue full → dropped = 1

        assertThat(count(reg, "rpe.hotpath.alerts.buffered")).isEqualTo(2.0);
        assertThat(count(reg, "rpe.outbox.dropped", "reason", "queue_full")).isEqualTo(1.0);
        verifyNoInteractions(ds); // no flush was triggered
    }

    @Test
    void flushWritesBatchAndCommitsOnSuccess() throws Exception {
        MeterRegistry reg = new SimpleMeterRegistry();
        Connection conn = mock(Connection.class);
        Statement setLocal = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(setLocal);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeBatch()).thenReturn(new int[]{1});

        OutboxBatchWriter w = new OutboxBatchWriter(ds, Schedulers.immediate(), reg, 10_000, 100);
        w.start();
        try {
            w.submit(intent());
            Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(count(reg, "rpe.outbox.flush.success")).isEqualTo(1.0));
        } finally {
            w.stop();
        }
        verify(conn, atLeastOnce()).commit();
    }

    @Test
    void flushFailureRequeuesWithoutDropping() throws Exception {
        MeterRegistry reg = new SimpleMeterRegistry();
        Connection conn = mock(Connection.class);
        Statement setLocal = mock(Statement.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(setLocal);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeBatch()).thenThrow(new SQLException("postgres down"));

        OutboxBatchWriter w = new OutboxBatchWriter(ds, Schedulers.immediate(), reg, 10_000, 100);
        w.start();
        try {
            w.submit(intent());
            // The 50ms scheduler flushes, fails, and re-queues — failure counter advances.
            Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(count(reg, "rpe.outbox.flush.failure")).isGreaterThanOrEqualTo(1.0));

            // Re-queue, not drop: the intent is preserved (idempotent ON CONFLICT re-insert).
            assertThat(count(reg, "rpe.outbox.dropped", "reason", "requeue_full")).isZero();
            assertThat(count(reg, "rpe.outbox.flush.success")).isZero();
            verify(conn, never()).commit();
        } finally {
            w.stop();
        }
    }

    /**
     * Failure-injection for finding #5 (arch-audit 2026-07-16): the threshold-triggered flush must
     * run on the flusher thread, NOT inline on the submitting caller — which on the hot path is a
     * lane VT parked in submitAlert's block(). The pre-fix code called {@code flushIfPending()}
     * inline, so a slow Postgres blocked the lane inside {@code getConnection()} up to ~20s, its
     * lane queue filled, the listener parked, and the whole consumer stalled. Architecture Spec's contract
     * ("ack gated on a bounded O(1) enqueue — lane never waits for the Postgres write itself")
     * depends on this staying off-caller.
     *
     * Injection: a DataSource whose {@code getConnection()} blocks (stuck Postgres). With a
     * flush threshold of 1, the first {@code submit()} triggers a flush. The assertion is that
     * {@code submit()} STILL returns in O(1) time while the flush is blocked in JDBC on another
     * thread. Negative-tested: reverting {@code wakeFlusher()} back to an inline
     * {@code flushIfPending()} makes {@code submit()} block on the latch and the elapsed assertion
     * fails.
     */
    @Test
    void submitAtThresholdDoesNotBlockTheCallerOnTheJdbcFlush() throws Exception {
        CountDownLatch flushReachedJdbc = new CountDownLatch(1);
        CountDownLatch releaseJdbc      = new CountDownLatch(1);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenAnswer(inv -> {
            flushReachedJdbc.countDown();                 // the flush got as far as JDBC…
            releaseJdbc.await(5, TimeUnit.SECONDS);        // …then blocks, like a stuck Postgres
            throw new SQLException("stuck postgres released");
        });

        MeterRegistry reg = new SimpleMeterRegistry();
        // Flush threshold = 1 so the first submit triggers a flush immediately.
        OutboxBatchWriter w = new OutboxBatchWriter(ds, Schedulers.immediate(), reg, 10_000, 1);
        w.start();
        try {
            long startNanos = System.nanoTime();
            boolean enqueued = w.submit(intent());
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertThat(enqueued).as("the intent is durably enqueued via an O(1) offer").isTrue();
            assertThat(elapsedMs)
                    .as("submit() must return without waiting for the JDBC flush — the flush runs "
                            + "on the flusher thread. Inline flush blocked the caller (a lane VT) "
                            + "inside getConnection() until Postgres responded (finding #5).")
                    .isLessThan(500L);
            assertThat(flushReachedJdbc.await(2, TimeUnit.SECONDS))
                    .as("the flush still happens — just on the flusher thread, proven by it "
                            + "reaching getConnection() off the caller")
                    .isTrue();
        } finally {
            releaseJdbc.countDown();
            w.stop();
        }
    }
}
