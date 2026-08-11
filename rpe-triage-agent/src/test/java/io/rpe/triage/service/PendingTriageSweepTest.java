package io.rpe.triage.service;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.domain.TriagedAlertMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sweep test (rules §2): rows stuck in PENDING_TRIAGE — the crash window between inbox
 * insert and verdict publish — are re-emitted as DEGRADED verdicts. The LLM is NEVER
 * re-called for them (the original result is gone; degraded is the correct answer).
 *
 * That guarantee is structural: {@link PendingTriageSweep} has no TriageAgent
 * dependency at all, so it cannot spend LLM budget. This test pins the behaviour.
 *
 * The sweep runs on EVERY instance, so the stale-row claim must be instance-exclusive
 * (FOR UPDATE SKIP LOCKED in the JDBC inbox; the in-memory inbox mirrors the atomic
 * claim). The concurrency test pins that contract: two racing sweeps over the same
 * rows emit exactly one verdict per alert_id.
 */
class PendingTriageSweepTest {

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final ObjectMapper objectMapper = TriageTestSupport.objectMapper();

    @Test
    void stalePendingRowsAreReEmittedAsDegradedVerdicts() {
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var sweep = new PendingTriageSweep(
                inbox, new DegradedTriageFallback(), publisher,
                objectMapper, registry, TriageTestSupport.lenientProps());

        UUID a1 = UUID.randomUUID();
        UUID a2 = UUID.randomUUID();
        inbox.seedPending(a1, "acct-1", "geo");
        inbox.seedPending(a2, "acct-2", "velocity");

        sweep.sweep();

        assertThat(publisher.published).hasSize(2);
        assertThat(publisher.published)
                .allSatisfy(v -> {
                    assertThat(v.triageStatus())
                            .isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);
                    assertThat(v.degradedReason()).isEqualTo("stale-pending-sweep");
                });
        // Rows transitioned out of PENDING_TRIAGE.
        assertThat(inbox.statusById.get(a1)).isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);
        assertThat(inbox.statusById.get(a2)).isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);
    }

    @Test
    void emptyPendingSetPublishesNothing() {
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var sweep = new PendingTriageSweep(
                inbox, new DegradedTriageFallback(), publisher,
                objectMapper, registry, TriageTestSupport.lenientProps());

        sweep.sweep();

        assertThat(publisher.published).isEmpty();
    }

    @Test
    void publishFailureLeavesRowPendingAndSweepsTheRest() {
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var sweep = new PendingTriageSweep(
                inbox, new DegradedTriageFallback(), publisher,
                objectMapper, registry, TriageTestSupport.lenientProps());

        UUID failing   = UUID.randomUUID();
        UUID surviving = UUID.randomUUID();
        inbox.seedPending(failing, "acct-1", "geo");
        inbox.seedPending(surviving, "acct-2", "velocity");
        publisher.failNext = true;   // first publish (the 'failing' row) throws

        sweep.sweep();

        // Per-row isolation: the failed row stays PENDING_TRIAGE, the rest sweep.
        assertThat(publisher.published).hasSize(1);
        assertThat(inbox.statusById.get(failing)).isEqualTo("PENDING_TRIAGE");
        assertThat(inbox.statusById.get(surviving))
                .isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);

        // Next cycle picks the failed row back up.
        sweep.sweep();
        assertThat(inbox.statusById.get(failing))
                .isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);
    }

    /**
     * The multi-instance race this suite exists to prevent: the sweep is @Scheduled
     * on every triage instance against the same triaged_alerts table. Two sweeps
     * racing over the same stale rows must partition them via the exclusive claim —
     * never double-publish. A small claim batch forces several overlapping claim
     * cycles per sweep; under the old plain-SELECT find, both sweeps would see the
     * same rows and this would fail with duplicate verdicts.
     */
    @Test
    void concurrentSweepsEmitExactlyOneVerdictPerAlertId() throws Exception {
        var inbox = new TriageTestSupport.InMemoryInbox();
        var topic = new TriageTestSupport.CapturingPublisher();   // shared payment.alerts.triaged
        var props = TriageTestSupport.sweepBatchProps(5);
        var sweepA = new PendingTriageSweep(
                inbox, new DegradedTriageFallback(), topic, objectMapper, registry, props);
        var sweepB = new PendingTriageSweep(
                inbox, new DegradedTriageFallback(), topic, objectMapper, registry, props);

        Set<UUID> seeded = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            UUID alertId = UUID.randomUUID();
            seeded.add(alertId);
            inbox.seedPending(alertId, "acct-" + i, "velocity");
        }

        // Each instance runs 4 scheduled cycles of batch 5 — 40 claim slots racing
        // for 20 rows. The barrier lines both instances up on every cycle.
        var barrier = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> a = pool.submit(() -> {
                for (int cycle = 0; cycle < 4; cycle++) {
                    barrier.await();
                    sweepA.sweep();
                }
                return null;
            });
            Future<?> b = pool.submit(() -> {
                for (int cycle = 0; cycle < 4; cycle++) {
                    barrier.await();
                    sweepB.sweep();
                }
                return null;
            });
            a.get(10, TimeUnit.SECONDS);
            b.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        // Exactly one verdict per alert_id — the claim is exclusive across instances.
        assertThat(topic.published).hasSize(20);
        assertThat(topic.published)
                .extracting(TriagedAlertMessage::alertId)
                .containsExactlyInAnyOrderElementsOf(seeded);
        // And every row left PENDING_TRIAGE exactly once.
        assertThat(seeded).allSatisfy(id ->
                assertThat(inbox.statusById.get(id))
                        .isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED));
    }

    /**
     * The mark UPDATE is guarded on triage_status = 'PENDING_TRIAGE' (WHERE clause in
     * the JDBC inbox; mirrored by the in-memory double): a consumer stalled past the
     * stale window whose mark lands AFTER a sweep already degraded the row must be a
     * no-op — the row leaves PENDING_TRIAGE exactly once, and a terminal status is
     * never overwritten.
     */
    @Test
    void lateConsumerMarkCannotOverwriteASweptRow() {
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var sweep = new PendingTriageSweep(
                inbox, new DegradedTriageFallback(), publisher,
                objectMapper, registry, TriageTestSupport.lenientProps());

        UUID alertId = UUID.randomUUID();
        inbox.seedPending(alertId, "acct-1", "velocity");

        // Sweep claims the stale row and transitions it to DEGRADED_RULE_BASED.
        sweep.sweep();
        assertThat(inbox.statusById.get(alertId))
                .isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);

        // The stalled consumer's mark arrives late — guarded UPDATE matches nothing.
        inbox.markTriaged(alertId, TriagedAlertMessage.STATUS_LLM_TRIAGED, "HIGH",
                "{}", "v1");

        assertThat(inbox.statusById.get(alertId))
                .isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);
    }
}
