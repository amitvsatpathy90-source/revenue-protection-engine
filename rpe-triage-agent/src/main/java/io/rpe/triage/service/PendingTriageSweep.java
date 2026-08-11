package io.rpe.triage.service;

import io.rpe.triage.config.TriageProperties;
import io.rpe.triage.domain.TriagedAlertMessage;
import io.rpe.triage.inbox.TriageInbox;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rpe.triage.config.BoundaryHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Re-emits DEGRADED verdicts for rows stuck in PENDING_TRIAGE — the crash window
 * between inbox insert and verdict publish (ai-triage-rules.md §2).
 *
 * NEVER re-calls the LLM for swept rows: the inbox row already exists, the original
 * LLM result is gone, and degraded is the correct answer. Re-calling would turn
 * every crash into duplicate spend.
 *
 * The sweep runs on EVERY triage instance, so the stale-row claim must be
 * instance-exclusive: {@link TriageInbox#sweepStalePending} claims rows with
 * {@code FOR UPDATE SKIP LOCKED} and marks them in the same transaction. A plain
 * find-then-mark here would have N instances publish N duplicate DEGRADED verdicts
 * for the same rows every cycle.
 */
@Component
@BoundaryHandler("scheduled-sweep per-row isolation: one bad row must not starve the batch; logs and the next cycle retries")
public class PendingTriageSweep {

    private static final Logger log = LoggerFactory.getLogger(PendingTriageSweep.class);

    private final TriageInbox             inbox;
    private final DegradedTriageFallback  fallback;
    private final TriagedVerdictPublisher publisher;
    private final ObjectMapper            objectMapper;
    private final MeterRegistry           meterRegistry;
    private final TriageProperties        props;

    public PendingTriageSweep(
            TriageInbox inbox,
            DegradedTriageFallback fallback,
            TriagedVerdictPublisher publisher,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            TriageProperties props) {
        this.inbox         = inbox;
        this.fallback      = fallback;
        this.publisher     = publisher;
        this.objectMapper  = objectMapper;
        this.meterRegistry = meterRegistry;
        this.props         = props;
    }

    @Scheduled(fixedDelay = 60_000, initialDelay = 30_000)
    public void sweep() {
        int swept = inbox.sweepStalePending(
                props.sweep().staleAfter(), props.sweep().batch(), this::emitDegraded);
        if (swept > 0) {
            log.warn("Swept {} stale PENDING_TRIAGE row(s) — degraded verdicts emitted", swept);
        }
    }

    /**
     * Runs per exclusively claimed row, inside the inbox's sweep transaction:
     * publish (confirmed) THEN return the mark — same ordering invariant as the
     * consumer path. Mark-then-publish would silently lose the verdict forever.
     */
    private TriageInbox.Mark emitDegraded(TriageInbox.PendingRow row) {
        try {
            TriagedAlertMessage verdict = fallback.degraded(
                    row.alertId(), row.accountId(), row.ruleName(),
                    "stale-pending-sweep", props.promptVersion());
            publisher.publish(verdict);
            meterRegistry.counter("triage.verdicts", "triage_status", verdict.triageStatus())
                    .increment();
            return new TriageInbox.Mark(
                    verdict.triageStatus(),
                    verdict.severity().name(),
                    objectMapper.writeValueAsString(verdict),
                    verdict.promptVersion());
        } catch (Exception e) {
            // null leaves the row PENDING_TRIAGE; next sweep retries. Per-row
            // isolation — one bad row must not starve the rest of the batch.
            log.error("Sweep failed for alertId={} — will retry next cycle", row.alertId(), e);
            return null;
        }
    }
}
