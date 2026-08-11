package io.rpe.triage.inbox;

import java.time.Duration;
import java.util.UUID;

/**
 * Inbox-pattern idempotency store over {@code triaged_alerts} (ADR-15 §3.5).
 *
 * The ordering invariant this interface exists to enforce (ai-triage-rules.md §2):
 * {@link #tryInsert} runs BEFORE the LLM call; a conflict means the alert was already
 * triaged (or is being triaged) — skip, commit offset, spend nothing.
 *
 * Interface (not just the JDBC class) so the replay test can verify the invariant
 * with an in-memory implementation — Mockito is unusable on this JVM.
 */
public interface TriageInbox {

    /**
     * {@code INSERT ... ON CONFLICT DO NOTHING} keyed on the deterministic UUIDv5
     * alert_id. @return true if this call inserted the row (we own the triage);
     * false on conflict (duplicate delivery — skip).
     */
    boolean tryInsert(UUID alertId, String accountId, String ruleName);

    /**
     * Records the final verdict. Row leaves PENDING_TRIAGE exactly once — the
     * UPDATE is guarded on {@code triage_status = 'PENDING_TRIAGE'}, so a mark
     * arriving after the row was already transitioned (a stale-pending sweep won
     * the race against a stalled consumer) is a no-op, never an overwrite of a
     * terminal status.
     */
    void markTriaged(UUID alertId, String triageStatus, String severity, String verdictJson,
                     String promptVersion);

    /**
     * Claims up to {@code limit} rows stuck in PENDING_TRIAGE longer than
     * {@code staleAfter} — the crash window between inbox insert and verdict
     * produce — and applies {@code action} to each within the SAME exclusive
     * claim. The claim must be instance-exclusive ({@code FOR UPDATE SKIP LOCKED}
     * in the JDBC implementation): the sweep runs on every triage instance, and a
     * shared plain SELECT would have N instances emit N duplicate DEGRADED
     * verdicts for the same rows.
     *
     * <p>Per row: {@code action} publishes first, then the returned {@link Mark}
     * is recorded — publish-then-mark, same ordering invariant as the consumer
     * path. A {@code null} return leaves the row PENDING_TRIAGE for the next
     * cycle ({@code action} owns logging that failure). The sweep NEVER re-calls
     * the LLM (the inbox row exists, the LLM result is gone, degraded is the
     * correct answer).
     *
     * @return number of rows swept (marked out of PENDING_TRIAGE)
     */
    int sweepStalePending(Duration staleAfter, int limit, SweepAction action);

    /** Publish-then-mark callback for one exclusively claimed stale row. */
    @FunctionalInterface
    interface SweepAction {
        /** @return the mark to record, or {@code null} to leave the row PENDING_TRIAGE. */
        Mark process(PendingRow row);
    }

    record PendingRow(UUID alertId, String accountId, String ruleName) {}

    /** Verdict fields recorded when a swept row leaves PENDING_TRIAGE. */
    record Mark(String triageStatus, String severity, String verdictJson, String promptVersion) {}
}
