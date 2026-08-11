package io.rpe.consumer;

/**
 * A detector fired but the alert intent could NOT be durably enqueued (outbox queue
 * saturated past its backpressure window — sustained Postgres outage). The event is
 * preserved on {@code payment.events.DLT}, but preserving the event alone is not enough:
 * the gate already marked the event's dedup key (step 4 runs before Java sees the result),
 * so a re-drive within the dedup TTL is dedup-blocked and never re-runs the detectors —
 * and past the TTL the velocity window has moved, so the original rule won't re-fire.
 *
 * This exception therefore carries the detection outcome to the DLT record as headers
 * (via the {@code DeadLetterPublishingRecoverer} headers function in {@code KafkaConfig}),
 * making the record self-sufficient: on re-drive, {@code PaymentEventConsumer} reconstructs
 * the alert intent deterministically from {@code eventId + ruleName} (UUIDv5 is
 * recomputable) and submits it straight to the outbox — no gate re-run, no state
 * pollution; {@code processed_alerts ON CONFLICT} absorbs any duplicate.
 *
 * The exception message deliberately carries the rule name only — {@code reason} may
 * contain amount/speed detail and rides in the header + alert payload, never in a
 * message that reaches exception fingerprints (security.md).
 */
public class AlertNotDurableException extends RuntimeException {

    /** Value of {@link #OUTCOME_HEADER} identifying a reconstructable firing. */
    public static final String OUTCOME_ALERT_UNDURABLE = "ALERT_UNDURABLE";

    /** DLT header: detection outcome at failure time. */
    public static final String OUTCOME_HEADER = "x-rpe-outcome";
    /** DLT header: the firing detector's ruleName — the UUIDv5 alert_id input. */
    public static final String RULE_HEADER = "x-rpe-rule";
    /** DLT header: the firing detector's human-readable reason. */
    public static final String REASON_HEADER = "x-rpe-reason";

    private final String ruleName;
    private final String reason;

    public AlertNotDurableException(String ruleName, String reason) {
        super("alert not durably enqueued (outbox saturated): rule=" + ruleName);
        this.ruleName = ruleName;
        this.reason = reason;
    }

    public String ruleName() { return ruleName; }

    public String reason() { return reason; }
}
