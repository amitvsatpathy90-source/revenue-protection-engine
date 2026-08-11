package io.rpe.triage.consumer;

/**
 * Structurally-deserializable alert missing required identity fields.
 * Deterministic per record — registered not-retryable; routes to DLT immediately.
 * Message carries field-presence shape only, never field values.
 */
public class TriageValidationException extends RuntimeException {

    public TriageValidationException(boolean alertIdPresent, boolean accountIdPresent,
                                     boolean ruleNamePresent) {
        super("alert missing required field(s): alertId=%b accountId=%b ruleName=%b"
                .formatted(alertIdPresent, accountIdPresent, ruleNamePresent));
    }
}
