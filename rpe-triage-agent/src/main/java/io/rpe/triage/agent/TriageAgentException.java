package io.rpe.triage.agent;

/**
 * Terminal LLM-path failure. {@code reason} is a bounded outcome enum-string
 * (circuit-open | rate-limited | bulkhead-full | timeout | llm-error |
 * schema-validation | round-budget-exhausted) — used as the degraded verdict's
 * {@code degradedReason} and in logs. Never carries prompt or completion content.
 */
public class TriageAgentException extends RuntimeException {

    private final String reason;

    public TriageAgentException(String reason) {
        super("triage agent failure: " + reason);
        this.reason = reason;
    }

    public TriageAgentException(String reason, Throwable cause) {
        super("triage agent failure: " + reason, cause);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
