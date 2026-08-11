package io.rpe.triage.service;

import io.rpe.triage.domain.PaymentAlert;
import io.rpe.triage.domain.Severity;
import io.rpe.triage.domain.TriagedAlertMessage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Rule-based degraded verdict — the path that must work with the LLM permanently
 * absent (ADR-15 §3.7).
 *
 * HARD CONSTRAINT (ai-triage-rules.md §5): this class has ZERO Spring AI imports.
 * It must compile and run with the Spring AI dependency deleted from the pom.
 * Do not add Spring AI types here — route LLM-shaped logic to TriageAgent instead.
 */
@Component
public class DegradedTriageFallback {

    /** Static rule → severity map. Calibration is unvalidated (synthetic data) — see Architecture Spec limitations. */
    private static final Map<String, Severity> SEVERITY_BY_RULE = Map.of(
            "geo",      Severity.HIGH,    // impossible travel = strongest single signal
            "velocity", Severity.MEDIUM,
            "zscore",   Severity.MEDIUM);

    private static final Severity DEFAULT_SEVERITY = Severity.MEDIUM;

    public TriagedAlertMessage degraded(PaymentAlert alert, String degradedReason, String promptVersion) {
        return build(alert.alertId(), alert.eventId(), alert.accountId(), alert.ruleName(),
                degradedReason, promptVersion);
    }

    /** Sweep path — only inbox columns are available for stale PENDING_TRIAGE rows. */
    public TriagedAlertMessage degraded(UUID alertId, String accountId, String ruleName,
                                        String degradedReason, String promptVersion) {
        return build(alertId, null, accountId, ruleName, degradedReason, promptVersion);
    }

    private TriagedAlertMessage build(UUID alertId, String eventId, String accountId,
                                      String ruleName, String degradedReason, String promptVersion) {
        Severity severity = SEVERITY_BY_RULE.getOrDefault(ruleName, DEFAULT_SEVERITY);
        return new TriagedAlertMessage(
                alertId,
                eventId,
                accountId,
                ruleName,
                severity,
                "Rule-based severity (LLM triage unavailable): %s alert.".formatted(ruleName),
                List.of(),                 // no tool evidence on the degraded path
                0.0,                       // advisory confidence floor
                TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED,
                promptVersion,
                false,
                0,
                degradedReason,
                Instant.now(),
                TriagedAlertMessage.SCHEMA_VERSION);
    }
}
