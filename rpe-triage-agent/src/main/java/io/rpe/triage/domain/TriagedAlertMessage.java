package io.rpe.triage.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Envelope published to {@code payment.alerts.triaged}.
 *
 * Advisory metadata ONLY — nothing in this message gates detection, alert identity,
 * or delivery (Architecture Spec immutable constraint). {@code schemaVersion} enables additive
 * evolution for downstream consumers (ADR-11 semantics).
 *
 * {@code triageStatus} values: {@code LLM_TRIAGED} | {@code DEGRADED_RULE_BASED}.
 * ({@code PENDING_TRIAGE} exists only as an inbox row state, never on the topic.)
 *
 * ADR-30: ragContextUsed is a new trailing component recording whether retrieved
 * corpus context was present at triage time. It is metadata only — it does not
 * gate evidence grounding; TriageVerdict/EvidenceValidator remain tool-call-
 * evidence-only. Additive-safe under the existing @JsonIgnoreProperties, no
 * SCHEMA_VERSION bump required.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TriagedAlertMessage(
        UUID     alertId,
        String   eventId,
        String   accountId,
        String   ruleName,
        Severity severity,
        String   narrative,
        List<TriageVerdict.Evidence> evidence,
        double   confidence,
        String   triageStatus,
        String   promptVersion,
        boolean  evidenceStripped,
        int      toolRounds,
        String   degradedReason,   // null on the LLM path
        Instant  triagedAt,
        String   schemaVersion,
        boolean  ragContextUsed    // ADR-30 — true iff RAG context was non-empty at triage time
) {
    /**
     * ADDITIVE constructor at the pre-ADR-30 15-arg shape — keeps every existing
     * construction site compiling unmodified, defaulting ragContextUsed to false.
     */
    public TriagedAlertMessage(
            UUID alertId, String eventId, String accountId, String ruleName,
            Severity severity, String narrative, List<TriageVerdict.Evidence> evidence,
            double confidence, String triageStatus, String promptVersion,
            boolean evidenceStripped, int toolRounds, String degradedReason,
            Instant triagedAt, String schemaVersion) {
        this(alertId, eventId, accountId, ruleName, severity, narrative, evidence,
                confidence, triageStatus, promptVersion, evidenceStripped, toolRounds,
                degradedReason, triagedAt, schemaVersion, false);
    }

    public static final String STATUS_LLM_TRIAGED        = "LLM_TRIAGED";
    public static final String STATUS_DEGRADED_RULE_BASED = "DEGRADED_RULE_BASED";
    public static final String SCHEMA_VERSION             = "1";
}
