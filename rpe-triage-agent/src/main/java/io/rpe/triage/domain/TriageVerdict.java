package io.rpe.triage.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * The LLM's structured output — exactly what the model produces, nothing more.
 * Bound via Spring AI structured output; any parse or validation failure routes
 * to the degraded verdict and is NEVER retried (same input ⇒ same garbage,
 * doubled cost — ai-triage-rules.md §3).
 *
 * Every {@code evidence} item must reference a tool-call ID from THIS run;
 * {@code EvidenceValidator} drops unmatched items (prompt-injection containment).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TriageVerdict(
        Severity       severity,
        String         narrative,
        List<Evidence> evidence,
        double         confidence
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Evidence(String toolCallId, String finding) {}

    /**
     * Post-binding schema validation. Jackson leaves missing fields null — a verdict
     * with nulls is a contract violation, not a usable answer.
     *
     * @throws IllegalArgumentException on any violation; message describes the
     *         violation shape only (field names), never field VALUES — narrative
     *         text may quote untrusted event data.
     */
    public TriageVerdict validated() {
        if (severity == null) {
            throw new IllegalArgumentException("verdict missing severity");
        }
        if (narrative == null || narrative.isBlank()) {
            throw new IllegalArgumentException("verdict missing narrative");
        }
        if (evidence == null) {
            throw new IllegalArgumentException("verdict missing evidence array (may be empty, not absent)");
        }
        if (confidence < 0.0 || confidence > 1.0 || Double.isNaN(confidence)) {
            throw new IllegalArgumentException("verdict confidence out of [0,1]");
        }
        for (Evidence item : evidence) {
            if (item == null || item.toolCallId() == null || item.toolCallId().isBlank()) {
                throw new IllegalArgumentException("evidence item missing toolCallId");
            }
        }
        return this;
    }
}
