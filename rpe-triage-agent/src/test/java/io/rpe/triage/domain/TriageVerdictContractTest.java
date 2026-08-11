package io.rpe.triage.domain;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * §7 contract test — TriageVerdict schema validation.
 *
 * The structured-output schema is a prompt-injection defense (ADR-15 §8): a verdict
 * the model cannot bind, or that is missing required fields, must be REJECTED so it
 * routes to a degraded verdict rather than emitting garbage. Validation messages
 * describe field shape only, never values (event narrative may quote untrusted text).
 */
class TriageVerdictContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void validVerdictPassesValidation() {
        var verdict = new TriageVerdict(
                Severity.HIGH, "Impossible travel between two locations.", List.of(), 0.85);
        assertThatCode(verdict::validated).doesNotThrowAnyException();
    }

    @Test
    void unknownSeverityValueIsRejectedByBinding() throws Exception {
        // The model emitting a severity outside the enum must fail to bind — not
        // silently coerce. @JsonIgnoreProperties ignores unknown PROPERTIES, but an
        // unknown enum VALUE is still a hard binding error.
        String json = "{\"severity\":\"SUPER_CRITICAL\",\"narrative\":\"x\","
                    + "\"evidence\":[],\"confidence\":0.5}";
        assertThatThrownBy(() -> objectMapper.readValue(json, TriageVerdict.class))
                .isInstanceOf(JacksonException.class);
    }

    @Test
    void unknownExtraPropertyIsIgnored() {
        // Additive schema evolution (ADR-11): an extra field from a newer producer
        // must not break an older consumer.
        String json = "{\"severity\":\"LOW\",\"narrative\":\"x\",\"evidence\":[],"
                    + "\"confidence\":0.5,\"futureField\":\"ignored\"}";
        assertThatCode(() -> objectMapper.readValue(json, TriageVerdict.class))
                .doesNotThrowAnyException();
    }

    @Test
    void missingSeverityIsRejected() {
        assertThatThrownBy(() -> new TriageVerdict(null, "x", List.of(), 0.5).validated())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("severity");
    }

    @Test
    void blankNarrativeIsRejected() {
        assertThatThrownBy(() -> new TriageVerdict(Severity.LOW, "  ", List.of(), 0.5).validated())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("narrative");
    }

    @Test
    void nullEvidenceArrayIsRejected() {
        // Empty is fine; ABSENT is a contract violation — the difference matters for
        // the evidence-grounding validator downstream.
        assertThatThrownBy(() -> new TriageVerdict(Severity.LOW, "x", null, 0.5).validated())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("evidence");
    }

    @Test
    void confidenceOutOfRangeIsRejected() {
        assertThatThrownBy(() -> new TriageVerdict(Severity.LOW, "x", List.of(), 1.5).validated())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("confidence");
        assertThatThrownBy(() -> new TriageVerdict(Severity.LOW, "x", List.of(), -0.1).validated())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("confidence");
        assertThatThrownBy(() -> new TriageVerdict(Severity.LOW, "x", List.of(), Double.NaN).validated())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("confidence");
    }

    @Test
    void evidenceItemWithBlankToolCallIdIsRejected() {
        var verdict = new TriageVerdict(Severity.LOW, "x",
                List.of(new TriageVerdict.Evidence("  ", "some finding")), 0.5);
        assertThatThrownBy(verdict::validated)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("toolCallId");
    }

    @Test
    void validationMessagesNeverLeakFieldValues() {
        // PII rule: messages describe shape, not content. A narrative carrying
        // untrusted text must not surface in getMessage().
        String secret = "card-4111111111111111";
        assertThatThrownBy(() -> new TriageVerdict(null, secret, List.of(), 0.5).validated())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining(secret);
    }
}
