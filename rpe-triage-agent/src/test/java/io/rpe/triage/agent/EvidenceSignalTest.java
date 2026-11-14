package io.rpe.triage.agent;

import io.rpe.triage.domain.Severity;
import io.rpe.triage.domain.TriageVerdict;
import io.rpe.triage.domain.TriageVerdict.Evidence;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the per-verdict signal classification and the drop counts it depends on.
 */
class EvidenceSignalTest {

    private final EvidenceValidator validator = new EvidenceValidator(new SimpleMeterRegistry());

    private static TriageVerdict verdict(List<Evidence> evidence) {
        return new TriageVerdict(Severity.MEDIUM, "n", evidence, 0.8);
    }

    @Test
    void fullyGroundedVerdictHasNoSignals() {
        var r = validator.validate(verdict(List.of(new Evidence("c1", "f"))), Set.of("c1"));

        assertThat(r.supplied()).isEqualTo(1);
        assertThat(r.dropped()).isZero();
        assertThat(EvidenceValidator.classify(r)).isEmpty();
    }

    @Test
    void partialDropIsFlaggedButNotAsStripped() {
        var r = validator.validate(
                verdict(List.of(new Evidence("c1", "f"), new Evidence("bad", "f"))), Set.of("c1"));

        assertThat(r.supplied()).isEqualTo(2);
        assertThat(r.dropped()).isEqualTo(1);
        assertThat(EvidenceValidator.classify(r)).containsExactly(VerdictSignal.EVIDENCE_PARTIAL_DROP);
    }

    @Test
    void totalDropIsStrippedNotPartial() {
        var r = validator.validate(verdict(List.of(new Evidence("bad", "f"))), Set.of("c1"));

        assertThat(r.dropped()).isEqualTo(1);
        assertThat(EvidenceValidator.classify(r)).containsExactly(VerdictSignal.EVIDENCE_STRIPPED);
    }

    @Test
    void emptyEvidenceIsObservedAndNotStripped() {
        var r = validator.validate(verdict(List.of()), Set.of());

        assertThat(r.evidenceStripped()).isFalse();   // keeps the existing contract
        assertThat(EvidenceValidator.classify(r)).containsExactly(VerdictSignal.NO_EVIDENCE);
    }
}
