package io.rpe.triage.agent;

import io.rpe.triage.domain.TriageVerdict;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * <p>Evidence grounding (the triage agent design discipline): every
 * {@code evidence[]} item must reference a tool-call ID from this run.
 * Unmatched items are dropped; an LLM (or an injected instruction)
 * cannot fabricate evidence that survives validation.</p>
 *
 * <p>If all items are dropped, confidence is downgraded and
 * {@code evidence_stripped=true} is flagged. The drop counter is a
 * §6.2 injection-detection signal.</p>
 */
@Component
public class EvidenceValidator {

    /**
     * Confidence ceiling for a verdict whose entire evidence list was fabricated.
     */
    private static final double STRIPPED_CONFIDENCE_CEILING = 0.2;

    private final MeterRegistry meterRegistry;

    public EvidenceValidator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * @param supplied evidence items before filtering
     * @param dropped  items removed as ungrounded
     */
    public record Result(TriageVerdict verdict, boolean evidenceStripped, int supplied, int dropped) {
    }

    /**
     * Maps a result to diagnostic signals. Pure: no I/O, no metrics.
     *
     * @return unmodifiable set; empty for a fully grounded verdict
     */
    public static Set<VerdictSignal> classify(Result result) {
        EnumSet<VerdictSignal> signals = EnumSet.noneOf(VerdictSignal.class);
        if (result.evidenceStripped()) signals.add(VerdictSignal.EVIDENCE_STRIPPED);
        else if (result.dropped() > 0) signals.add(VerdictSignal.EVIDENCE_PARTIAL_DROP);   // stripped covers total
        if (result.supplied() == 0) signals.add(VerdictSignal.NO_EVIDENCE);
        return Collections.unmodifiableSet(signals);
    }

    public Result validate(TriageVerdict verdict, Set<String> executedToolCallIds) {
        List<TriageVerdict.Evidence> grounded = verdict.evidence().stream()
                .filter(item -> executedToolCallIds.contains(item.toolCallId()))
                .toList();

        int supplied = verdict.evidence().size();   // captured before filtering
        int dropped = supplied - grounded.size();

        if (dropped > 0) {
            meterRegistry.counter("triage.evidence.dropped").increment(dropped);
        }

        boolean allDropped = !verdict.evidence().isEmpty() && grounded.isEmpty();
        double confidence = allDropped
                ? Math.min(verdict.confidence(), STRIPPED_CONFIDENCE_CEILING)
                : verdict.confidence();

        return new Result(
                new TriageVerdict(verdict.severity(), verdict.narrative(), grounded, confidence),
                allDropped, supplied, dropped);
    }
}
