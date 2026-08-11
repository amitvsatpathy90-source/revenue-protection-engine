package io.rpe.triage.agent;

import io.rpe.triage.domain.TriageVerdict;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * Evidence grounding (ai-triage-rules.md §3): every {@code evidence[]} item must
 * reference a tool-call ID from THIS run. Unmatched items are dropped — an LLM
 * (or an injected instruction) cannot fabricate evidence that survives validation.
 *
 * If ALL items drop, confidence is downgraded and {@code evidence_stripped=true}
 * is flagged. The drop counter is a §6.2 injection-detection signal.
 */
@Component
public class EvidenceValidator {

    /** Confidence ceiling for a verdict whose entire evidence list was fabricated. */
    private static final double STRIPPED_CONFIDENCE_CEILING = 0.2;

    private final MeterRegistry meterRegistry;

    public EvidenceValidator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public record Result(TriageVerdict verdict, boolean evidenceStripped) {}

    public Result validate(TriageVerdict verdict, Set<String> executedToolCallIds) {
        List<TriageVerdict.Evidence> grounded = verdict.evidence().stream()
                .filter(item -> executedToolCallIds.contains(item.toolCallId()))
                .toList();

        int dropped = verdict.evidence().size() - grounded.size();
        if (dropped > 0) {
            meterRegistry.counter("triage.evidence.dropped").increment(dropped);
        }

        boolean allDropped = !verdict.evidence().isEmpty() && grounded.isEmpty();
        double confidence = allDropped
                ? Math.min(verdict.confidence(), STRIPPED_CONFIDENCE_CEILING)
                : verdict.confidence();

        return new Result(
                new TriageVerdict(verdict.severity(), verdict.narrative(), grounded, confidence),
                allDropped);
    }
}
