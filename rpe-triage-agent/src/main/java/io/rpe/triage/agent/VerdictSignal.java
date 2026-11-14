package io.rpe.triage.agent;

/**
 * Per-verdict grounding observations. Diagnostic only: no gating, routing or calibration.
 */
public enum VerdictSignal {
    /**
     * Every supplied evidence item was dropped.
     */
    EVIDENCE_STRIPPED,
    /**
     * Some, but not all, supplied items were dropped as ungrounded.
     */
    EVIDENCE_PARTIAL_DROP,
    /**
     * No evidence supplied; may be legitimate for some alert types.
     */
    NO_EVIDENCE
}
