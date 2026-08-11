package io.rpe.triage.domain;

/**
 * Triage severity. Unknown values in LLM output fail Jackson enum binding —
 * a schema-validation failure that routes to the degraded verdict, never a retry.
 */
public enum Severity {
    LOW, MEDIUM, HIGH, CRITICAL
}
