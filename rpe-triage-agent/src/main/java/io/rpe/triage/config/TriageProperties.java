package io.rpe.triage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Binding config for the triage module. The {@code llm} block is the Resilience4j
 * boundary spec from ai-triage-rules.md §5 — values are BINDING, no global defaults.
 */
@ConfigurationProperties(prefix = "triage")
public record TriageProperties(
        Llm    llm,
        Sweep  sweep,
        String promptVersion,
        Dev    dev) {

    public record Llm(
            long  timeLimitMs,          // 12s hard cap, cancelRunningFuture=true
            long  slowCallMs,           // slow ≥ 8s — degradation shows as latency first
            float slowRateThreshold,    // 50%
            float failureRateThreshold, // 50%
            int   slidingWindowSize,    // 20
            int   halfOpenCalls,        // 3
            long  waitOpenMs,           // CB open → half-open wait
            int   bulkheadConcurrent,   // 5 — cost + provider rate-limit protection
            int   rpm,                  // the billing breaker (TRIAGE_LLM_RPM env)
            int   retryMaxAttempts,     // 3 = initial + 2 retries, transport/429/5xx ONLY
            long  retryBackoffMs) {}    // exponential base 1s

    public record Sweep(Duration staleAfter, int batch) {}

    /** Dev-only prompt logging — off by default, never in compose defaults (rules §6). */
    public record Dev(boolean logPrompts) {}
}
