package io.rpe.triage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Binding config for the triage module. The {@code llm} block is the Resilience4j
 */
@ConfigurationProperties(prefix = "triage")
public record TriageProperties(
        Llm    llm,
        Sweep  sweep,
        String promptVersion,
        Dev    dev,
        Rag    rag) {

    /** Additive ctor at the pre-ADR-30 4-arg shape — existing callers/fixtures compile unmodified. */
    public TriageProperties(Llm llm, Sweep sweep, String promptVersion, Dev dev) {
        this(llm, sweep, promptVersion, dev, null);
    }

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

    /** Dev-only prompt logging — off by default, never in compose defaults. */
    public record Dev(boolean logPrompts) {}

    /** ADR-29: embedding + retrieval get fully independent R4j config — no field sharing. */
    public record Rag(Embedding embedding, Retrieval retrieval) {

        public record Embedding(
                long  timeLimitMs,
                long  slowCallMs,
                float slowRateThreshold,
                float failureRateThreshold,
                int   slidingWindowSize,
                int   halfOpenCalls,
                long  waitOpenMs,
                int   bulkheadConcurrent,   // sourced: OpenAI Tier-1 headroom
                int   rpm,                  // sourced: OpenAI Tier-1 3000rpm ÷10 headroom
                int   retryMaxAttempts,
                long  retryBackoffMs) {}

        /** No rpm — local Postgres call, not a billed API, no RateLimiter on this boundary. */
        public record Retrieval(
                long   timeLimitMs,
                long   slowCallMs,
                float  slowRateThreshold,
                float  failureRateThreshold,
                int    slidingWindowSize,
                int    halfOpenCalls,
                long   waitOpenMs,
                int    bulkheadConcurrent,
                int    retryMaxAttempts,
                long   retryBackoffMs,
                int    topK,
                double similarityThreshold) {}
    }
}
