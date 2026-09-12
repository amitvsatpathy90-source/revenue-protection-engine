package io.rpe.triage.agent;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.timelimiter.TimeLimiter;

/**
 * R4j instances for the RAG retrieval boundary (raw JDBC pgvector query).
 * Independent from {@link EmbeddingResilience}/{@link LlmResilience} (ADR-29/ADR-30) —
 * a slow/failing pgvector query must not trip the embedding or chat boundaries.
 *
 * No {@link io.github.resilience4j.ratelimiter.RateLimiter} — local Postgres call,
 * not a billed API, no per-account quota to protect. If a shared pgvector instance
 * later needs load shedding under concurrent triage load, that's a {@link Bulkhead}
 * sizing question, not a rate-limiting one — don't add one without a documented
 * reason for reversing this call (ADR-30 assumption-validation table).
 */
public record RetrievalResilience(
        CircuitBreaker circuitBreaker,
        Retry          retry,
        TimeLimiter    timeLimiter,
        Bulkhead       bulkhead) {}
