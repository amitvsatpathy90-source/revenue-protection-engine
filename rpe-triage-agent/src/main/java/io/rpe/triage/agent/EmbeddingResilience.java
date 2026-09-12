package io.rpe.triage.agent;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.timelimiter.TimeLimiter;

/**
 * The five Resilience4j instances guarding the RAG embedding boundary (OpenAI
 * {@code text-embedding-3-small}), bundled so {@code RagEmbeddingClient}'s decorator
 * chain and its tests share one wiring shape — same pattern as {@link LlmResilience}.
 *
 * <p>ADR-29/ADR-30: this is a FULLY INDEPENDENT boundary from {@link LlmResilience}.
 * A trip here (bad embedding key, OpenAI embeddings outage) must never affect chat
 * triage, and vice versa. Do not share instances or registries between the two.
 *
 * <p>Carries a {@link RateLimiter} because embedding calls are billed, metered API
 * calls against a per-account OpenAI quota — same billing-breaker role the LLM
 * boundary's rate limiter plays (contrast {@link RetrievalResilience}, which has none).
 */
public record EmbeddingResilience(
        CircuitBreaker circuitBreaker,
        Retry          retry,
        TimeLimiter    timeLimiter,
        Bulkhead       bulkhead,
        RateLimiter    rateLimiter) {}
