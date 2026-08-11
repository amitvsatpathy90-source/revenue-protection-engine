package io.rpe.triage.agent;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.timelimiter.TimeLimiter;

/**
 * The five Resilience4j instances guarding the LLM boundary, bundled so the
 * decorator chain in {@link TriageLlmClient} and its tests share one wiring shape.
 */
public record LlmResilience(
        CircuitBreaker circuitBreaker,
        Retry          retry,
        TimeLimiter    timeLimiter,
        Bulkhead       bulkhead,
        RateLimiter    rateLimiter) {}
