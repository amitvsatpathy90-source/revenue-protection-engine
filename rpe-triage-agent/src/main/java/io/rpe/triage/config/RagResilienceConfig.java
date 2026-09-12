package io.rpe.triage.config;

import io.rpe.triage.agent.EmbeddingResilience;
import io.rpe.triage.agent.RetrievalResilience;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedBulkheadMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRateLimiterMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.micrometer.core.instrument.MeterRegistry;
import com.openai.errors.OpenAIIoException;
import com.openai.errors.OpenAIRetryableException;
import com.openai.errors.OpenAIServiceException;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.time.Duration;

/**
 * Builds the two RAG R4j boundaries from {@code triage.rag.*} (ADR-30). Fully
 * isolated registries per boundary — no shared config, no cross-boundary metric
 * collision (ADR-29). Mirrors {@code LlmResilienceConfig}'s per-boundary discipline
 * (architecture design: one Resilience4j instance set per external dependency,
 * zero global defaults).
 */
@Configuration
public class RagResilienceConfig {

    private static final Logger log = LoggerFactory.getLogger(RagResilienceConfig.class);

    private static final String EMBEDDING_NAME = "rag-embedding";
    private static final String RETRIEVAL_NAME = "rag-retrieval";

    /**
     * Embedding boundary — OpenAI text-embedding-3-small, same SDK and same
     * {@code com.openai.errors.*} exception hierarchy as the chat LLM call. CB is
     * slow-call-rate based in addition to error-rate based for the same reason as
     * {@code LlmResilienceConfig}'s chat CB: provider degradation surfaces as
     * latency before it surfaces as errors.
     */
    @Bean
    public EmbeddingResilience embeddingResilience(TriageProperties props, MeterRegistry meterRegistry) {
        TriageProperties.Rag.Embedding p = props.rag().embedding();

        var cbRegistry = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slowCallDurationThreshold(Duration.ofMillis(p.slowCallMs()))
                .slowCallRateThreshold(p.slowRateThreshold())
                .failureRateThreshold(p.failureRateThreshold())
                .slidingWindowSize(p.slidingWindowSize())
                .minimumNumberOfCalls(p.slidingWindowSize())
                .permittedNumberOfCallsInHalfOpenState(p.halfOpenCalls())
                .waitDurationInOpenState(Duration.ofMillis(p.waitOpenMs()))
                .build());

        var retryRegistry = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(p.retryMaxAttempts())
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(p.retryBackoffMs(), 2.0, 0.5))  // jittered — avoid synchronized retry waves (RPE-07)
                .retryOnException(RagResilienceConfig::isRetryableEmbeddingException)
                .build());

        var timeLimiter = TimeLimiter.of(EMBEDDING_NAME, TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(p.timeLimitMs()))
                .cancelRunningFuture(true)
                .build());

        var bulkheadRegistry = BulkheadRegistry.of(BulkheadConfig.custom()
                .maxConcurrentCalls(p.bulkheadConcurrent())
                .maxWaitDuration(Duration.ZERO)
                .build());

        // Billing breaker, independent of the LLM boundary's rpm — per-instance, not global (ADR-15/29).
        var rateLimiterRegistry = RateLimiterRegistry.of(RateLimiterConfig.custom()
                .limitForPeriod(p.rpm())
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .timeoutDuration(Duration.ZERO)
                .build());

        CircuitBreaker cb       = cbRegistry.circuitBreaker(EMBEDDING_NAME);
        Retry retry             = retryRegistry.retry(EMBEDDING_NAME);
        Bulkhead bulkhead       = bulkheadRegistry.bulkhead(EMBEDDING_NAME);
        RateLimiter rateLimiter = rateLimiterRegistry.rateLimiter(EMBEDDING_NAME);

        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(cbRegistry).bindTo(meterRegistry);
        TaggedRetryMetrics.ofRetryRegistry(retryRegistry).bindTo(meterRegistry);
        TaggedBulkheadMetrics.ofBulkheadRegistry(bulkheadRegistry).bindTo(meterRegistry);
        TaggedRateLimiterMetrics.ofRateLimiterRegistry(rateLimiterRegistry).bindTo(meterRegistry);

        cb.getEventPublisher().onStateTransition(event ->
                log.warn("RAG embedding circuit breaker {}: {}", EMBEDDING_NAME, event.getStateTransition()));

        return new EmbeddingResilience(cb, retry, timeLimiter, bulkhead, rateLimiter);
    }

    /**
     * Retrieval boundary — raw JDBC pgvector query. No RateLimiter (see
     * {@link RetrievalResilience}). Thresholds are separately tunable from embedding's
     * because the failure shape is different (DB contention, not provider throttling) —
     * do not copy embedding's numbers here without re-deriving them; both sets are
     * PROVISIONAL pending real latency samples (ADR-30 open item).
     */
    @Bean
    public RetrievalResilience retrievalResilience(TriageProperties props, MeterRegistry meterRegistry) {
        TriageProperties.Rag.Retrieval p = props.rag().retrieval();

        var cbRegistry = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slowCallDurationThreshold(Duration.ofMillis(p.slowCallMs()))
                .slowCallRateThreshold(p.slowRateThreshold())
                .failureRateThreshold(p.failureRateThreshold())
                .slidingWindowSize(p.slidingWindowSize())
                .minimumNumberOfCalls(p.slidingWindowSize())
                .permittedNumberOfCallsInHalfOpenState(p.halfOpenCalls())
                .waitDurationInOpenState(Duration.ofMillis(p.waitOpenMs()))
                .build());

        var retryRegistry = RetryRegistry.of(RetryConfig.custom()
                .maxAttempts(p.retryMaxAttempts())
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(p.retryBackoffMs(), 2.0, 0.5))
                .retryOnException(RagResilienceConfig::isRetryableRetrievalException)
                .build());

        var timeLimiter = TimeLimiter.of(RETRIEVAL_NAME, TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(p.timeLimitMs()))
                .cancelRunningFuture(true)
                .build());

        var bulkheadRegistry = BulkheadRegistry.of(BulkheadConfig.custom()
                .maxConcurrentCalls(p.bulkheadConcurrent())
                .maxWaitDuration(Duration.ZERO)
                .build());

        CircuitBreaker cb = cbRegistry.circuitBreaker(RETRIEVAL_NAME);
        Retry retry       = retryRegistry.retry(RETRIEVAL_NAME);
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(RETRIEVAL_NAME);

        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(cbRegistry).bindTo(meterRegistry);
        TaggedRetryMetrics.ofRetryRegistry(retryRegistry).bindTo(meterRegistry);
        TaggedBulkheadMetrics.ofBulkheadRegistry(bulkheadRegistry).bindTo(meterRegistry);

        cb.getEventPublisher().onStateTransition(event ->
                log.warn("RAG retrieval circuit breaker {}: {}", RETRIEVAL_NAME, event.getStateTransition()));

        return new RetrievalResilience(cb, retry, timeLimiter, bulkhead);
    }

    /**
     * Identical exception hierarchy to LlmResilienceConfig's chat classifier — Spring
     * AI's OpenAiEmbeddingModel goes through the same official OpenAI Java SDK as
     * OpenAiChatModel. Retry ONLY transport/IO, SDK-declared retryable, 429, and 5xx.
     * Never 4xx content/auth — deterministic, retrying is pure spend (architecture design).
     */
    static boolean isRetryableEmbeddingException(Throwable t) {
        if (t instanceof OpenAIIoException) return true;
        if (t instanceof OpenAIRetryableException) return true;
        if (t instanceof OpenAIServiceException svc) {
            int status = svc.statusCode();
            return status == 429 || status >= 500;
        }
        return false;
    }

    /**
     * Retry ONLY transient connection conditions (timeout / 08xxx SQLSTATE) — never
     * syntax/permission/constraint failures (42xxx/23xxx), which are deterministic bugs.
     * SELECT-only, so retry is idempotency-safe by construction. Same class of bug as
     * this boundary's LLM-side counterpart's first, broken retry predicate — a wrong
     * exception-class assumption silently defeats retry coverage.
     *
     * <p><b>Verified against JdbcTriageVectorRepository:</b> JdbcTemplate translates every
     * JDBC exception into Spring's DataAccessException hierarchy before it reaches this
     * predicate — the original PSQLException is only reachable via getCause(). An earlier
     * version of this method checked PSQLException directly and was dead code against the
     * real call site. Fixed by checking TransientDataAccessException first, then unwrapping
     * getCause() for the 08xxx case. See {@code RagRetrievalRetryClassificationTest}.
     */
    static boolean isRetryableRetrievalException(Throwable t) {
        if (t instanceof org.springframework.dao.TransientDataAccessException) return true;
        if (t instanceof SQLTimeoutException) return true;
        if (t instanceof SQLTransientException) return true;
        if (t instanceof PSQLException pe) {
            String sqlState = pe.getSQLState();
            return sqlState != null && sqlState.startsWith("08");
        }
        // Spring wraps the real driver exception as the cause — check one level down.
        Throwable cause = t.getCause();
        if (cause instanceof PSQLException pe) {
            String sqlState = pe.getSQLState();
            return sqlState != null && sqlState.startsWith("08");
        }
        return false;
    }
}
