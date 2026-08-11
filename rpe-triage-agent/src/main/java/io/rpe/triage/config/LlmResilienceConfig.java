package io.rpe.triage.config;

import io.rpe.triage.agent.LlmResilience;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Builds the LLM Resilience4j boundary from {@code triage.llm.*} (ai-triage-rules.md §5).
 * Per-boundary config, no global defaults — same discipline as the core module.
 *
 * The CB is slow-call-rate based IN ADDITION to error rate: provider degradation
 * shows as latency before errors (ADR-15 §6.1); an error-rate-only CB is blind to it.
 */
@Configuration
public class LlmResilienceConfig {

    private static final Logger log = LoggerFactory.getLogger(LlmResilienceConfig.class);

    private static final String NAME = "llm";

    @Bean
    public LlmResilience llmResilience(TriageProperties props, MeterRegistry meterRegistry) {
        TriageProperties.Llm p = props.llm();

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
                // Exponential backoff WITH jitter (randomization 0.5): a deterministic schedule
                // makes every replica retry a provider 429 on the same 1s/2s beats — synchronized
                // waves that re-trip the provider's limiter and feed the slow-call CB correlated
                // samples. Jitter de-correlates them (RPE-07).
                .intervalFunction(
                        IntervalFunction.ofExponentialRandomBackoff(p.retryBackoffMs(), 2.0, 0.5))
                // ONLY transport/429/5xx — see isRetryableLlmException. Content/auth failures (4xx)
                // and parse failures are deterministic; retrying them is pure spend (rules §5).
                .retryOnException(LlmResilienceConfig::isRetryableLlmException)
                .build());

        var timeLimiter = TimeLimiter.of(NAME, TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(p.timeLimitMs()))
                .cancelRunningFuture(true)   // abandoned VTs must not accumulate silently
                .build());

        var bulkheadRegistry = BulkheadRegistry.of(BulkheadConfig.custom()
                .maxConcurrentCalls(p.bulkheadConcurrent())
                .maxWaitDuration(Duration.ZERO)   // reject immediately → degraded verdict
                .build());

        var rateLimiterRegistry = RateLimiterRegistry.of(RateLimiterConfig.custom()
                .limitForPeriod(p.rpm())
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .timeoutDuration(Duration.ZERO)   // the billing breaker rejects, never queues
                .build());

        CircuitBreaker cb       = cbRegistry.circuitBreaker(NAME);
        Retry retry             = retryRegistry.retry(NAME);
        Bulkhead bulkhead       = bulkheadRegistry.bulkhead(NAME);
        RateLimiter rateLimiter = rateLimiterRegistry.rateLimiter(NAME);

        // Locked observability signal: resilience4j_circuitbreaker_state{name="llm"}
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(cbRegistry).bindTo(meterRegistry);
        TaggedRetryMetrics.ofRetryRegistry(retryRegistry).bindTo(meterRegistry);
        TaggedBulkheadMetrics.ofBulkheadRegistry(bulkheadRegistry).bindTo(meterRegistry);
        TaggedRateLimiterMetrics.ofRateLimiterRegistry(rateLimiterRegistry).bindTo(meterRegistry);

        cb.getEventPublisher().onStateTransition(event ->
                log.warn("LLM circuit breaker {}: {}", NAME, event.getStateTransition()));

        return new LlmResilience(cb, retry, timeLimiter, bulkhead, rateLimiter);
    }

    /**
     * Retry classification for the LLM boundary — retry ONLY transient failures: transport/IO,
     * SDK-declared retryable, HTTP 429 (throttle), and 5xx (server). Never 4xx content/auth
     * (400/401/403/404/422) — those are deterministic; retrying is pure spend (rules §5).
     *
     * <p><b>Why this exists as a named, tested method:</b> Spring AI 2.0's {@code OpenAiChatModel}
     * calls the <em>official</em> OpenAI Java SDK ({@code com.openai.client.OpenAIClient}, OkHttp),
     * NOT Spring's {@code RestClient}. It propagates the SDK's own {@code com.openai.errors.*}
     * hierarchy unwrapped (verified: {@code OpenAiChatModel} has zero references to
     * {@code org.springframework.web.client.Http*}). The previous predicate matched
     * {@code HttpClientErrorException.TooManyRequests}/{@code HttpServerErrorException} — Spring Web
     * types the SDK never throws — so it matched nothing: every real 429/5xx/transport error was
     * misclassified non-retryable and the Retry boundary was inert. Pinned by
     * {@code LlmRetryClassificationTest} against real SDK exception instances.
     */
    static boolean isRetryableLlmException(Throwable t) {
        if (t instanceof OpenAIIoException) {
            return true;                                   // transport (connect/read/reset)
        }
        if (t instanceof OpenAIRetryableException) {
            return true;                                   // SDK-declared retryable
        }
        if (t instanceof OpenAIServiceException svc) {
            int status = svc.statusCode();
            return status == 429 || status >= 500;         // throttle + server; NOT 4xx content/auth
        }
        return false;
    }
}
