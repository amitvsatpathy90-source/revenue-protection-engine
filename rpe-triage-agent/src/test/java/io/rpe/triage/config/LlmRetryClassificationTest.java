package io.rpe.triage.config;

import io.rpe.triage.agent.LlmResilience;
import com.openai.core.http.Headers;
import com.openai.errors.BadRequestException;
import com.openai.errors.InternalServerException;
import com.openai.errors.OpenAIIoException;
import com.openai.errors.RateLimitException;
import io.github.resilience4j.retry.Retry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RPE-10 regression guard. Spring AI 2.0's {@code OpenAiChatModel} calls the official OpenAI Java
 * SDK ({@code com.openai.client.OpenAIClient}) and propagates its {@code com.openai.errors.*}
 * hierarchy unwrapped — NOT Spring Web's {@code HttpClientErrorException}/{@code HttpServerErrorException}.
 * The retry predicate once matched the Spring Web types, so it matched nothing the SDK actually
 * throws: every real 429/5xx/transport error was misclassified non-retryable and the R4j Retry was
 * inert. This test drives the classification with <b>real SDK exception instances</b> (empirical,
 * not {@code instanceof} tautology) both directly and through the real wired {@link LlmResilience}.
 *
 * <p>No Mockito / no network — same discipline as the rest of the triage suite (§7).
 */
class LlmRetryClassificationTest {

    /** The SDK's service-exception builders require headers; an empty set is fine for classification. */
    private static final Headers EMPTY_HEADERS = Headers.builder().build();

    private final MeterRegistry registry = new SimpleMeterRegistry();

    // ── Part A: the classification predicate against real SDK exceptions ──────────

    @Test
    void transport429And5xxAreRetryable() {
        assertThat(LlmResilienceConfig.isRetryableLlmException(
                new OpenAIIoException("connection reset")))
                .as("transport/IO failure").isTrue();
        assertThat(LlmResilienceConfig.isRetryableLlmException(
                RateLimitException.builder().headers(EMPTY_HEADERS).build()))
                .as("HTTP 429 throttle").isTrue();
        assertThat(LlmResilienceConfig.isRetryableLlmException(
                InternalServerException.builder().statusCode(503).headers(EMPTY_HEADERS).build()))
                .as("HTTP 5xx server error").isTrue();
    }

    @Test
    void contentAuthAndParseErrorsAreNotRetryable() {
        assertThat(LlmResilienceConfig.isRetryableLlmException(
                BadRequestException.builder().headers(EMPTY_HEADERS).build()))
                .as("HTTP 400 — deterministic content error").isFalse();
        assertThat(LlmResilienceConfig.isRetryableLlmException(
                new RuntimeException("structured-output parse failure")))
                .as("post-call parse failure — retrying is pure spend").isFalse();
    }

    // ── Part B: the SAME classification through the real wired Retry ──────────────

    @Test
    void wiredRetryRetries429ToTheCapThenFails() {
        Retry retry = resilienceWithAttempts(3).retry();
        AtomicInteger calls = new AtomicInteger();
        Supplier<String> throttled = Retry.decorateSupplier(retry, () -> {
            calls.incrementAndGet();
            throw RateLimitException.builder().headers(EMPTY_HEADERS).build();     // 429 — must be retried
        });

        assertThatThrownBy(throttled::get).isInstanceOf(RateLimitException.class);
        assertThat(calls.get())
                .as("429 retried: initial + 2 retries = 3 invocations")
                .isEqualTo(3);
    }

    @Test
    void wiredRetryDoesNotRetry4xxContentErrors() {
        Retry retry = resilienceWithAttempts(3).retry();
        AtomicInteger calls = new AtomicInteger();
        Supplier<String> badRequest = Retry.decorateSupplier(retry, () -> {
            calls.incrementAndGet();
            throw BadRequestException.builder().headers(EMPTY_HEADERS).build();    // 400 — must NOT be retried
        });

        assertThatThrownBy(badRequest::get).isInstanceOf(BadRequestException.class);
        assertThat(calls.get())
                .as("400 not retried: exactly 1 invocation")
                .isEqualTo(1);
    }

    /** Real {@link LlmResilienceConfig} wiring, retry cap overridden, tiny backoff to stay fast. */
    private LlmResilience resilienceWithAttempts(int attempts) {
        TriageProperties props = new TriageProperties(
                new TriageProperties.Llm(
                        5000, 4000, 50f, 50f, 100, 3, 10_000, 5, 100_000, attempts, 5),
                new TriageProperties.Sweep(Duration.ofMinutes(5), 50),
                "v1",
                new TriageProperties.Dev(false));
        return new LlmResilienceConfig().llmResilience(props, registry);
    }
}
