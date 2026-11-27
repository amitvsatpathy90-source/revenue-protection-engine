package io.rpe.triage.agent;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.rpe.triage.config.BoundaryHandler;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Every provider round-trip goes through this decorator chain. The LLM is the
 * system's least reliable, most expensive outbound dependency (ADR-15).
 *
 * <p>Decorator order, innermost to outermost (binding design discipline):
 *
 * <pre>
 * LLM call -> Retry -> TimeLimiter -> Bulkhead -> RateLimiter -> CircuitBreaker
 * </pre>
 *
 * <p>Retry sits inside the TimeLimiter so all attempts share one 12-second
 * budget. The outermost CircuitBreaker observes the wall-clock duration of the
 * complete retried operation for slow-call-rate detection.
 */
@Component
public class TriageLlmClient {

    private static final Logger log = LoggerFactory.getLogger(TriageLlmClient.class);

    private final ChatModel chatModel;
    private final LlmResilience resilience;
    private final ExecutorService llmExecutor;
    private final MeterRegistry meterRegistry;
    private final String provider;

    public TriageLlmClient(
            ChatModel chatModel,
            LlmResilience resilience,
            @Qualifier("llmExecutor") ExecutorService llmExecutor,
            MeterRegistry meterRegistry,
            @Value("${triage.llm.provider-tag:openai}") String provider) {
        this.chatModel = chatModel;
        this.resilience = resilience;
        this.llmExecutor = llmExecutor;
        this.meterRegistry = meterRegistry;
        this.provider = provider;
    }

    /**
     * Returns a tool-calling options builder seeded from the model's default options.
     *
     * <p>Keeps the provider-specific options type. {@code OpenAiChatModel} casts
     * prompt options to {@code OpenAiChatOptions}, so a generic builder can cause
     * a {@code ClassCastException}.
     *
     * @return a builder derived from model defaults, or a generic builder when
     * the defaults do not implement {@link ToolCallingChatOptions}
     */
    ToolCallingChatOptions.Builder<?> newToolCallingOptionsBuilder() {
        return chatModel.getOptions() instanceof ToolCallingChatOptions options
                ? options.mutate()
                : ToolCallingChatOptions.builder();
    }

    /**
     * Executes one guarded provider call.
     *
     * <p>Maps terminal failures to {@link TriageAgentException} with a bounded
     * reason so callers can route to the degraded verdict. Retries are limited
     * to the transport/429/5xx policy configured by the inner Retry decorator.
     */
    @BoundaryHandler(
            "r4j-decorator-boundary: Callable.call() declares checked Exception; "
                    + "terminal failures map to TriageAgentException for degraded fallback")
    public ChatResponse call(Prompt prompt) {

        Supplier<ChatResponse> retried =
                Retry.decorateSupplier(
                        resilience.retry(),
                        () -> chatModel.call(prompt));

        // Retain the FutureTask so caller interruption can explicitly cancel it.
        FutureTask<ChatResponse> providerCall = new FutureTask<>(retried::get);

        /*
         * Use ExecutorService execution rather than CompletableFuture.supplyAsync.
         * FutureTask cancellation requests interruption of its worker; cancellation
         * of a CompletableFuture created by supplyAsync does not interrupt its
         * running task.
         *
         * Interruption is best-effort for the underlying HTTP request. The configured
         * 15-second OpenAI HTTP timeout provides the transport-level backstop.
         */
        Callable<ChatResponse> timeLimited =
                TimeLimiter.decorateFutureSupplier(
                        resilience.timeLimiter(),
                        () -> {
                            llmExecutor.execute(providerCall);
                            return providerCall;
                        });

        Callable<ChatResponse> guarded =
                CircuitBreaker.decorateCallable(
                        resilience.circuitBreaker(),
                        RateLimiter.decorateCallable(
                                resilience.rateLimiter(),
                                Bulkhead.decorateCallable(
                                        resilience.bulkhead(),
                                        timeLimited)));

        long start = System.nanoTime();
        String outcome = "success";

        try {
            ChatResponse response = guarded.call();
            logUsage(response);
            return response;

        } catch (CallNotPermittedException e) {
            outcome = "circuit_open";
            throw new TriageAgentException("circuit-open", e);

        } catch (RequestNotPermitted e) {
            outcome = "rate_limited";
            throw new TriageAgentException("rate-limited", e);

        } catch (BulkheadFullException e) {
            outcome = "bulkhead_full";
            throw new TriageAgentException("bulkhead-full", e);

        } catch (TimeoutException e) {
            outcome = "timeout";
            throw new TriageAgentException("timeout", e);

        } catch (InterruptedException e) {
            providerCall.cancel(true);
            Thread.currentThread().interrupt();

            outcome = "error";
            throw new TriageAgentException("llm-error", e);

        } catch (Exception e) {
            outcome = "error";
            throw new TriageAgentException("llm-error", e);

        } finally {
            // Records the LLM latency signal, categorized by provider and outcome.
            latencyTimer(outcome).record(
                    System.nanoTime() - start,
                    TimeUnit.NANOSECONDS);
        }
    }

    private Timer latencyTimer(String outcome) {
        return Timer.builder("triage.llm.latency")
                .tag("provider", provider)
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    /**
     * Logs token counts only. Prompt and completion bodies must never be logged
     * (security rules §6).
     */
    private void logUsage(ChatResponse response) {
        Usage usage = response.getMetadata().getUsage();
        log.debug("LLM usage promptTokens={} completionTokens={}",
                usage.getPromptTokens(), usage.getCompletionTokens());
    }
}
