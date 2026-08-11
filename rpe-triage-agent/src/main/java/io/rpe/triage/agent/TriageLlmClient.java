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
import org.springframework.beans.factory.annotation.Qualifier;
import io.rpe.triage.config.BoundaryHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Every provider round-trip goes through this decorator chain — the LLM is the
 * system's least reliable, most expensive outbound dependency (ADR-15).
 *
 * Decorator order, innermost → outermost (ai-triage-rules.md §5 — BINDING):
 *
 *   LLM call → Retry → TimeLimiter → Bulkhead → RateLimiter → CircuitBreaker
 *
 * Retry sits INSIDE the TimeLimiter so retries share one 12s budget — each retry
 * must not consume a fresh budget unobserved, and the CB (outermost) must see the
 * true wall-clock of the retried unit for slow-call-rate detection.
 */
@Component
public class TriageLlmClient {

    private static final Logger log = LoggerFactory.getLogger(TriageLlmClient.class);

    private final ChatModel       chatModel;
    private final LlmResilience   resilience;
    private final ExecutorService llmExecutor;
    private final MeterRegistry   meterRegistry;
    private final String          provider;

    public TriageLlmClient(
            ChatModel chatModel,
            LlmResilience resilience,
            @Qualifier("llmExecutor") ExecutorService llmExecutor,
            MeterRegistry meterRegistry,
            @Value("${triage.llm.provider-tag:openai}") String provider) {
        this.chatModel     = chatModel;
        this.resilience    = resilience;
        this.llmExecutor   = llmExecutor;
        this.meterRegistry = meterRegistry;
        this.provider      = provider;
    }

    /**
     * One guarded provider call. Maps every terminal failure to a
     * {@link TriageAgentException} with a bounded reason — callers route to the
     * degraded verdict; nothing here is retried beyond the inner Retry's
     * transport/429/5xx policy.
     */
    @BoundaryHandler("r4j-decorator-boundary: Callable.call() declares checked Exception; all terminal failures map to TriageAgentException for the degraded fallback")
    public ChatResponse call(Prompt prompt) {
        Supplier<ChatResponse> retried =
                Retry.decorateSupplier(resilience.retry(), () -> chatModel.call(prompt));
        // ExecutorService.submit (not CompletableFuture.supplyAsync): the TimeLimiter cancels this
        // Future with interrupt=true on timeout, and a FutureTask from submit() actually interrupts
        // its worker — a CompletableFuture from supplyAsync never does (its cancel cannot interrupt
        // the running task), so the abandoned call would keep blocking. Best-effort against OkHttp
        // (the hard bound is spring.ai.openai.chat.timeout=15s); together they cap the leak (RPE-02).
        Callable<ChatResponse> timeLimited = TimeLimiter.decorateFutureSupplier(
                resilience.timeLimiter(),
                () -> llmExecutor.submit(retried::get));
        Callable<ChatResponse> guarded =
                CircuitBreaker.decorateCallable(resilience.circuitBreaker(),
                        RateLimiter.decorateCallable(resilience.rateLimiter(),
                                Bulkhead.decorateCallable(resilience.bulkhead(), timeLimited)));

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
        } catch (Exception e) {
            outcome = "error";
            throw new TriageAgentException("llm-error", e);
        } finally {
            // Locked signal: triage_llm_latency_seconds{provider, outcome}
            latencyTimer(outcome).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Timer latencyTimer(String outcome) {
        return Timer.builder("triage.llm.latency")
                .tag("provider", provider)
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    /** Token counts only — never prompt or completion bodies (rules §6). */
    private void logUsage(ChatResponse response) {
        if (response.getMetadata() == null) return;
        Usage usage = response.getMetadata().getUsage();
        if (usage != null) {
            log.debug("LLM usage promptTokens={} completionTokens={}",
                    usage.getPromptTokens(), usage.getCompletionTokens());
        }
    }
}
