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
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingOptions;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import io.rpe.triage.config.BoundaryHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Guarded embedding call — independent R4j instance set from {@link TriageLlmClient}
 * (ADR-29/ADR-30). Decorator order: embed() → Retry → TimeLimiter → Bulkhead →
 * RateLimiter → CircuitBreaker, same shape as the LLM boundary.
 *
 * <p><b>Caller contract (ADR-30):</b> {@code text} must be fixed, known-safe alert
 * fields only — never raw attacker-controlled free-text (merchant/memo). Enforced
 * by the caller ({@code TriageService.buildFixedFieldQuery}), not here.
 *
 * <p>API verified against Spring AI 2.0.0's actual EmbeddingModel/EmbeddingRequest/
 * EmbeddingResponse/Embedding classes this session, not from memory — cross-version
 * SDK shape is easy to get wrong.
 */
@Component
public class RagEmbeddingClient {

    private static final Logger log = LoggerFactory.getLogger(RagEmbeddingClient.class);

    private final EmbeddingModel      embeddingModel;
    private final EmbeddingResilience resilience;
    private final ExecutorService     ragExecutor;
    private final MeterRegistry       meterRegistry;
    private final String              provider;

    public RagEmbeddingClient(
            EmbeddingModel embeddingModel,
            EmbeddingResilience resilience,
            @Qualifier("ragExecutor") ExecutorService ragExecutor,   // separate from llmExecutor for incident attribution
            MeterRegistry meterRegistry,
            @Value("${triage.rag.embedding.provider-tag:openai}") String provider) {
        this.embeddingModel = embeddingModel;
        this.resilience     = resilience;
        this.ragExecutor    = ragExecutor;
        this.meterRegistry  = meterRegistry;
        this.provider       = provider;
    }

    /** @return query embedding vector; terminal failures map to TriageAgentException (caller skips RAG) */
    @BoundaryHandler("r4j-decorator-boundary: all terminal failures map to TriageAgentException so callers can skip RAG and proceed unaugmented")
    public float[] embed(String text) {
        Supplier<EmbeddingResponse> retried = Retry.decorateSupplier(
                resilience.retry(),
                // Pass an empty instantiated options object to satisfy provider null-checks
                // and completely avoid the risk of an NPE.
                () -> embeddingModel.call(new EmbeddingRequest(List.of(text), EmbeddingOptions.builder().build())));

        Callable<EmbeddingResponse> timeLimited = TimeLimiter.decorateFutureSupplier(
                resilience.timeLimiter(),
                () -> ragExecutor.submit(retried::get));   // submit(), not supplyAsync — TimeLimiter needs a cancellable Future

        Callable<EmbeddingResponse> guarded =
                CircuitBreaker.decorateCallable(resilience.circuitBreaker(),
                        RateLimiter.decorateCallable(resilience.rateLimiter(),
                                Bulkhead.decorateCallable(resilience.bulkhead(), timeLimited)));

        long start = System.nanoTime();
        String outcome = "success";
        try {
            EmbeddingResponse response = guarded.call();
            logUsage(response);
            return extractVector(response);
        } catch (CallNotPermittedException e) {
            outcome = "circuit_open";
            throw new TriageAgentException("rag-embedding-circuit-open", e);
        } catch (RequestNotPermitted e) {
            outcome = "rate_limited";
            throw new TriageAgentException("rag-embedding-rate-limited", e);
        } catch (BulkheadFullException e) {
            outcome = "bulkhead_full";
            throw new TriageAgentException("rag-embedding-bulkhead-full", e);
        } catch (TimeoutException e) {
            outcome = "timeout";
            throw new TriageAgentException("rag-embedding-timeout", e);
        } catch (Exception e) {
            outcome = "error";
            throw new TriageAgentException("rag-embedding-error", e);
        } finally {
            latencyTimer(outcome).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Always one input string → one result; index 0 is always the answer — not a
     * defensive/arbitrary choice. Verified against Spring AI 2.0.0's Embedding/
     * EmbeddingResponse classes this session.
     */
    private float[] extractVector(EmbeddingResponse response) {
        return response.getResults().get(0).getOutput();
    }

    private Timer latencyTimer(String outcome) {
        return Timer.builder("triage.rag.embedding.latency")
                .tag("provider", provider)
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    /** Token counts only — never the embedded text itself (architecture design: same policy as the chat LLM boundary). */
    private void logUsage(EmbeddingResponse response) {
        if (response.getMetadata() == null) return;
        var usage = response.getMetadata().getUsage();
        if (usage != null) {
            log.debug("RAG embedding usage promptTokens={}", usage.getPromptTokens());
        }
    }
}
