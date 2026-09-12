package io.rpe.triage.agent;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.rpe.triage.config.BoundaryHandler;
import io.rpe.triage.rag.TriageVectorRepository;
import io.rpe.triage.rag.TriageVectorRepository.RetrievedChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Guarded pgvector retrieval (ADR-29/ADR-30). Independent R4j instance set, no
 * RateLimiter (see {@link RetrievalResilience}). Decorator order: query() →
 * Retry → TimeLimiter → Bulkhead → CircuitBreaker.
 */
@Component
public class RagRetrievalClient {

    private static final Logger log = LoggerFactory.getLogger(RagRetrievalClient.class);

    private final TriageVectorRepository repository;
    private final RetrievalResilience    resilience;
    private final ExecutorService        jdbcExecutor;   // pgvector query is JDBC — dedicated pool, no VT exemption
    private final MeterRegistry          meterRegistry;

    public RagRetrievalClient(
            TriageVectorRepository repository,
            RetrievalResilience resilience,
            @Qualifier("jdbcExecutor") ExecutorService jdbcExecutor,
            MeterRegistry meterRegistry) {
        this.repository    = repository;
        this.resilience    = resilience;
        this.jdbcExecutor  = jdbcExecutor;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Same failure taxonomy as {@link RagEmbeddingClient#embed} minus rate-limit
     * outcomes (no RateLimiter here) — callers route any failure to "skip RAG,
     * proceed unaugmented," identically to an embedding failure.
     *
     * @return retrieved chunks, possibly empty — that's a normal outcome, not a failure
     */
    @BoundaryHandler("r4j-decorator-boundary: all terminal failures map to TriageAgentException so callers can skip RAG and proceed unaugmented")
    public List<RetrievedChunk> retrieve(float[] queryEmbedding, int topK, double similarityThreshold) {
        Supplier<List<RetrievedChunk>> retried = Retry.decorateSupplier(
                resilience.retry(),
                () -> repository.similaritySearch(queryEmbedding, topK, similarityThreshold));

        Callable<List<RetrievedChunk>> timeLimited = TimeLimiter.decorateFutureSupplier(
                resilience.timeLimiter(),
                () -> jdbcExecutor.submit(retried::get));

        Callable<List<RetrievedChunk>> guarded =
                CircuitBreaker.decorateCallable(resilience.circuitBreaker(),
                        Bulkhead.decorateCallable(resilience.bulkhead(), timeLimited));

        long start = System.nanoTime();
        String outcome = "success";
        try {
            return guarded.call();
        } catch (CallNotPermittedException e) {
            outcome = "circuit_open";
            throw new TriageAgentException("rag-retrieval-circuit-open", e);
        } catch (BulkheadFullException e) {
            outcome = "bulkhead_full";
            throw new TriageAgentException("rag-retrieval-bulkhead-full", e);
        } catch (TimeoutException e) {
            outcome = "timeout";
            throw new TriageAgentException("rag-retrieval-timeout", e);
        } catch (Exception e) {
            outcome = "error";
            throw new TriageAgentException("rag-retrieval-error", e);
        } finally {
            latencyTimer(outcome).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Timer latencyTimer(String outcome) {
        // No "provider" tag, unlike embedding/llm timers — this isn't a third-party call.
        return Timer.builder("triage.rag.retrieval.latency")
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }
}
