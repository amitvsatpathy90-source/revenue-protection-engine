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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Guarded pgvector retrieval (ADR-29/ADR-30).
 * <p>
 * Uses an independent Resilience4j instance set without a RateLimiter (see
 * {@link RetrievalResilience}).
 * <p>
 * Decorator order: {@code query() -> Retry -> TimeLimiter -> Bulkhead -> CircuitBreaker}.
 * <p>
 * Two entry points, {@code retrieveByRule} (v1) and {@code retrieve} (v2), share
 * {@code guarded()}.
 */
@Component
public class RagRetrievalClient {

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
     * Performs vector similarity retrieval using the supplied query embedding.
     * Terminal retrieval exceptions are surfaced as TriageAgentException so
     * callers can skip RAG and proceed without augmentation.
     *
     * @return retrieved chunks; an empty list is a valid no-match outcome
     */
    @SuppressWarnings("UnusedReturnValue") // Dormant: v2 similarity path, no production caller yet
    public List<RetrievedChunk> retrieve(float[] queryEmbedding, int topK, double similarityThreshold) {
        return guarded(() -> repository.similaritySearch(queryEmbedding, topK, similarityThreshold));
    }

    /**
     * v1 path (ADR-30 D3): exact rule_name lookup, no embedding call.
     * Same R4j chain and failure mapping as {@link #retrieve}.
     */
    public List<RetrievedChunk> retrieveByRule(String ruleName, int limit) {
        List<RetrievedChunk> chunks = guarded(() -> repository.findByRuleName(ruleName, limit));
        // Empty = no corpus row; chat-CB skips never reach here
        if (chunks.isEmpty()) {
            // Untagged: rule names are message-sourced, a tag is unbounded
            meterRegistry.counter("triage.rag.retrieval.empty").increment();
        }
        return chunks;
    }

    /**
     * Shared R4j chain; maps every terminal failure to TriageAgentException.
     */
    @BoundaryHandler("r4j-decorator-boundary: all terminal failures map to TriageAgentException so callers can skip RAG and proceed unaugmented")
    private List<RetrievedChunk> guarded(Supplier<List<RetrievedChunk>> query) {
        Supplier<List<RetrievedChunk>> retried = Retry.decorateSupplier(resilience.retry(), query);

        Callable<List<RetrievedChunk>> timeLimited = TimeLimiter.decorateFutureSupplier(
                resilience.timeLimiter(),
                () -> jdbcExecutor.submit(retried::get));

        // CB outermost: open circuit rejects before the bulkhead
        Callable<List<RetrievedChunk>> decorated =
                CircuitBreaker.decorateCallable(resilience.circuitBreaker(),
                        Bulkhead.decorateCallable(resilience.bulkhead(), timeLimited));

        long start = System.nanoTime();
        String outcome = "success";
        try {
            return decorated.call();
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
