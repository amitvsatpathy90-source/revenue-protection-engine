package io.rpe.triage.agent;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.config.LlmResilienceConfig;
import io.rpe.triage.config.RagResilienceConfig;
import io.rpe.triage.config.TriageProperties;
import io.rpe.triage.rag.TriageVectorRepository;
import io.rpe.triage.rag.TriageVectorRepository.RetrievedChunk;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

// ADR-30 bars #1/#6/#8: R4j chain on the retrieval boundary, both entry points
class RagRetrievalClientTest {

    // Scripted repo: counts calls, then blocks/sleeps/throws/returns as configured
    private static final class ScriptedRepository implements TriageVectorRepository {
        final AtomicInteger calls = new AtomicInteger();
        volatile long sleepMs;

        final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        final AtomicReference<CountDownLatch> block = new AtomicReference<>();
        final AtomicReference<List<RetrievedChunk>> result = new AtomicReference<>(List.of());

        private List<RetrievedChunk> respond() {
            calls.incrementAndGet();
            try {
                CountDownLatch latch = block.get();
                if (latch != null) latch.await();
                if (sleepMs > 0) Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();   // TimeLimiter cancelled the future
            }
            RuntimeException f = failure.get();
            if (f != null) throw f;
            return result.get();
        }

        @Override
        public List<RetrievedChunk> similaritySearch(float[] q, int topK, double threshold) {
            return respond();
        }

        @Override
        public List<RetrievedChunk> findByRuleName(String ruleName, int limit) {
            return respond();
        }
    }

    private static final class Rig implements AutoCloseable {
        final ScriptedRepository repo = new ScriptedRepository();
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        final ExecutorService jdbc = Executors.newFixedThreadPool(2);
        final RetrievalResilience resilience;
        final RagRetrievalClient client;

        Rig(TriageProperties props) {
            resilience = new RagResilienceConfig().retrievalResilience(props, registry);
            client = new RagRetrievalClient(repo, resilience, jdbc, registry);
        }

        @Override
        public void close() {
            jdbc.shutdownNow();
        }
    }

    // slowCall/slowRate set so only failures, timeouts and bulkhead can trip anything
    private static TriageProperties props(long timeLimitMs, int window, int bulkhead) {
        var base = TriageTestSupport.lenientProps();
        var retrieval = new TriageProperties.Rag.Retrieval(
                timeLimitMs, 10_000, 100f, 50f, window, 1, 60_000, bulkhead, 1, 1, 5, 0.75);
        return new TriageProperties(
                base.llm(), new TriageProperties.Sweep(Duration.ofMinutes(5), 50),
                base.promptVersion(), base.dev(),
                new TriageProperties.Rag(base.rag().embedding(), retrieval));
    }

    private static String reasonOf(Runnable call) {
        try {
            call.run();
        } catch (TriageAgentException e) {
            return e.reason();
        }
        return "no-exception";
    }

    @Test // Non-retryable failure maps to error on both paths
    void repositoryFailureMapsToErrorOnBothEntryPoints() {
        try (var rig = new Rig(props(5000, 20, 5))) {
            rig.repo.failure.set(new IllegalStateException("db down"));
            assertThat(reasonOf(() -> rig.client.retrieveByRule("velocity", 5)))
                    .isEqualTo("rag-retrieval-error");
            assertThat(reasonOf(() -> rig.client.retrieve(new float[]{0f}, 5, 0.75)))
                    .isEqualTo("rag-retrieval-error");
        }
    }

    @Test // Query outlives the 100ms TimeLimiter
    void slowQueryMapsToTimeout() {
        try (var rig = new Rig(props(100, 20, 5))) {
            rig.repo.sleepMs = 1000;
            assertThat(reasonOf(() -> rig.client.retrieveByRule("velocity", 5)))
                    .isEqualTo("rag-retrieval-timeout");
        }
    }

    @Test // Window 2: two failures open CB, third call rejected
    void openCircuitRejectsWithoutTouchingRepository() {
        try (var rig = new Rig(props(5000, 2, 5))) {
            rig.repo.failure.set(new IllegalStateException("db down"));
            reasonOf(() -> rig.client.retrieveByRule("velocity", 5));
            reasonOf(() -> rig.client.retrieveByRule("velocity", 5));
            assertThat(rig.resilience.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);

            assertThat(reasonOf(() -> rig.client.retrieveByRule("velocity", 5)))
                    .isEqualTo("rag-retrieval-circuit-open");
            assertThat(rig.repo.calls.get()).isEqualTo(2);   // third call never reached the repo
        }
    }

    @Test // First caller parks in repo holding the only permit
    void fullBulkheadRejectsSecondCaller() throws InterruptedException {
        try (var rig = new Rig(props(5000, 20, 1))) {
            var latch = new CountDownLatch(1);
            rig.repo.block.set(latch);
            Thread first = Thread.ofVirtual().start(() -> rig.client.retrieveByRule("velocity", 5));
            try {
                long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
                // Wait until the first call holds the permit
                while (rig.resilience.bulkhead().getMetrics().getAvailableConcurrentCalls() > 0
                        && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertThat(reasonOf(() -> rig.client.retrieveByRule("velocity", 5)))
                        .isEqualTo("rag-retrieval-bulkhead-full");
            } finally {
                latch.countDown();
                first.join(2000);
            }
        }
    }

    @Test // Counter moves only on an empty keyed result
    void emptyResultCountsOnlyWhenKeyedLookupFindsNothing() {
        try (var rig = new Rig(props(5000, 20, 5))) {
            rig.client.retrieveByRule("no_such_rule", 5);
            assertThat(rig.registry.counter("triage.rag.retrieval.empty").count()).isEqualTo(1.0);
            rig.repo.result.set(List.of(new RetrievedChunk("velocity", "doc", 1.0)));
            rig.client.retrieveByRule("velocity", 5);
            assertThat(rig.registry.counter("triage.rag.retrieval.empty").count()).isEqualTo(1.0);
        }
    }

    @Test // Covers instance separation only, not Spring bean wiring
    void retrievalCircuitOpenLeavesChatCircuitClosed() {
        var chat = new LlmResilienceConfig().llmResilience(
                TriageTestSupport.lenientProps(), new SimpleMeterRegistry());
        try (var rig = new Rig(props(5000, 2, 5))) {
            rig.repo.failure.set(new IllegalStateException("db down"));
            reasonOf(() -> rig.client.retrieveByRule("velocity", 5));
            reasonOf(() -> rig.client.retrieveByRule("velocity", 5));
            assertThat(rig.resilience.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
            assertThat(chat.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }
    }
}
