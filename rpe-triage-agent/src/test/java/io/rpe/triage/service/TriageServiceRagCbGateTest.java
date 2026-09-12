package io.rpe.triage.service;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.agent.RagEmbeddingClient;
import io.rpe.triage.agent.RagRetrievalClient;
import io.rpe.triage.agent.TriageAgent;
import io.rpe.triage.agent.TriageLlmClient;
import io.rpe.triage.agent.TriageTools;
import io.rpe.triage.agent.EvidenceValidator;
import io.rpe.triage.config.LlmResilienceConfig;
import io.rpe.triage.domain.TriagedAlertMessage;
import io.rpe.triage.rag.TriageVectorRepository.RetrievedChunk;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.prompt.Prompt;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-30 testing bar #5 — chat CB open must short-circuit RAG entirely.
 * No Mockito on this JVM; invocation flags assert zero calls directly,
 * independent of how the verdict path handles a thrown failure.
 */
class TriageServiceRagCbGateTest {

    @Test
    void chatCircuitOpenSkipsRagClientsEntirely() {
        var registry = new SimpleMeterRegistry();
        var props = TriageTestSupport.cbProps();
        ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor();

        // Same slow-call CB-open mechanism as LlmCircuitBreakerTest — real trip, not a stub flag.
        var slowModel = new TriageTestSupport.StubChatModel(
                TriageTestSupport.VALID_VERDICT_JSON).withSleep(150);
        var resilience = new LlmResilienceConfig().llmResilience(props, registry);
        var client = new TriageLlmClient(slowModel, resilience, vt, registry, "test");
        for (int i = 0; i < 2; i++) {
            try {
                client.call(new Prompt("probe"));
            } catch (RuntimeException expected) {
                // Slow-call (150ms) intentionally trips the CB — failure here is the test setup, not a bug.
            }
        }
        assertThat(resilience.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Invocation flags — direct proof of zero calls, not dependent on how process() handles a throw.
        AtomicBoolean embedCalled = new AtomicBoolean(false);
        AtomicBoolean retrieveCalled = new AtomicBoolean(false);

        RagEmbeddingClient embedding = new RagEmbeddingClient(null, null, null, registry, "test") {
            @Override
            public float[] embed(String text) {
                embedCalled.set(true);
                return new float[0];
            }
        };
        RagRetrievalClient retrieval = new RagRetrievalClient(null, null, null, registry) {
            @Override
            public List<RetrievedChunk> retrieve(float[] q, int topK, double threshold) {
                retrieveCalled.set(true);
                return List.of();
            }
        };

        // Real agent wired to the now-open client — verdict path is genuinely degraded, not mocked.
        var agent = new TriageAgent(
                client, new EvidenceValidator(registry),
                new TriageTools(null, null, null, TriageTestSupport.objectMapper(), registry),
                TriageTestSupport.objectMapper(), props, registry);

        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var service = new TriageService(
                inbox, agent, new DegradedTriageFallback(), publisher,
                TriageTestSupport.objectMapper(), registry, props,
                embedding, retrieval, resilience);

        service.process(TriageTestSupport.alert("geo"));

        // Primary assertion: neither RAG client fired, regardless of downstream verdict handling.
        assertThat(embedCalled.get()).isFalse();
        assertThat(retrieveCalled.get()).isFalse();

        assertThat(publisher.published).hasSize(1);
        TriagedAlertMessage verdict = publisher.published.get(0);
        assertThat(verdict.ragContextUsed()).isFalse();

        vt.close();
    }
}