package io.rpe.triage.service;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.agent.*;
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

        // Invocation flag — direct proof of zero calls, not dependent on how process() handles a throw.
        AtomicBoolean retrieveCalled = new AtomicBoolean(false);

        RagRetrievalClient retrieval =
                new RagRetrievalClient(null, null, null, registry) {
                    @Override
                    public List<RetrievedChunk> retrieveByRule(String ruleName, int limit) {
                        // Flag proves keyed lookup was invoked
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
                inbox, agent, new DegradedTriageFallback(TriageTestSupport.lenientProps()), publisher,
                TriageTestSupport.objectMapper(), registry, props, retrieval, resilience);

        service.process(TriageTestSupport.alert("geo"));

        // Primary assertion: retrieval client never fired, regardless of downstream verdict handling.
        assertThat(retrieveCalled.get()).isFalse();

        assertThat(publisher.published).hasSize(1);
        TriagedAlertMessage verdict = publisher.published.getFirst();
        assertThat(verdict.ragContextUsed()).isFalse();

        vt.close();
    }
}
