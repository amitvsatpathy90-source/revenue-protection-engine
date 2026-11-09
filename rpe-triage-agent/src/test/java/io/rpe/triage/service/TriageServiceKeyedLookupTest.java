package io.rpe.triage.service;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.agent.RagRetrievalClient;
import io.rpe.triage.agent.TriageAgentException;
import io.rpe.triage.config.LlmResilienceConfig;
import io.rpe.triage.domain.TriagedAlertMessage;
import io.rpe.triage.rag.TriageVectorRepository.RetrievedChunk;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-30 testing bar #8: keyed lookup, service level, no DB.
 */
class TriageServiceKeyedLookupTest {

    private static final String VELOCITY_DOC = "Velocity detector behavior description.";

    // Records prompt text and call count
    private static final class CapturingChatModel implements ChatModel {
        final List<String> prompts = new ArrayList<>();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public ChatResponse call(Prompt prompt) {
            calls.incrementAndGet();
            prompts.add(prompt.getInstructions().stream()
                    .map(Message::getText)
                    .collect(Collectors.joining("\n")));
            return TriageTestSupport.cannedResponse(TriageTestSupport.VALID_VERDICT_JSON);
        }
    }

    // Records key and limit; returns or throws as configured
    private static final class StubRetrieval extends RagRetrievalClient {
        final AtomicInteger calls = new AtomicInteger();
        volatile String lastRule;
        volatile int lastLimit;
        private final List<RetrievedChunk> result;
        private final RuntimeException failure;

        StubRetrieval(List<RetrievedChunk> result, RuntimeException failure) {
            // Null deps: only the overridden method is called
            super(null, null, null, new SimpleMeterRegistry());
            this.result = result;
            this.failure = failure;
        }

        @Override
        public List<RetrievedChunk> retrieveByRule(String ruleName, int limit) {
            calls.incrementAndGet();
            lastRule = ruleName;
            lastLimit = limit;
            if (failure != null) throw failure;
            // Mimics the exact-key match
            return result.stream().filter(c -> c.ruleName().equals(ruleName)).toList();
        }
    }

    private record Fixture(TriageService service, CapturingChatModel model, StubRetrieval retrieval,
                           TriageTestSupport.CapturingPublisher publisher) {
    }

    private final List<ExecutorService> executors = new ArrayList<>();

    @AfterEach // Fixture executors must not leak across tests
    void closeExecutors() {
        executors.forEach(ExecutorService::close);
    }

    private Fixture fixture(StubRetrieval retrieval) {
        var props = TriageTestSupport.lenientProps();
        var registry = new SimpleMeterRegistry();
        var model = new CapturingChatModel();

        var vt = Executors.newVirtualThreadPerTaskExecutor();
        executors.add(vt);

        var agent = TriageTestSupport.agent(model, props, registry, vt);
        var publisher = new TriageTestSupport.CapturingPublisher();
        // Separate closed chat CB for the RAG gate, as in TriageServiceReplayTest.
        var gate = new LlmResilienceConfig().llmResilience(props, new SimpleMeterRegistry());
        var service = new TriageService(
                new TriageTestSupport.InMemoryInbox(), agent,
                new DegradedTriageFallback(props), publisher,
                TriageTestSupport.objectMapper(), registry, props, retrieval, gate);
        return new Fixture(service, model, retrieval, publisher);
    }

    @Test
    void exactRuleMatchReachesPromptAndMarksContextUsed() {
        var retrieval = new StubRetrieval(
                List.of(new RetrievedChunk("velocity", VELOCITY_DOC, 1.0),
                        new RetrievedChunk("geo", "Geo doc.", 1.0)), null);
        var f = fixture(retrieval);

        f.service().process(TriageTestSupport.alert("velocity"));

        assertThat(retrieval.lastRule).isEqualTo("velocity");
        assertThat(retrieval.lastLimit).isEqualTo(TriageTestSupport.lenientProps().rag().retrieval().topK());
        assertThat(f.model().prompts).hasSize(1);
        assertThat(f.model().prompts.getFirst()).contains("<rag_context>").contains(VELOCITY_DOC);

        // Other rules' chunks must not reach the prompt
        assertThat(f.model().prompts.getFirst()).doesNotContain("Geo doc.");

        assertThat(f.publisher().published).hasSize(1);
        assertThat(f.publisher().published.getFirst().ragContextUsed()).isTrue();
    }

    @Test // No corpus row ⇒ no rag_context block
    void unknownRuleYieldsEmptyContextAndUnaugmentedVerdict() {
        var retrieval = new StubRetrieval(
                List.of(new RetrievedChunk("velocity", VELOCITY_DOC, 1.0)), null);
        var f = fixture(retrieval);

        f.service().process(TriageTestSupport.alert("no_such_rule"));

        assertThat(f.model().prompts).hasSize(1);
        assertThat(f.model().prompts.getFirst()).doesNotContain("<rag_context>");
        var verdict = f.publisher().published.getFirst();
        assertThat(verdict.ragContextUsed()).isFalse();
        assertThat(verdict.triageStatus()).isEqualTo(TriagedAlertMessage.STATUS_LLM_TRIAGED);
    }

    @Test // Retrieval failure degrades RAG only, not the verdict
    void retrievalFailureStillProducesUnaugmentedLlmVerdict() {
        var retrieval = new StubRetrieval(List.of(),
                new TriageAgentException("rag-retrieval-error", new RuntimeException("db down")));
        var f = fixture(retrieval);

        f.service().process(TriageTestSupport.alert("velocity"));

        assertThat(retrieval.calls.get()).isEqualTo(1);
        assertThat(f.model().calls.get()).isEqualTo(1);   // RAG outage never gates chat
        var verdict = f.publisher().published.getFirst();
        assertThat(verdict.ragContextUsed()).isFalse();
        assertThat(verdict.triageStatus()).isEqualTo(TriagedAlertMessage.STATUS_LLM_TRIAGED);
    }
}
