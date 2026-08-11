package io.rpe.triage;

import io.rpe.triage.agent.EvidenceValidator;
import io.rpe.triage.agent.LlmResilience;
import io.rpe.triage.agent.TriageAgent;
import io.rpe.triage.agent.TriageLlmClient;
import io.rpe.triage.agent.TriageTools;
import io.rpe.triage.config.LlmResilienceConfig;
import io.rpe.triage.config.TriageProperties;
import io.rpe.triage.domain.PaymentAlert;
import io.rpe.triage.domain.TriagedAlertMessage;
import io.rpe.triage.inbox.TriageInbox;
import io.rpe.triage.service.TriagedVerdictPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.Set;

/**
 * Shared fixtures + hand-rolled stubs for the triage test suite.
 *
 * No Mockito: Byte Buddy cannot instrument on this JVM (same constraint the core
 * module hit). All collaborators are real or hand-rolled stubs — which also keeps
 * the §7 promise that "all provider interaction is stubbed in mvn verify".
 */
public final class TriageTestSupport {

    private TriageTestSupport() {}

    /**
     * ObjectMapper matching Spring Boot's autoconfigured bean: JavaTimeModule registered
     * so {@code Instant} fields (PaymentAlert.producedAt, TriagedAlertMessage.triagedAt)
     * serialize. A bare {@code new ObjectMapper()} throws on java.time types — a test
     * artifact, never the production path.
     */
    public static ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /** Minimal valid TriageVerdict JSON for a no-tool-call response. */
    public static final String VALID_VERDICT_JSON =
            "{\"severity\":\"LOW\",\"narrative\":\"No prior anomalies; low urgency.\","
          + "\"evidence\":[],\"confidence\":0.4}";

    // ── Property fixtures ────────────────────────────────────────────────────

    /** Wide windows — CB/RL/bulkhead never trip; for replay/agent-path tests. */
    public static TriageProperties lenientProps() {
        return new TriageProperties(
                new TriageProperties.Llm(
                        5000, 4000, 50f, 50f, 100, 3, 10_000, 5, 100_000, 1, 10),
                new TriageProperties.Sweep(Duration.ofMinutes(5), 50),
                "v1",
                new TriageProperties.Dev(false));
    }

    /** lenientProps with a small sweep batch — forces multiple claim cycles per sweep in tests. */
    public static TriageProperties sweepBatchProps(int batch) {
        var p = lenientProps();
        return new TriageProperties(
                p.llm(), new TriageProperties.Sweep(Duration.ofMinutes(5), batch),
                p.promptVersion(), p.dev());
    }

    /** Tight window (2 calls), 50ms slow threshold — opens the CB fast in tests. */
    public static TriageProperties cbProps() {
        return new TriageProperties(
                new TriageProperties.Llm(
                        5000, 50, 50f, 50f, 2, 1, 10_000, 5, 100_000, 1, 10),
                new TriageProperties.Sweep(Duration.ofMinutes(5), 50),
                "v1",
                new TriageProperties.Dev(false));
    }

    // ── Builders ─────────────────────────────────────────────────────────────

    public static PaymentAlert alert(String ruleName) {
        return alert(ruleName, ruleName + " alert");
    }

    public static PaymentAlert alert(String ruleName, String reason) {
        return new PaymentAlert(
                UUID.randomUUID(), "evt-" + UUID.randomUUID(), "acct-1234",
                ruleName, reason, Instant.now(), "1");
    }

    public static ChatResponse cannedResponse(String text) {
        return new ChatResponse(List.of(new Generation(new AssistantMessage(text))));
    }

    /** Real TriageAgent wired around a stub ChatModel and a real R4j boundary. */
    public static TriageAgent agent(ChatModel model, TriageProperties props,
                                    MeterRegistry reg, ExecutorService vt) {
        LlmResilience res = new LlmResilienceConfig().llmResilience(props, reg);
        TriageLlmClient client = new TriageLlmClient(model, res, vt, reg, "test");
        return new TriageAgent(
                client,
                new EvidenceValidator(reg),
                new TriageTools(null, null, null, objectMapper(), reg),
                objectMapper(),
                props,
                reg);
    }

    // ── Stub ChatModel — the only abstract method is call(Prompt) ─────────────

    public static final class StubChatModel implements ChatModel {
        public final AtomicInteger calls = new AtomicInteger();
        private final String json;
        private final long   sleepMs;
        private final RuntimeException error;

        public StubChatModel(String json) { this(json, 0, null); }

        private StubChatModel(String json, long sleepMs, RuntimeException error) {
            this.json = json; this.sleepMs = sleepMs; this.error = error;
        }

        /** Each call sleeps {@code ms} → exceeds the CB slow-call threshold. */
        public StubChatModel withSleep(long ms) { return new StubChatModel(json, ms, error); }

        public StubChatModel withError(RuntimeException e) { return new StubChatModel(json, sleepMs, e); }

        @Override
        public ChatResponse call(Prompt prompt) {
            calls.incrementAndGet();
            if (sleepMs > 0) {
                try { Thread.sleep(sleepMs); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            if (error != null) throw error;
            return cannedResponse(json);
        }
    }

    // ── In-memory inbox with real ON CONFLICT semantics ───────────────────────

    public static final class InMemoryInbox implements TriageInbox {
        public final Set<UUID> inserted = ConcurrentHashMap.newKeySet();
        public final Map<UUID, String> statusById = new ConcurrentHashMap<>();
        private final List<PendingRow> pending = Collections.synchronizedList(new ArrayList<>());

        @Override
        public boolean tryInsert(UUID alertId, String accountId, String ruleName) {
            boolean added = inserted.add(alertId);
            if (added) {
                statusById.put(alertId, "PENDING_TRIAGE");
                pending.add(new PendingRow(alertId, accountId, ruleName));
            }
            return added;
        }

        @Override
        public void markTriaged(UUID alertId, String triageStatus, String severity,
                                String verdictJson, String promptVersion) {
            // Mirrors the JDBC status guard (WHERE triage_status = 'PENDING_TRIAGE'):
            // a mark that lost the race to a sweep is a no-op, never an overwrite.
            if (statusById.replace(alertId, "PENDING_TRIAGE", triageStatus)) {
                pending.removeIf(p -> p.alertId().equals(alertId));
            }
        }

        /**
         * Mirrors the JDBC claim semantics: the claim is ATOMIC and exclusive
         * (FOR UPDATE SKIP LOCKED analogue) — a concurrent sweep cannot see rows
         * this sweep has claimed; it takes the next unclaimed rows instead.
         */
        @Override
        public int sweepStalePending(Duration staleAfter, int limit, SweepAction action) {
            List<PendingRow> claimed = new ArrayList<>();
            synchronized (pending) {
                var it = pending.iterator();
                while (it.hasNext() && claimed.size() < limit) {
                    claimed.add(it.next());
                    it.remove();
                }
            }
            int swept = 0;
            for (PendingRow row : claimed) {
                Mark mark = action.process(row);
                if (mark == null) {
                    // Claim released un-marked — row stays PENDING_TRIAGE for the next cycle.
                    pending.add(row);
                    continue;
                }
                // Same status guard as the JDBC mark — claimed rows are always
                // PENDING_TRIAGE, so this counts 1 per marked row.
                if (statusById.replace(row.alertId(), "PENDING_TRIAGE", mark.triageStatus())) {
                    swept++;
                }
            }
            return swept;
        }

        /** Test helper: seed a stale PENDING_TRIAGE row directly. */
        public void seedPending(UUID alertId, String accountId, String ruleName) {
            inserted.add(alertId);
            statusById.put(alertId, "PENDING_TRIAGE");
            pending.add(new PendingRow(alertId, accountId, ruleName));
        }
    }

    // ── Capturing publisher ───────────────────────────────────────────────────

    public static final class CapturingPublisher implements TriagedVerdictPublisher {
        public final List<TriagedAlertMessage> published =
                Collections.synchronizedList(new ArrayList<>());
        public volatile boolean failNext = false;

        @Override
        public void publish(TriagedAlertMessage verdict) {
            if (failNext) {
                failNext = false;
                throw new PublishFailedException("induced publish failure", null);
            }
            published.add(verdict);
        }
    }
}
