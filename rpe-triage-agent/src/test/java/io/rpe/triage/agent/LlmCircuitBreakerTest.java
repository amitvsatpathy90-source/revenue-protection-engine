package io.rpe.triage.agent;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.config.LlmResilienceConfig;
import io.rpe.triage.config.TriageProperties;
import io.rpe.triage.domain.TriagedAlertMessage;
import io.rpe.triage.service.DegradedTriageFallback;
import io.rpe.triage.service.TriageService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.prompt.Prompt;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * §7 circuit-breaker test (ADR-15 §6.1 — provider degradation as LATENCY, not errors).
 *
 * A slow provider drives the slow-call-rate CB open, after which calls fail fast
 * (no consumer-lag stall) and the service emits DEGRADED_RULE_BASED verdicts. This is
 * the demoable property: "kill the LLM, watch the CB open, watch degraded verdicts
 * still flow."
 */
class LlmCircuitBreakerTest {

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor();
    private final TriageProperties props = TriageTestSupport.cbProps();

    @AfterEach
    void tearDown() { vt.close(); }

    @Test
    void slowProviderOpensBreakerThenCallsFailFast() {
        var slowModel = new TriageTestSupport.StubChatModel(
                TriageTestSupport.VALID_VERDICT_JSON).withSleep(150);   // > 50ms slow threshold
        var resilience = new LlmResilienceConfig().llmResilience(props, registry);
        var client = new TriageLlmClient(slowModel, resilience, vt, registry, "test");

        // Window is 2 calls: two slow calls take the slow-rate to 100% ≥ 50% → OPEN.
        for (int i = 0; i < 2; i++) {
            try { client.call(new Prompt("probe")); } catch (RuntimeException ignored) { }
        }

        assertThat(resilience.circuitBreaker().getState())
                .isEqualTo(CircuitBreaker.State.OPEN);

        // Post-open: must fail fast (circuit-open), NOT wait on the slow provider.
        long startNanos = System.nanoTime();
        assertThatThrownBy(() -> client.call(new Prompt("probe")))
                .isInstanceOf(TriageAgentException.class)
                .extracting(e -> ((TriageAgentException) e).reason())
                .isEqualTo("circuit-open");
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        assertThat(elapsedMs).isLessThan(50);   // no stall — the slow call is 150ms
    }

    @Test
    void openBreakerYieldsDegradedVerdictThroughTheService() {
        var slowModel = new TriageTestSupport.StubChatModel(
                TriageTestSupport.VALID_VERDICT_JSON).withSleep(150);
        var resilience = new LlmResilienceConfig().llmResilience(props, registry);
        var client = new TriageLlmClient(slowModel, resilience, vt, registry, "test");

        // Force the breaker open via the same slow-call mechanism.
        for (int i = 0; i < 2; i++) {
            try { client.call(new Prompt("probe")); } catch (RuntimeException ignored) { }
        }
        assertThat(resilience.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Wire a service whose agent uses the now-open client.
        var agent = new TriageAgent(
                client, new EvidenceValidator(registry),
                new TriageTools(null, null, null, TriageTestSupport.objectMapper(), registry),
                TriageTestSupport.objectMapper(), props, registry);
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var service = new TriageService(
                inbox, agent, new DegradedTriageFallback(), publisher,
                TriageTestSupport.objectMapper(), registry, props);

        service.process(TriageTestSupport.alert("geo"));

        assertThat(publisher.published).hasSize(1);
        TriagedAlertMessage verdict = publisher.published.get(0);
        assertThat(verdict.triageStatus()).isEqualTo(TriagedAlertMessage.STATUS_DEGRADED_RULE_BASED);
        assertThat(verdict.degradedReason()).isEqualTo("circuit-open");
        // geo → HIGH in the static fallback map (verdict still useful with LLM down)
        assertThat(verdict.severity().name()).isEqualTo("HIGH");
    }
}
