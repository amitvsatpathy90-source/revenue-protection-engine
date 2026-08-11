package io.rpe.triage.service;

import io.rpe.triage.TriageTestSupport;
import io.rpe.triage.agent.TriageAgent;
import io.rpe.triage.domain.PaymentAlert;
import io.rpe.triage.domain.TriagedAlertMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 replay test — the ordering invariant "that costs money when wrong" (rules §2).
 *
 * Redelivery of the same alert_id must NOT trigger a second LLM invocation: the inbox
 * INSERT ON CONFLICT runs before the agent loop, so a duplicate short-circuits. The
 * LLM-call counter on the stub model is the ground truth.
 */
class TriageServiceReplayTest {

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor();
    private final ObjectMapper objectMapper = TriageTestSupport.objectMapper();

    @AfterEach
    void tearDown() { vt.close(); }

    private TriageService service(TriageTestSupport.StubChatModel model,
                                  TriageTestSupport.InMemoryInbox inbox,
                                  TriageTestSupport.CapturingPublisher publisher) {
        TriageAgent agent = TriageTestSupport.agent(
                model, TriageTestSupport.lenientProps(), registry, vt);
        return new TriageService(
                inbox, agent, new DegradedTriageFallback(), publisher,
                objectMapper, registry, TriageTestSupport.lenientProps());
    }

    @Test
    void redeliveryOfSameAlertInvokesLlmExactlyOnce() {
        var model = new TriageTestSupport.StubChatModel(TriageTestSupport.VALID_VERDICT_JSON);
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var service = service(model, inbox, publisher);

        PaymentAlert alert = TriageTestSupport.alert("velocity");

        service.process(alert);   // first delivery
        service.process(alert);   // redelivery — same alert_id
        service.process(alert);   // and again

        // The whole point: exactly one LLM call across all redeliveries.
        assertThat(model.calls.get()).isEqualTo(1);
        // And exactly one verdict published, marked LLM_TRIAGED.
        assertThat(publisher.published).hasSize(1);
        assertThat(publisher.published.get(0).triageStatus())
                .isEqualTo(TriagedAlertMessage.STATUS_LLM_TRIAGED);
        assertThat(inbox.statusById.get(alert.alertId()))
                .isEqualTo(TriagedAlertMessage.STATUS_LLM_TRIAGED);
        // Duplicate deliveries are counted, not silently ignored.
        assertThat(registry.counter("triage.inbox.duplicate").count()).isEqualTo(2.0);
    }

    @Test
    void distinctAlertsEachInvokeTheLlmOnce() {
        var model = new TriageTestSupport.StubChatModel(TriageTestSupport.VALID_VERDICT_JSON);
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        var service = service(model, inbox, publisher);

        service.process(TriageTestSupport.alert("velocity"));
        service.process(TriageTestSupport.alert("geo"));

        assertThat(model.calls.get()).isEqualTo(2);
        assertThat(publisher.published).hasSize(2);
    }

    @Test
    void publishFailureLeavesRowPendingForTheSweep() {
        var model = new TriageTestSupport.StubChatModel(TriageTestSupport.VALID_VERDICT_JSON);
        var inbox = new TriageTestSupport.InMemoryInbox();
        var publisher = new TriageTestSupport.CapturingPublisher();
        publisher.failNext = true;   // induce a publish failure after the LLM call
        var service = service(model, inbox, publisher);

        PaymentAlert alert = TriageTestSupport.alert("zscore");
        service.process(alert);

        // LLM was called, but the verdict never published → row stays PENDING_TRIAGE
        // for the sweep. Never lost, never silently dropped.
        assertThat(model.calls.get()).isEqualTo(1);
        assertThat(publisher.published).isEmpty();
        assertThat(inbox.statusById.get(alert.alertId())).isEqualTo("PENDING_TRIAGE");
        assertThat(registry.counter("triage.verdicts", "triage_status", "PUBLISH_FAILED").count())
                .isEqualTo(1.0);
    }
}
