package io.rpe.triage.service;

import io.rpe.triage.agent.TriageAgent;
import io.rpe.triage.agent.TriageAgentException;
import io.rpe.triage.config.TriageProperties;
import io.rpe.triage.domain.PaymentAlert;
import io.rpe.triage.domain.TriagedAlertMessage;
import io.rpe.triage.inbox.TriageInbox;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rpe.triage.config.BoundaryHandler;
import org.springframework.stereotype.Service;

/**
 * Orchestrates the ordering invariant (ai-triage-rules.md §2) — "the one that costs
 * money when wrong":
 *
 * <pre>
 * consume → inbox INSERT ON CONFLICT DO NOTHING (alert_id)
 *         → conflict? → skip, done (NO LLM spend on redelivery)
 *         → LLM agent loop  (any failure ⇒ degraded verdict, never a throw upward)
 *         → publish verdict (confirmed)
 *         → UPDATE triaged_alerts
 * </pre>
 *
 * Failure semantics: publish/mark failures leave the row PENDING_TRIAGE; the sweep
 * re-emits a DEGRADED verdict later. The LLM is never re-called for such rows.
 * This method never throws for LLM-path reasons — alerts are never dropped or
 * delayed indefinitely by LLM unavailability (ADR-15 §3.7).
 */
@Service
public class TriageService {

    private static final Logger log = LoggerFactory.getLogger(TriageService.class);

    private final TriageInbox             inbox;
    private final TriageAgent             agent;
    private final DegradedTriageFallback  fallback;
    private final TriagedVerdictPublisher publisher;
    private final ObjectMapper            objectMapper;
    private final MeterRegistry           meterRegistry;
    private final TriageProperties        props;

    public TriageService(
            TriageInbox inbox,
            TriageAgent agent,
            DegradedTriageFallback fallback,
            TriagedVerdictPublisher publisher,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            TriageProperties props) {
        this.inbox         = inbox;
        this.agent         = agent;
        this.fallback      = fallback;
        this.publisher     = publisher;
        this.objectMapper  = objectMapper;
        this.meterRegistry = meterRegistry;
        this.props         = props;
    }

    @BoundaryHandler("triage-message-boundary: LLM failure ⇒ degraded verdict; publish/mark failure ⇒ leave PENDING_TRIAGE for the sweep; never propagate to the consumer")
    public void process(PaymentAlert alert) {
        // Inbox BEFORE the LLM call — reversing this turns every redelivery into
        // duplicate spend. UUIDv5 alert_id makes the conflict check exact (ADR-13).
        boolean inserted = inbox.tryInsert(alert.alertId(), alert.accountId(), alert.ruleName());
        if (!inserted) {
            meterRegistry.counter("triage.inbox.duplicate").increment();
            log.debug("Duplicate delivery skipped alertId={}", alert.alertId());
            return;
        }

        TriagedAlertMessage verdict;
        try {
            verdict = agent.triage(alert);
        } catch (Exception e) {
            String reason = e instanceof TriageAgentException tae
                    ? tae.reason()
                    : "unexpected:" + e.getClass().getSimpleName();
            // Outcome enum + alert_id only — never prompt/completion bodies (rules §6)
            log.warn("LLM triage degraded alertId={} reason={}", alert.alertId(), reason);
            verdict = fallback.degraded(alert, reason, props.promptVersion());
        }

        try {
            // Publish (confirmed) THEN mark — a crash here leaves PENDING_TRIAGE for
            // the sweep. Mark-then-publish would silently lose the verdict forever.
            publisher.publish(verdict);
            inbox.markTriaged(
                    alert.alertId(),
                    verdict.triageStatus(),
                    verdict.severity().name(),
                    objectMapper.writeValueAsString(verdict),
                    verdict.promptVersion());
            meterRegistry.counter("triage.verdicts", "triage_status", verdict.triageStatus())
                    .increment();
            log.info("Triaged alertId={} status={} severity={} rounds={}",
                    alert.alertId(), verdict.triageStatus(), verdict.severity(), verdict.toolRounds());
        } catch (Exception e) {
            // Row stays PENDING_TRIAGE — sweep emits a degraded verdict later.
            // Offset is still committed: the inbox row prevents duplicate LLM spend
            // on redelivery, and the sweep guarantees a verdict is eventually emitted.
            meterRegistry.counter("triage.verdicts", "triage_status", "PUBLISH_FAILED").increment();
            log.error("Verdict publish/mark failed alertId={} — left PENDING_TRIAGE for sweep",
                    alert.alertId(), e);
        }
    }
}
