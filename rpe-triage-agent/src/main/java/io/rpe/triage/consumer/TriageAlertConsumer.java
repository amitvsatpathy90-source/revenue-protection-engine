package io.rpe.triage.consumer;

import io.rpe.triage.domain.PaymentAlert;
import io.rpe.triage.service.TriageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code payment.alerts} on the triage module's OWN consumer group with
 * {@code isolation.level=read_committed} (transactional relay upstream).
 *
 * Returns {@code void} — same Spring Kafka constraint as the core module.
 * LLM failures never propagate here: {@code TriageService} converts every LLM-path
 * failure into a degraded verdict internally. The only throws out of this listener
 * are validation failures (→ DLT, not-retryable) and genuine bugs (→ bounded
 * retries → DLT). A poison alert never stalls the triage group.
 */
@Component
public class TriageAlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(TriageAlertConsumer.class);

    private final TriageService triageService;

    public TriageAlertConsumer(TriageService triageService) {
        this.triageService = triageService;
    }

    @KafkaListener(
            id               = "triage-consumer",
            topics           = "payment.alerts",
            containerFactory = "triageListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, PaymentAlert> record, Acknowledgment ack) {
        if (record.value() == null) {
            // Tombstone; deserialization failures are thrown by the container before
            // the listener runs (ErrorHandlingDeserializer header check)
            log.debug("Skipping tombstone record at offset {}", record.offset());
            ack.acknowledge();
            return;
        }

        PaymentAlert alert = record.value();
        if (alert.alertId() == null || alert.accountId() == null || alert.ruleName() == null) {
            // Deterministic → not-retryable → DLT. The inbox PK and the degraded
            // severity map both need these fields; processing without them is undefined.
            throw new TriageValidationException(
                    alert.alertId() != null, alert.accountId() != null, alert.ruleName() != null);
        }

        // alert_id is a non-PII UUID — loggable verbatim (rules §6). account_id is not put in MDC.
        MDC.put("alert_id", alert.alertId().toString());
        try {
            triageService.process(alert);
            ack.acknowledge();
        } finally {
            MDC.remove("alert_id");
        }
    }
}
