package io.rpe.alert.consumer;

import io.rpe.alert.domain.AlertMessage;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Duration;

/**
 * Alert consumer for {@code payment.alerts}.
 *
 * Idempotent actioning via {@code processed_alerts ON CONFLICT DO NOTHING}:
 * the same {@code alert_id} is always safe to receive twice — the second insert is a no-op.
 * {@code alert_id} is a deterministic UUIDv5, so replay produces the same id (ADR-13).
 *
 * {@code isolation.level=read_committed} is set on the consumer factory (immutable constraint):
 * required for Kafka transactional producer compatibility — without it, the consumer reads
 * uncommitted messages from aborted Kafka transactions.
 *
 * {@code produced_at} from alert payload = business ordering (when the transaction occurred).
 * {@code acted_at DEFAULT now()} = operational ordering (when we actioned the alert).
 * These must NOT be conflated.
 *
 * Structurally broken alerts route to {@code payment.alerts.DLT} via ErrorHandlingDeserializer.
 * A poison alert never stalls the consumer.
 */
@Component
public class AlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private final DataSource    dataSource;
    private final Scheduler     jdbcScheduler;
    private final MeterRegistry meterRegistry;

    public AlertConsumer(DataSource dataSource, Scheduler jdbcScheduler, MeterRegistry meterRegistry) {
        this.dataSource    = dataSource;
        this.jdbcScheduler = jdbcScheduler;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Returns {@code void} — immutable constraint. Spring Kafka silently ignores reactive returns.
     *
     * Acks AFTER the insert commits so that a failed insert is retried (exception propagates
     * to DefaultErrorHandler → bounded retries → DLT). Fire-and-forget ack would silently
     * drop alerts whose insert failed (acked offset + no processed_alerts row).
     */
    @KafkaListener(
            id               = "alert-consumer",
            topics           = "payment.alerts",
            containerFactory = "alertListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, AlertMessage> record, Acknowledgment ack) {
        if (record.value() == null) {
            // Deserialization failure handled by ErrorHandlingDeserializer → DLT
            ack.acknowledge();
            return;
        }

        AlertMessage alert = record.value();

        // Block on the VT until the insert commits. VT parks; carrier thread is NOT pinned.
        // Exception (including timeout) propagates to DefaultErrorHandler → retry → DLT.
        Integer inserted = Mono.fromCallable(() -> insertProcessedAlert(alert))
                .subscribeOn(jdbcScheduler)
                .block(Duration.ofSeconds(30));

        // inserted == 0 → ON CONFLICT DO NOTHING absorbed a replayed alert_id (expected
        // under ADR-13 replay semantics; bounded tag set: rule names only)
        meterRegistry.counter("rpe.alerts.actioned",
                        "rule_type", alert.ruleName() == null ? "unknown" : alert.ruleName(),
                        "outcome", inserted != null && inserted > 0 ? "actioned" : "duplicate")
                .increment();
        log.debug("Actioned alert alertId={} rule={}", alert.alertId(), alert.ruleName());
        ack.acknowledge();
    }

    private int insertProcessedAlert(AlertMessage alert) throws Exception {
        String sql =
                "INSERT INTO processed_alerts(alert_id, account_id, rule_name, produced_at) "
              + "VALUES (?::uuid, ?, ?, ?) ON CONFLICT DO NOTHING";

        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(10);
            ps.setString(1, alert.alertId().toString());
            ps.setString(2, alert.accountId());
            ps.setString(3, alert.ruleName());
            ps.setTimestamp(4, Timestamp.from(alert.producedAt()));
            return ps.executeUpdate();
        }
    }
}
