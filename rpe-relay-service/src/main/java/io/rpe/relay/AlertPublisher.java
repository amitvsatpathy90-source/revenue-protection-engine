package io.rpe.relay;

import io.rpe.observability.TraceContextReader;
import io.rpe.util.Pii;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rpe.relay.config.BoundaryHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.UUID;

/**
 * Publishes an outbox row to {@code payment.alerts} using a Kafka transactional producer.
 *
 * Ordering: Commit Kafka transaction FIRST, then UPDATE outbox status.
 * If relay crashes between Kafka commit and JDBC update: the row remains PENDING and the
 * relay re-sends on restart.
 *
 * <b>Delivery is at-least-once ON THE TOPIC, not exactly-once.</b> Producer idempotency only
 * de-dupes retries WITHIN a producer session (PID+epoch+sequence); a post-restart re-send is a
 * fresh transaction, so a duplicate record DOES land on {@code payment.alerts}. Exactly-once is
 * realized only at the consumer, by idempotent recording keyed on the deterministic {@code alert_id}
 * ({@code processed_alerts ON CONFLICT DO NOTHING}; triage inbox {@code ON CONFLICT}). Every
 * {@code payment.alerts} consumer MUST dedupe — a non-idempotent consumer double-acts (kafka-consumer.md).
 * (The batch-wide {@code conn.commit()} in {@link OutboxRelay} widens this re-send window to the
 * whole batch on a crash; consumer dedup absorbs it either way.)
 *
 * The payload is forwarded as an OPAQUE {@link JsonNode} — never deserialized to a typed
 * DTO (ADR-11). Alert/event schema evolution has zero relay impact by design: a field
 * added by a newer producer flows through this relay byte-identically.
 *
 * Called from the relay loop which runs JDBC on the jdbcScheduler (platform threads).
 * This method is called from that platform-thread context — no reactive wrapping needed.
 */
@Component
@BoundaryHandler("relay-publish-boundary: any failure (Kafka tx abort, SQL error) ⇒ return false so the relay retries the outbox row")
public class AlertPublisher {

    private static final Logger log         = LoggerFactory.getLogger(AlertPublisher.class);
    private static final String ALERT_TOPIC = "payment.alerts";

    private final KafkaTemplate<String, JsonNode> relayKafkaTemplate;
    private final TraceContextReader traceContextReader;

    public AlertPublisher(
            @Qualifier("relay") KafkaTemplate<String, JsonNode> relayKafkaTemplate,
            TraceContextReader traceContextReader) {
        this.relayKafkaTemplate = relayKafkaTemplate;
        this.traceContextReader = traceContextReader;
    }

    /**
     * Publishes the outbox payload to Kafka and marks the row as PUBLISHED.
     *
     * @param conn        open JDBC connection holding the FOR UPDATE SKIP LOCKED row lock
     * @param outboxId     outbox row PK
     * @param accountId    routing key for Kafka (per-account ordering)
     * @param payload      opaque JSONB payload from outbox row (AlertMessage JSON)
     * @param traceparent  W3C trace context persisted by detection (ADR-25); nullable
     * @param tracestate   W3C vendor state persisted by detection (ADR-25); nullable
     * @return {@code true} if successfully published and marked; {@code false} on failure
     */
    public boolean publish(Connection conn, UUID outboxId, String accountId, JsonNode payload,
                           String traceparent, String tracestate) {
        // Restore the originating event's trace context (ADR-25) so the produce span below is a
        // continuous child of detection's consume span — the outbox latency shows up IN the trace.
        // A NULL traceparent (legacy row) runs the publish with no restored parent (fail-open).
        return traceContextReader.continueTrace(traceparent, tracestate, "rpe.outbox.relay",
                () -> doPublish(conn, outboxId, accountId, payload));
    }

    private boolean doPublish(Connection conn, UUID outboxId, String accountId, JsonNode payload) {
        try {
            // Commit Kafka transaction first (exactly-once delivery on payment.alerts). The
            // observation-enabled template injects this produce span's traceparent into the
            // payment.alerts headers, so alert-service and triage auto-continue the trace.
            relayKafkaTemplate.executeInTransaction(ops -> {
                ops.send(ALERT_TOPIC, accountId, payload);
                return null;
            });

            // Kafka tx committed — now mark outbox row as PUBLISHED
            try (var stmt = conn.prepareStatement(
                    "UPDATE outbox SET status='PUBLISHED', attempts=attempts+1 WHERE id=?::uuid")) {
                stmt.setQueryTimeout(10);
                stmt.setString(1, outboxId.toString());
                stmt.executeUpdate();
            }
            return true;

        } catch (Exception e) {
            log.error("Failed to publish alert outbox={} account=...{}",
                    outboxId, Pii.maskAccountId(accountId), e);
            return false;
        }
    }
}
