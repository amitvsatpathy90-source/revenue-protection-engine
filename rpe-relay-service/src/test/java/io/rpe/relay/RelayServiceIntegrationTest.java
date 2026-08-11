package io.rpe.relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Relay service exactly-once test — the re-homed core "scenario 6" (ADR-17 §7 Stage 1).
 *
 * Tests the relay's actual contract in isolation, with detection ABSENT: a single PENDING
 * {@code outbox} row → the relay publishes it to {@code payment.alerts} EXACTLY ONCE (Kafka
 * transactional producer) → the row transitions to PUBLISHED. The relay reads the outbox
 * payload as an opaque JsonNode and republishes it byte-identically (ADR-11).
 *
 * No Redis container: the relay touches only Postgres + Kafka. Flyway is enabled here only
 * (it is disabled in the relay's production profile — the relay owns no tables) to provision
 * the outbox contract DDL (V1__outbox_contract.sql) in the ephemeral Postgres.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.flyway.enabled=true",
                "rpe.relay.min-poll-ms=50",
                "rpe.relay.max-poll-ms=1000"
        }
)
@Testcontainers(disabledWithoutDocker = true)
class RelayServiceIntegrationTest {

    @Container
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v24.1.11"));

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.3"))
                    .withDatabaseName("rpe")
                    .withUsername("rpe")
                    .withPassword("rpe_test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", REDPANDA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("DB_URL", POSTGRES::getJdbcUrl);
        registry.add("DB_USER", POSTGRES::getUsername);
        registry.add("DB_PASSWORD", POSTGRES::getPassword);
    }

    @Autowired DataSource   dataSource;
    @Autowired ObjectMapper objectMapper;

    @Test
    @DisplayName("Relay publishes exactly once to payment.alerts for one PENDING outbox row, then marks it PUBLISHED")
    void relayPublishesExactlyOnceForOnePendingOutboxRow() throws Exception {
        UUID   outboxId  = UUID.randomUUID();
        UUID   alertId   = UUID.randomUUID();
        String accountId = UUID.randomUUID().toString();
        String eventId   = UUID.randomUUID().toString();

        String payload = objectMapper.writeValueAsString(java.util.Map.of(
                "alertId", alertId.toString(),
                "eventId", eventId,
                "accountId", accountId,
                "ruleName", "velocity",
                "reason", "velocity prior-count breach",
                "producedAt", Instant.now().toString()));

        // ADR-25: seed the outbox row WITH a known W3C trace context, as detection would. The
        // relay must restore it and the produced payment.alerts record must carry a traceparent
        // header in the SAME trace (a child span — same trace-id, new span-id).
        String parentTraceId = "0af7651916cd43dd8448eb211c80319c";
        String seededTraceparent = "00-" + parentTraceId + "-b7ad6b7169203331-01";

        // Seed a single PENDING outbox row — the relay loop (already running) picks it up
        // via the V4 pg_notify trigger or its next poll.
        seedPendingOutboxRow(outboxId, accountId, payload, seededTraceparent);

        // Consume from the beginning of a fresh topic; only this one row is ever produced.
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (var consumer = freshConsumer()) {
            consumer.subscribe(List.of("payment.alerts"));
            long deadline = System.currentTimeMillis() + 30_000L;
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }

            assertThat(records).hasSize(1);
            JsonNode published = objectMapper.readTree(records.get(0).value());
            assertThat(published.get("alertId").asText()).isEqualTo(alertId.toString());
            assertThat(published.get("ruleName").asText()).isEqualTo("velocity");
            assertThat(published.get("accountId").asText()).isEqualTo(accountId);

            // The trace stitch: the produced record carries a traceparent header whose trace-id
            // equals the seeded parent's — i.e. the relay's produce span continues the SAME trace
            // across the durable-queue gap (its span-id differs; the trace-id does not).
            Header tp = records.get(0).headers().lastHeader("traceparent");
            assertThat(tp).as("relay must inject a traceparent header (ADR-25 outbox stitch)").isNotNull();
            String producedTraceparent = new String(tp.value());
            assertThat(producedTraceparent)
                    .as("produced span must be in the seeded trace")
                    .startsWith("00-" + parentTraceId + "-");

            // Exactly-once: no further messages after a grace window.
            long graceEnd = System.currentTimeMillis() + 3_000L;
            while (System.currentTimeMillis() < graceEnd) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
            assertThat(records).hasSize(1);
        }

        // Row transitioned PENDING → PUBLISHED.
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(outboxStatus(outboxId)).isEqualTo("PUBLISHED"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private KafkaConsumer<String, String> freshConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "relay-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // read_committed: the relay produces transactionally; only committed records count.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(props);
    }

    private void seedPendingOutboxRow(UUID id, String accountId, String payloadJson, String traceparent)
            throws Exception {
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                     "INSERT INTO outbox(id, account_id, payload, status, attempts, traceparent) "
                   + "VALUES (?::uuid, ?, ?::jsonb, 'PENDING', 0, ?)")) {
            ps.setString(1, id.toString());
            ps.setString(2, accountId);
            ps.setString(3, payloadJson);
            ps.setString(4, traceparent);
            ps.executeUpdate();
        }
    }

    private String outboxStatus(UUID id) throws Exception {
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement("SELECT status FROM outbox WHERE id = ?::uuid")) {
            ps.setString(1, id.toString());
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
