package io.rpe.alert;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Alert service integration tests — the re-homed core scenarios 2 & 4 (ADR-17 §7 Stage 2),
 * now exercising the real {@code AlertConsumer} rather than direct SQL.
 *
 *  - Idempotent actioning: a redelivered alert_id → exactly one processed_alerts row
 *    (deterministic UUIDv5 + ON CONFLICT DO NOTHING).
 *  - Poison alert (invalid JSON) → payment.alerts.DLT; the consumer never stalls.
 *
 * No Redis container — the alert service touches only Kafka + Postgres. Flyway (enabled by
 * default here) provisions the processed_alerts table the service owns.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers(disabledWithoutDocker = true)
class AlertServiceIntegrationTest {

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
        // Testcontainers Postgres has no 'alert' schema (no k8s bootstrap). Use public so
        // Flyway can create processed_alerts without the per-service schema setup.
        registry.add("spring.flyway.schemas", () -> "public");
        registry.add("spring.flyway.table", () -> "flyway_schema_history");
    }

    @Autowired DataSource   dataSource;
    @Autowired ObjectMapper objectMapper;

    @Test
    @DisplayName("Redelivered alert_id → exactly one processed_alerts row (ON CONFLICT DO NOTHING)")
    void duplicateAlertIsActionedExactlyOnce() throws Exception {
        UUID   alertId   = UUID.randomUUID();
        String accountId = UUID.randomUUID().toString();
        String alertJson = alertJson(alertId, accountId, "velocity");

        sendRaw("payment.alerts", accountId, alertJson);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countProcessedAlerts(alertId)).isEqualTo(1));

        // Redeliver the SAME alert_id — deterministic id + ON CONFLICT keep it to one row.
        sendRaw("payment.alerts", accountId, alertJson);

        // Barrier, not a fixed sleep: a sentinel alert with a NEW alert_id but the SAME account key
        // rides the same partition and, sent after the redelivery, is actioned strictly after it.
        // When the sentinel's processed_alerts row lands, the redelivery has already had its chance —
        // a broken ON CONFLICT guard would have inserted a duplicate by now. Replaces a fixed 3s
        // sleep a slow runner could pass vacuously (redelivery not yet consumed).
        UUID sentinelId = UUID.randomUUID();
        sendRaw("payment.alerts", accountId, alertJson(sentinelId, accountId, "velocity"));
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countProcessedAlerts(sentinelId)).isEqualTo(1));

        assertThat(countProcessedAlerts(alertId)).isEqualTo(1);
    }

    @Test
    @DisplayName("Poison alert (invalid JSON) → payment.alerts.DLT, consumer not stalled")
    void poisonAlertRoutedToDlt() throws Exception {
        sendRaw("payment.alerts", UUID.randomUUID().toString(), "NOT_VALID_ALERT_JSON");

        List<String> dlt = consume("payment.alerts.DLT", 1, 20);
        assertThat(dlt).hasSizeGreaterThanOrEqualTo(1);

        // Consumer still alive: a valid alert after the poison message is actioned.
        UUID   alertId   = UUID.randomUUID();
        String accountId = UUID.randomUUID().toString();
        sendRaw("payment.alerts", accountId, alertJson(alertId, accountId, "geo"));
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countProcessedAlerts(alertId)).isEqualTo(1));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String alertJson(UUID alertId, String accountId, String ruleName) throws Exception {
        return objectMapper.writeValueAsString(Map.of(
                "alertId", alertId.toString(),
                "eventId", UUID.randomUUID().toString(),
                "accountId", accountId,
                "ruleName", ruleName,
                "reason", ruleName + " threshold breach",
                "producedAt", Instant.now().toString()));
    }

    private void sendRaw(String topic, String key, String value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        }
    }

    private List<String> consume(String topic, int minMessages, int timeoutSeconds) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<String> messages = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);
            while (messages.size() < minMessages && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> messages.add(r.value()));
            }
        }
        return messages;
    }

    private int countProcessedAlerts(UUID alertId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM processed_alerts WHERE alert_id = ?::uuid")) {
            ps.setString(1, alertId.toString());
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }
}
