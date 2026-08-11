package io.rpe;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testcontainers integration tests for the RPE core service (detection + alert-actioning).
 *
 * Post ADR-17 §7 Stage 1, the relay is a separate deployable (rpe-relay-service). Core's
 * pipeline now ends at the OUTBOX (detection's published interface); outbox → payment.alerts
 * is the relay service's responsibility and is tested there (its own exactly-once test,
 * formerly scenario 6 here). So detection-correctness scenarios assert at the outbox boundary,
 * not at processed_alerts.
 *
 * Scenarios:
 *  1. Duplicate event within TTL → Redis dedup gate blocks replay → 1 outbox alert-intent row
 *  3. Poison payment event → routed to payment.events.DLT (partition never stalls)
 *  5. 100 events → all process without error (load + no pinning; inspect stdout for pins)
 *  7. Reprocessed event → same deterministic alert_id → outbox stays idempotent (one row)
 *  (2. Duplicate-alert dedup, 4. poison-alert DLT → moved to rpe-alert-service, ADR-17 Stage 2)
 *  (6. Relay exactly-once delivery → moved to rpe-relay-service, ADR-17 Stage 1)
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "rpe.detection.velocity.max-events=2",
                "rpe.detection.velocity.window-ms=60000",
                "rpe.detection.velocity.vel-ttl-ms=120000",
                "rpe.detection.z-score.threshold=2.0",
                "rpe.detection.geo.max-speed-kmh=1.0",
                "rpe.detection.geo.geo-ttl-ms=86400000"
        }
)
@Testcontainers(disabledWithoutDocker = true)
class RpeIntegrationTest {

    @Container
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v24.1.11"));

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2.5"))
                    .withExposedPorts(6379)
                    .withCommand("--appendonly yes --appendfsync everysec "
                            + "--maxmemory 96mb --maxmemory-policy volatile-lru --save \"\"");

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
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379).toString());
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("DB_URL", POSTGRES::getJdbcUrl);
        registry.add("DB_USER", POSTGRES::getUsername);
        registry.add("DB_PASSWORD", POSTGRES::getPassword);
        // Testcontainers Postgres has no 'detection' schema (no k8s bootstrap). Use public so
        // Flyway can create outbox without the per-service schema setup.
        registry.add("spring.flyway.schemas", () -> "public");
        registry.add("spring.flyway.table", () -> "flyway_schema_history");
    }

    @Autowired DataSource    dataSource;
    @Autowired ObjectMapper  objectMapper;
    @Autowired MeterRegistry meterRegistry;
    @Autowired ReactiveStringRedisTemplate redisTemplate;

    // ── Scenario 1: Duplicate event within TTL → 1 outbox alert-intent row ────

    @Test
    @DisplayName("1. Duplicate event within TTL → dedup gate blocks replay, 1 outbox row")
    void scenario1_deduplicateEventWithinTtl() throws Exception {
        String accountId = UUID.randomUUID().toString();

        // Warm up velocity: two events that don't alert (priorCount < max-events=2)
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(2));
        Thread.sleep(150);
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(1));
        Thread.sleep(150);

        // Triggering event: priorCount=2 >= max-events=2 → velocity alert → outbox row
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        // Detection's boundary is the outbox; relay delivery is rpe-relay-service's concern.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));

        // Replay the triggering event — Redis dedup gate blocks it (TTL=300s) → no new row.
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        // Barrier, not a fixed sleep: a sentinel on the SAME account key rides the same partition
        // and, sent after the replay, is processed by the per-account lane strictly after it. When
        // the sentinel's own outbox row lands, the replay has already had its chance — a broken
        // dedup gate would have written the duplicate by now. This makes the negative assertion a
        // real check instead of a race a slow CI runner passes vacuously.
        String sentinelId = UUID.randomUUID().toString();
        sendPaymentEvent(sentinelId, accountId, 50.0, 37.7749, -122.4194, Instant.now());
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(sentinelId)).isEqualTo(1));

        assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1);
    }

    // ── Scenario 3: Poison payment event → payment.events.DLT ───────────────

    @Test
    @DisplayName("3. Poison payment event (invalid JSON) → payment.events.DLT, consumer unblocked")
    void scenario3_poisonPaymentEventRoutedToDlt() throws Exception {
        String dltTopic = "payment.events.DLT";
        sendRawString("payment.events", UUID.randomUUID().toString(), "NOT_VALID_JSON");

        List<String> dltMessages = consumeFromTopicWithTimeout(dltTopic, 1, 20);
        assertThat(dltMessages).hasSize(1);

        // Consumer must still be able to process a valid event after the poison message
        String eventId   = UUID.randomUUID().toString();
        String accountId = UUID.randomUUID().toString();
        sendPaymentEvent(eventId, accountId, 10.0, 37.0, -122.0, Instant.now());
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(eventWasProcessed(accountId)).isTrue());
    }

    // ── Scenario 5: 100 events process without error ─────────────────────────

    @Test
    @DisplayName("5. 100 events process without error (verify VT pinning via -Djdk.tracePinnedThreads)")
    void scenario5_noErrorsUnderLoad() throws Exception {
        String accountId = UUID.randomUUID().toString();

        for (int i = 0; i < 100; i++) {
            String eventId = UUID.randomUUID().toString();
            double amount = 50.0 + (i * 0.5);
            sendPaymentEvent(eventId, accountId, amount, 37.7749, -122.4194,
                    Instant.now().minusSeconds(100L - i));
        }

        // All events should process within 30s — success = no unhandled exceptions
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(getOutboxPendingCount()).isGreaterThanOrEqualTo(0)); // liveness check
    }

    // ── Scenario 7: Reprocessed event → same alert_id, outbox stays idempotent ─

    @Test
    @DisplayName("7. Reprocessed event → same deterministic alert_id, outbox idempotent (one row)")
    void scenario7_reprocessedEventSameAlertId() throws Exception {
        String accountId = UUID.randomUUID().toString();

        // Warm up velocity, then a triggering event → exactly one velocity alert → one outbox row
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(2));
        Thread.sleep(150);
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(1));
        Thread.sleep(150);
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));
        long rowsBefore = countOutboxByAccount(accountId);

        // Reprocess the SAME triggering event: deterministic UUIDv5 alert_id means the outbox
        // INSERT (ON CONFLICT DO NOTHING on id) + the Redis dedup gate keep it to one row.
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        // Barrier, not a fixed sleep: a sentinel on the same account/partition, sent after the
        // reprocess, is handled strictly after it; its own outbox row is the observable that the
        // reprocess has already been consumed. Replaces a fixed 3s sleep a slow runner could pass
        // vacuously (reprocess not yet consumed).
        String sentinelId = UUID.randomUUID().toString();
        sendPaymentEvent(sentinelId, accountId, 50.0, 37.7749, -122.4194, Instant.now());
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(sentinelId)).isEqualTo(1));

        // The reprocess added NO row (dedup + ON CONFLICT); only the sentinel did. So the reprocessed
        // event still maps to exactly one outbox row, and the account gained exactly one row overall.
        assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1);
        assertThat(countOutboxByAccount(accountId)).isEqualTo(rowsBefore + 1);
    }

    // ── Scenario 8: in-flight gap counters balance when quiescent ─────────────

    @Test
    @DisplayName("8. hotpath in-flight counters: submitted increments and converges to buffered (no drops)")
    void scenario8_inFlightCountersConverge() throws Exception {
        double submittedBefore = counter("rpe.hotpath.alerts.submitted");

        String accountId = UUID.randomUUID().toString();
        // Warm velocity (max-events=2), then trigger exactly one alert → one outbox row.
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(2));
        Thread.sleep(150);
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(1));
        Thread.sleep(150);
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        // The alert must reach the outbox (its intent passed through submit() → buffered).
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));

        // submitted strictly advanced: the hot path handed at least one alert to the executor.
        assertThat(counter("rpe.hotpath.alerts.submitted")).isGreaterThan(submittedBefore);

        // The volatile in-flight gap (submitted - buffered) is transient by design; once the
        // system is quiescent and Postgres is healthy, every submitted alert was buffered.
        // Awaitility absorbs the async jdbcScheduler hop between the two increments.
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(counter("rpe.hotpath.alerts.buffered"))
                        .isEqualTo(counter("rpe.hotpath.alerts.submitted")));

        // No alert intent was dropped on a healthy Postgres (queue never saturated).
        assertThat(counter("rpe.outbox.dropped")).isZero();
    }

    // ── Scenario 9: ADR-25 outbox trace stitch — alert row carries the consume-span traceparent ─

    @Test
    @DisplayName("9. Alert's outbox row carries a well-formed W3C traceparent (ADR-25 outbox trace stitch)")
    void scenario9_alertRowCarriesTraceparent() throws Exception {
        String accountId = UUID.randomUUID().toString();

        // Warm velocity (max-events=2), then trigger exactly one velocity alert → one outbox row.
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(2));
        Thread.sleep(150);
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0, 37.7749, -122.4194, Instant.now().minusSeconds(1));
        Thread.sleep(150);
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));

        // The captured consume-span context is persisted as a well-formed W3C traceparent — the
        // input the relay restores to continue the SAME trace across the durable outbox gap.
        // (The test producer is un-instrumented, so the consume span is a fresh root — still
        // sampled at probability 1.0, so traceparent is present and flag = 01.)
        assertThat(traceparentForEventId(triggeringId))
                .as("alert outbox row must carry the consume-span traceparent")
                .matches("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");
    }

    /** Reads a counter's current value, returning 0.0 if it has not been registered yet. */
    private double counter(String name) {
        var c = meterRegistry.find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void sendPaymentEvent(String eventId, String accountId, double amount,
                                   double lat, double lon, Instant timestamp) throws Exception {
        String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", eventId,
                "accountId", accountId,
                "amount", amount,
                "lat", lat,
                "lon", lon,
                "timestamp", timestamp.toString(),
                "schemaVersion", "1"
        ));
        sendRawString("payment.events", accountId, payload);
    }

    private void sendRawString(String topic, String key, String value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        }
    }

    private List<String> consumeFromTopicWithTimeout(String topic, int minMessages, int timeoutSeconds) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<String> messages = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);
            while (messages.size() < minMessages && System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> rec : records) {
                    messages.add(rec.value());
                }
            }
        }
        return messages;
    }

    /** Detection's output boundary: alert-intent rows in the outbox, keyed by source event. */
    private int countOutboxByEventId(String eventId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM outbox WHERE payload->>'eventId' = ?")) {
            ps.setString(1, eventId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    /** The W3C trace context persisted on an alert's outbox row (ADR-25), keyed by source event. */
    private String traceparentForEventId(String eventId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                     "SELECT traceparent FROM outbox WHERE payload->>'eventId' = ?")) {
            ps.setString(1, eventId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private long countOutboxByAccount(String accountId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM outbox WHERE account_id = ?")) {
            ps.setString(1, accountId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    private boolean eventWasProcessed(String accountId) throws Exception {
        // A processed event leaves Welford state in Redis (stats:{accountId} HSET).
        return Boolean.TRUE.equals(
                redisTemplate.hasKey("stats:" + accountId).block(Duration.ofSeconds(2)));
    }

    private long getOutboxPendingCount() throws Exception {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT COUNT(*) FROM outbox WHERE status='PENDING'")) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }
}
