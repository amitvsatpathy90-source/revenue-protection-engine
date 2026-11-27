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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testcontainers integration tests for the RPE core service (detection + alert-actioning).
 * <p>
 * Post ADR-17 §7 Stage 1, core's pipeline ends at the outbox; outbox → payment.alerts
 * is rpe-relay-service's responsibility. Detection scenarios therefore assert at the outbox boundary.
 * <p>
 * Scenarios:
 * 1. Duplicate event within TTL → Redis dedup gate blocks replay → 1 outbox row
 * 3. Poison payment event → routed to payment.events.DLT
 * 5. 100 events drain fully → sentinel lands in outbox, state persisted, no drops
 * 7. Reprocessed event → same deterministic alert_id → outbox stays idempotent
 * 8. Hot-path in-flight counters converge when quiescent
 * 9. Outbox row carries a well-formed W3C traceparent (ADR-25)
 * (2, 4 moved to rpe-alert-service per ADR-17 Stage 2; 6 moved to rpe-relay-service per Stage 1)
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

    private static final String PAYMENT_EVENTS_TOPIC = "payment.events";
    private static final String PAYMENT_EVENTS_DLT = PAYMENT_EVENTS_TOPIC + ".DLT";
    private static final int DLT_TIMEOUT_SECONDS = 20;

    private static final Logger RP_LOG = LoggerFactory.getLogger("redpanda");

    /**
     * Logs filtered Redpanda ERROR/WARN lines once, on the first failed test.
     */
    @RegisterExtension
    static final TestWatcher REDPANDA_LOGS_ON_FAILURE = new TestWatcher() {
        private final AtomicBoolean dumped = new AtomicBoolean();

        @Override
        public void testFailed(ExtensionContext ctx, Throwable cause) {
            if (!dumped.compareAndSet(false, true)) {
                return;
            }

            List<String> lines = REDPANDA.getLogs().lines()
                    .filter(line -> line.matches("(?i).*\\b(ERROR|WARN|WARNING)\\b.*")
                            || line.toLowerCase(Locale.ROOT).contains("storage space alert"))
                    .toList();

            int from = Math.max(0, lines.size() - 50);
            List<String> tail = lines.subList(from, lines.size());

            RP_LOG.error("Redpanda logs after failure of {}:\n{}",
                    ctx.getDisplayName(), String.join("\n", tail));
        }
    };

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
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16.3"))
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
        // No 'detection' schema in Testcontainers Postgres; use public
        registry.add("spring.flyway.schemas", () -> "public");
        registry.add("spring.flyway.table", () -> "flyway_schema_history");
    }

    @Autowired
    DataSource dataSource;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    MeterRegistry meterRegistry;
    @Autowired
    ReactiveStringRedisTemplate redisTemplate;

    // ── Scenario 1 ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("1. Duplicate event within TTL → dedup gate blocks replay, 1 outbox row")
    void scenario1_deduplicateEventWithinTtl() throws Exception {
        String accountId = UUID.randomUUID().toString();

        sendVelocityWarmUp(accountId);

        // priorCount=2 >= max-events=2 → velocity alert → outbox row
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));

        // Replay: Redis dedup gate (TTL=300s) must block it
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        // Sentinel on same partition is processed after the replay; its row is the barrier
        String sentinelId = UUID.randomUUID().toString();
        sendPaymentEvent(sentinelId, accountId, 50.0, 37.7749, -122.4194, Instant.now());
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(sentinelId)).isEqualTo(1));

        assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1);
    }

    // ── Scenario 3 ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("3. Poison payment event (invalid JSON) → payment.events.DLT, consumer unblocked")
    void scenario3_poisonPaymentEventRoutedToDlt() throws Exception {
        sendRawPaymentEvent(UUID.randomUUID().toString(), "NOT_VALID_JSON");

        List<String> dltMessages = consumeFromDlt();
        assertThat(dltMessages).hasSize(1);

        // Consumer must still process a valid event after the poison message
        String eventId = UUID.randomUUID().toString();
        String accountId = UUID.randomUUID().toString();
        sendPaymentEvent(eventId, accountId, 10.0, 37.0, -122.0, Instant.now());
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(eventWasProcessed(accountId)).isTrue());
    }

    // ── Scenario 5 ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("5. 100 events fully drain: sentinel lands in outbox, state persisted, no drops")
    void scenario5_noErrorsUnderLoad() throws Exception {
        String accountId = UUID.randomUUID().toString();
        double submittedBefore = counter("rpe.hotpath.alerts.submitted");

        for (int i = 0; i < 100; i++) {
            sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0 + (i * 0.5),
                    37.7749, -122.4194, Instant.now().minusSeconds(100L - i));
        }

        // Same key → same partition; sentinel is processed after all 100
        String sentinelId = UUID.randomUUID().toString();
        sendPaymentEvent(sentinelId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(countOutboxByEventId(sentinelId)).isEqualTo(1));

        assertThat(eventWasProcessed(accountId)).isTrue();                // Welford state in Redis
        assertThat(countOutboxByAccount(accountId)).isGreaterThan(0);     // alerts persisted
        assertThat(counter("rpe.hotpath.alerts.submitted")).isGreaterThan(submittedBefore);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(counter("rpe.hotpath.alerts.buffered"))
                        .isEqualTo(counter("rpe.hotpath.alerts.submitted")));
        assertThat(counter("rpe.outbox.dropped")).isZero();               // no backpressure drops
    }

    // ── Scenario 7 ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("7. Reprocessed event → same deterministic alert_id, outbox idempotent (one row)")
    void scenario7_reprocessedEventSameAlertId() throws Exception {
        String accountId = UUID.randomUUID().toString();

        sendVelocityWarmUp(accountId);
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));
        long rowsBefore = countOutboxByAccount(accountId);

        // Same event again: UUIDv5 alert_id + ON CONFLICT DO NOTHING + dedup gate → no new row
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        // Sentinel row proves the reprocess was already consumed
        String sentinelId = UUID.randomUUID().toString();
        sendPaymentEvent(sentinelId, accountId, 50.0, 37.7749, -122.4194, Instant.now());
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(sentinelId)).isEqualTo(1));

        assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1);
        assertThat(countOutboxByAccount(accountId)).isEqualTo(rowsBefore + 1); // sentinel only
    }

    // ── Scenario 8 ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("8. hotpath in-flight counters: submitted increments and converges to buffered (no drops)")
    void scenario8_inFlightCountersConverge() throws Exception {
        double submittedBefore = counter("rpe.hotpath.alerts.submitted");

        String accountId = UUID.randomUUID().toString();
        sendVelocityWarmUp(accountId);
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));

        assertThat(counter("rpe.hotpath.alerts.submitted")).isGreaterThan(submittedBefore);

        // In-flight gap is transient; Awaitility absorbs the async jdbcScheduler hop
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(counter("rpe.hotpath.alerts.buffered"))
                        .isEqualTo(counter("rpe.hotpath.alerts.submitted")));

        assertThat(counter("rpe.outbox.dropped")).isZero();
    }

    // ── Scenario 9 ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("9. Alert's outbox row carries a well-formed W3C traceparent (ADR-25 outbox trace stitch)")
    void scenario9_alertRowCarriesTraceparent() throws Exception {
        String accountId = UUID.randomUUID().toString();

        sendVelocityWarmUp(accountId);
        String triggeringId = UUID.randomUUID().toString();
        sendPaymentEvent(triggeringId, accountId, 50.0, 37.7749, -122.4194, Instant.now());

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(countOutboxByEventId(triggeringId)).isEqualTo(1));

        // Un-instrumented producer → consume span is a fresh sampled root (flag 01)
        assertThat(traceparentForEventId(triggeringId))
                .as("alert outbox row must carry the consume-span traceparent")
                .matches("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Reads a counter's current value; 0.0 if not yet registered.
     */
    private double counter(String name) {
        var c = meterRegistry.find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    /**
     * Sends two non-alerting events so priorCount stays below velocity max-events=2.
     * Same account key → same partition → processed in send order.
     */
    private void sendVelocityWarmUp(String accountId) throws Exception {
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0,
                37.7749, -122.4194, Instant.now().minusSeconds(2));
        sendPaymentEvent(UUID.randomUUID().toString(), accountId, 50.0,
                37.7749, -122.4194, Instant.now().minusSeconds(1));
    }

    /**
     * Serializes a payment event and publishes it to payment.events, keyed by account.
     */
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
        sendRawPaymentEvent(accountId, payload);
    }

    /**
     * Publishes a raw string value to payment.events and blocks until acked.
     */
    private void sendRawPaymentEvent(String key, String value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(PAYMENT_EVENTS_TOPIC, key, value)).get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Reads payment.events.DLT from earliest until the minimum count or timeout.
     */
    private List<String> consumeFromDlt() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<String> messages = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(PAYMENT_EVENTS_DLT));
            long deadline = System.currentTimeMillis() + (DLT_TIMEOUT_SECONDS * 1000L);
            while (messages.isEmpty() && System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> rec : records) {
                    messages.add(rec.value());
                }
            }
        }
        return messages;
    }

    /**
     * Counts outbox alert-intent rows for a source event (detection's output boundary).
     */
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

    /**
     * Returns the W3C traceparent persisted on an alert's outbox row (ADR-25).
     */
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

    /**
     * Counts all outbox rows for an account.
     */
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

    /**
     * True if Welford state (stats:{accountId}) exists in Redis, i.e. the event was processed.
     */
    private boolean eventWasProcessed(String accountId) {
        return Boolean.TRUE.equals(
                redisTemplate.hasKey("stats:" + accountId).block(Duration.ofSeconds(2)));
    }
}
