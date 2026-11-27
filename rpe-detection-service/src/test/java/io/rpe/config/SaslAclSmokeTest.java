package io.rpe.config;

import io.rpe.observability.DltDepthMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Manual SASL/ACL smoke test against a SASL-enabled Redpanda with authorization on. Not run in CI.
 *
 * <pre>
 *   RPE_SASL_SMOKE=true mvn -f rpe-detection-service/pom.xml -q test -Dtest=SaslAclSmokeTest
 * </pre>
 * <p>
 * Disabled unless {@code RPE_SASL_SMOKE=true}, so the PLAINTEXT Testcontainers suites are unaffected.
 *
 * <ol>
 *   <li>Each DLT owner authenticates via the real {@code KafkaSecurity} and queries its own DLT depth.</li>
 *   <li>A principal without a grant gets {@code TopicAuthorizationException}.</li>
 *   <li>Without the {@code .parked} DESCRIBE grant, {@code DltDepthMetrics} reports a false-healthy 0
 *       while {@code rpe.dlt.depth.poll_failures} rises; the grant fixes it.</li>
 *   <li>Relay transactional init fails without its prefixed txn-id ACL, succeeds once granted (ADR-06/20).</li>
 * </ol>
 * <p>
 * Test order is load-bearing (deny-then-grant), hence {@code @TestMethodOrder}.
 */
@Tag("sasl-smoke")
@EnabledIfEnvironmentVariable(named = "RPE_SASL_SMOKE", matches = "true")
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(OrderAnnotation.class)
class SaslAclSmokeTest {

    private static final String SUPERUSER = "rpe-admin";
    private static final String PW = "smoke-pw";
    private static final String MECHANISM = "SCRAM-SHA-512";
    private static final long TIMEOUT_SEC = 15;

    private static final String EVENTS_DLT = "payment.events.DLT";
    private static final String EVENTS_PARKED = "payment.events.DLT.parked";

    @Container
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v24.1.11"))
                    .enableAuthorization()
                    .enableSasl()
                    .withSuperuser(SUPERUSER);

    /**
     * Shared client for Redpanda Admin HTTP API calls; closed in {@link #closeAll()}.
     */
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static Admin superAdmin;
    private static final List<AutoCloseable> toClose = new ArrayList<>();

    /**
     * Creates SCRAM users, topics, and the ACL matrix (minus grants proven missing in tests 3 and 4).
     */
    @BeforeAll
    static void provision() throws Exception {
        toClose.add(HTTP);

        // withSuperuser() only names the user; it must still be created.
        for (String user : List.of(SUPERUSER, "rpe-detection", "rpe-alert", "rpe-triage", "rpe-relay")) {
            createScramUser(user);
        }

        superAdmin = Admin.create(clientProps(SUPERUSER));
        toClose.add(superAdmin);

        superAdmin.createTopics(List.of(
                new NewTopic("payment.events", 1, (short) 1),
                new NewTopic("payment.alerts", 1, (short) 1),
                new NewTopic(EVENTS_DLT, 1, (short) 1),
                new NewTopic(EVENTS_PARKED, 1, (short) 1),
                new NewTopic("payment.alerts.DLT", 1, (short) 1),
                new NewTopic("payment.alerts.DLT.parked", 1, (short) 1),
                new NewTopic("payment.alerts.triage.DLT", 1, (short) 1),
                new NewTopic("payment.alerts.triage.DLT.parked", 1, (short) 1)
        )).all().get(TIMEOUT_SEC, TimeUnit.SECONDS);

        // provision-acls.sh matrix, minus .parked DESCRIBE (test 3) and relay txn-id ACL (test 4).
        List<AclBinding> matrix = new ArrayList<>(List.of(
                topicAcl("rpe-detection", "payment.events", AclOperation.READ),
                topicAcl("rpe-detection", EVENTS_DLT, AclOperation.WRITE),
                topicAcl("rpe-detection", EVENTS_DLT, AclOperation.DESCRIBE),
                idempotentWriteAcl("rpe-detection"),
                topicAcl("rpe-alert", "payment.alerts", AclOperation.READ),
                topicAcl("rpe-alert", "payment.alerts.DLT", AclOperation.WRITE),
                topicAcl("rpe-alert", "payment.alerts.DLT", AclOperation.DESCRIBE),
                topicAcl("rpe-triage", "payment.alerts", AclOperation.READ),
                topicAcl("rpe-triage", "payment.alerts.triage.DLT", AclOperation.WRITE),
                topicAcl("rpe-triage", "payment.alerts.triage.DLT", AclOperation.DESCRIBE),
                topicAcl("rpe-relay", "payment.alerts", AclOperation.WRITE),
                topicAcl("rpe-relay", "payment.alerts", AclOperation.DESCRIBE),
                idempotentWriteAcl("rpe-relay")
        ));
        superAdmin.createAcls(matrix).all().get(TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    /**
     * Closes the shared HTTP client and admin clients in insertion order.
     */
    @AfterAll
    static void closeAll() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
    }

    // ── 1. transport auth + own-DLT depth query per owner ──────────────────────────────────

    /**
     * Verifies SASL handshake and per-service ACL rows: each owner can read its own DLT depth.
     */
    @Test
    @Order(1)
    void eachDltOwnerCanQueryItsOwnDltDepth() throws Exception {
        Map<String, String> ownerToDlt = Map.of(
                "rpe-detection", EVENTS_DLT,
                "rpe-alert", "payment.alerts.DLT",
                "rpe-triage", "payment.alerts.triage.DLT");

        for (var entry : ownerToDlt.entrySet()) {
            try (Admin owner = Admin.create(clientProps(entry.getKey()))) {
                assertThat(depth(owner, entry.getValue()))
                        .as("%s depth query on %s under SASL", entry.getKey(), entry.getValue())
                        .isZero();
            }
        }
    }

    // ── 2. least privilege actually denies ─────────────────────────────────────────────────

    /**
     * Verifies the broker, not convention, rejects a principal with no grant on the topic.
     */
    @Test
    @Order(2)
    void principalDeniedATopicGetsTopicAuthorizationException() {
        // Detection has no grant of any kind on payment.alerts.
        try (Admin detection = Admin.create(clientProps("rpe-detection"))) {
            assertThatThrownBy(() -> depth(detection, "payment.alerts"))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    // ── 3. the Bundle-1 finding, end-to-end ─────────────────────────────────────────────────

    /**
     * Proves the false-healthy gauge without DESCRIBE on {@code .parked}, and recovery after the grant.
     */
    @Test
    @Order(3)
    void parkedDepthGaugeIsFalseHealthyWithoutDescribeGrantAndSeesAfterIt() throws Exception {
        // Superuser seeds one record (operator-only write in prod).
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(SUPERUSER))) {
            producer.send(new ProducerRecord<>(EVENTS_PARKED, "k", "poison")).get(TIMEOUT_SEC, TimeUnit.SECONDS);
        }

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try (Admin detectionAdmin = Admin.create(clientProps("rpe-detection"))) {
            DltDepthMetrics poller = new DltDepthMetrics(detectionAdmin, registry);

            // No DESCRIBE grant: poll denied, handler absorbs it, gauge stays 0 (false-healthy).
            poller.refresh();
            double blindDepth = registry.get("rpe.dlt.depth").tag("topic", EVENTS_PARKED).gauge().value();
            double failures = registry.get("rpe.dlt.depth.poll_failures").tag("topic", EVENTS_PARKED).counter().count();
            assertThat(blindDepth)
                    .as("false-healthy: gauge frozen at 0 despite 1 parked record")
                    .isZero();
            assertThat(failures)
                    .as("the meta-signal must expose the blind poll")
                    .isGreaterThan(0);

            // Grant DESCRIBE; same poller instance must now see the record.
            superAdmin.createAcls(List.of(topicAcl("rpe-detection", EVENTS_PARKED, AclOperation.DESCRIBE)))
                    .all().get(TIMEOUT_SEC, TimeUnit.SECONDS);
            poller.refresh();
            assertThat(registry.get("rpe.dlt.depth").tag("topic", EVENTS_PARKED).gauge().value())
                    .as("with the DESCRIBE grant the parked record is visible")
                    .isEqualTo(1);
        }
    }

    // ── 4. relay txn-id ACL (ADR-06/20) ─────────────────────────────────────────────────────

    /**
     * Verifies transactional init is denied without the prefixed txn-id ACL and works once granted.
     */
    @Test
    @Order(4)
    void relayTransactionalInitFailsWithoutTxnIdAclThenSucceedsWithIt() throws Exception {
        Map<String, Object> txnProps = producerProps("rpe-relay");
        txnProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "smoke-relay-1");

        try (KafkaProducer<String, String> denied = new KafkaProducer<>(txnProps)) {
            assertThatThrownBy(denied::initTransactions)
                    .isInstanceOf(TransactionalIdAuthorizationException.class);
        }

        superAdmin.createAcls(List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "smoke-relay-", PatternType.PREFIXED),
                        allow("rpe-relay", AclOperation.WRITE)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "smoke-relay-", PatternType.PREFIXED),
                        allow("rpe-relay", AclOperation.DESCRIBE))
        )).all().get(TIMEOUT_SEC, TimeUnit.SECONDS);

        try (KafkaProducer<String, String> granted = new KafkaProducer<>(txnProps)) {
            granted.initTransactions();
            granted.beginTransaction();
            granted.send(new ProducerRecord<>("payment.alerts", "alert-1", "{}"));
            granted.commitTransaction();
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────

    /**
     * Creates a SCRAM user via the Redpanda Admin HTTP API (no auth on the lab admin listener).
     *
     * @param username SCRAM principal name
     */
    private static void createScramUser(String username) throws Exception {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(REDPANDA.getAdminAddress() + "/v1/security/users"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"username\":\"%s\",\"password\":\"%s\",\"algorithm\":\"%s\"}"
                                        .formatted(username, PW, MECHANISM)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as("SCRAM user creation for %s: %s", username, response.body())
                .isEqualTo(200);
    }

    /**
     * Builds client props through the real production transport assembly (ADR-20).
     *
     * @param username SCRAM principal to authenticate as
     */
    private static Map<String, Object> clientProps(String username) {
        Map<String, Object> props = new HashMap<>(
                KafkaSecurity.transportProps("SASL_PLAINTEXT", MECHANISM, username, PW, "", ""));
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 12_000);
        return props;
    }

    /**
     * Extends {@link #clientProps(String)} with String serializers for producers.
     *
     * @param username SCRAM principal to authenticate as
     */
    private static Map<String, Object> producerProps(String username) {
        Map<String, Object> props = clientProps(username);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 12_000);
        return props;
    }

    /**
     * Computes topic depth as sum of (latest - earliest) offsets across partitions.
     *
     * @param admin authenticated admin client
     * @param topic topic to measure
     * @return record count currently retained
     */
    private static long depth(Admin admin, String topic) throws Exception {
        var description = admin.describeTopics(List.of(topic))
                .topicNameValues().get(topic).get(TIMEOUT_SEC, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetSpec> earliestReq = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latestReq = new HashMap<>();
        description.partitions().forEach(p -> {
            TopicPartition tp = new TopicPartition(topic, p.partition());
            earliestReq.put(tp, OffsetSpec.earliest());
            latestReq.put(tp, OffsetSpec.latest());
        });
        var earliest = admin.listOffsets(earliestReq).all().get(TIMEOUT_SEC, TimeUnit.SECONDS);
        var latest = admin.listOffsets(latestReq).all().get(TIMEOUT_SEC, TimeUnit.SECONDS);
        long depth = 0;
        for (TopicPartition tp : earliestReq.keySet()) {
            depth += latest.get(tp).offset() - earliest.get(tp).offset();
        }
        return depth;
    }

    /**
     * Literal-pattern ALLOW ACL on a single topic.
     */
    private static AclBinding topicAcl(String user, String topic, AclOperation op) {
        return new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), allow(user, op));
    }

    /**
     * Cluster-scope IDEMPOTENT_WRITE grant, required by idempotent/transactional producers.
     */
    private static AclBinding idempotentWriteAcl(String user) {
        return new AclBinding(
                new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
                allow(user, AclOperation.IDEMPOTENT_WRITE));
    }

    /**
     * ALLOW entry for {@code User:<user>} from any host.
     */
    private static AccessControlEntry allow(String user, AclOperation op) {
        return new AccessControlEntry("User:" + user, "*", op, AclPermissionType.ALLOW);
    }
}
