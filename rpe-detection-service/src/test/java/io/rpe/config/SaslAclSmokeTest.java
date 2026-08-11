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
 * The SASL/ACL smoke run (kafka-security.md test bar) — MANUAL, not CI:
 *
 * <pre>
 *   RPE_SASL_SMOKE=true mvn -f rpe-detection-service/pom.xml -q test -Dtest=SaslAclSmokeTest
 * </pre>
 *
 * Without {@code RPE_SASL_SMOKE=true} the class is disabled entirely, so the ordinary
 * Testcontainers suites keep running PLAINTEXT unchanged. Everything CI cannot see — the SASL
 * handshake, broker-side ACL enforcement, and the admin-client transport fold — is exercised
 * here against a SASL-enabled Redpanda with authorization on:
 *
 * <ol>
 *   <li>Each DLT owner's principal, authenticated via the REAL {@code KafkaSecurity} helper,
 *       performs a successful depth query on its own DLT (the per-service ACL matrix rows).</li>
 *   <li>A principal denied a topic gets {@code TopicAuthorizationException}.</li>
 *   <li>The Bundle-1 finding end-to-end: WITHOUT the {@code .parked} DESCRIBE grant, the real
 *       {@code DltDepthMetrics} silently reports 0 for a NON-EMPTY parked topic (false-healthy)
 *       while {@code rpe.dlt.depth.poll_failures} rises; adding the grant makes the gauge see
 *       the record. This is the only executable proof that the frozen-gauge failure mode and
 *       its fix behave as documented.</li>
 *   <li>The relay's transactional init fails without its prefixed txn-id ACL
 *       ({@code TransactionalIdAuthorizationException}) and succeeds once granted (ADR-06/20).</li>
 * </ol>
 *
 * Test order is load-bearing (deny-then-grant narratives) — hence {@code @TestMethodOrder}.
 * SCRAM users are created through Redpanda's Admin HTTP API; ACLs through the Kafka Admin API
 * under a superuser — the same wire operations {@code provision-acls.sh} issues via rpk.
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

    private static Admin superAdmin;
    private static final List<AutoCloseable> toClose = new ArrayList<>();

    @BeforeAll
    static void provision() throws Exception {
        // Users first (Redpanda Admin HTTP API — no auth required on the lab admin listener),
        // superuser included: withSuperuser() only names it in redpanda.superusers.
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

        // The provision-acls.sh matrix — DELIBERATELY MINUS the .parked DESCRIBE grants
        // (added mid-test 3 to prove the blind-gauge finding) and MINUS the relay txn-id ACL
        // (added mid-test 4 to prove the init failure).
        List<AclBinding> matrix = new ArrayList<>(List.of(
                topicAcl("rpe-detection", "payment.events", AclOperation.READ),
                topicAcl("rpe-detection", EVENTS_DLT, AclOperation.WRITE),
                topicAcl("rpe-detection", EVENTS_DLT, AclOperation.DESCRIBE),
                clusterAcl("rpe-detection", AclOperation.IDEMPOTENT_WRITE),
                topicAcl("rpe-alert", "payment.alerts", AclOperation.READ),
                topicAcl("rpe-alert", "payment.alerts.DLT", AclOperation.WRITE),
                topicAcl("rpe-alert", "payment.alerts.DLT", AclOperation.DESCRIBE),
                topicAcl("rpe-triage", "payment.alerts", AclOperation.READ),
                topicAcl("rpe-triage", "payment.alerts.triage.DLT", AclOperation.WRITE),
                topicAcl("rpe-triage", "payment.alerts.triage.DLT", AclOperation.DESCRIBE),
                topicAcl("rpe-relay", "payment.alerts", AclOperation.WRITE),
                topicAcl("rpe-relay", "payment.alerts", AclOperation.DESCRIBE),
                clusterAcl("rpe-relay", AclOperation.IDEMPOTENT_WRITE)
        ));
        superAdmin.createAcls(matrix).all().get(TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    @AfterAll
    static void closeAll() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
    }

    // ── 1. transport auth + own-DLT depth query per owner ──────────────────────────────────

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

    @Test
    @Order(2)
    void principalDeniedATopicGetsTopicAuthorizationException() {
        // Detection holds no grant of any kind on payment.alerts (relay writes it; alert/triage
        // read it). The broker, not convention, must reject.
        try (Admin detection = Admin.create(clientProps("rpe-detection"))) {
            assertThatThrownBy(() -> depth(detection, "payment.alerts"))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    // ── 3. the Bundle-1 finding, end-to-end ─────────────────────────────────────────────────

    @Test
    @Order(3)
    void parkedDepthGaugeIsFalseHealthyWithoutDescribeGrantAndSeesAfterIt() throws Exception {
        // A real record sits on the parked topic (superuser writes it — operator-only in prod).
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(SUPERUSER))) {
            producer.send(new ProducerRecord<>(EVENTS_PARKED, "k", "poison")).get(TIMEOUT_SEC, TimeUnit.SECONDS);
        }

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try (Admin detectionAdmin = Admin.create(clientProps("rpe-detection"))) {
            DltDepthMetrics poller = new DltDepthMetrics(detectionAdmin, registry);

            // WITHOUT the .parked DESCRIBE grant: the poll is authorization-denied, the
            // @BoundaryHandler absorbs it, and the gauge reads 0 for a NON-EMPTY topic.
            // Only the poll_failures counter betrays that the 0 is a lie.
            poller.refresh();
            double blindDepth = registry.get("rpe.dlt.depth").tag("topic", EVENTS_PARKED).gauge().value();
            double failures = registry.get("rpe.dlt.depth.poll_failures").tag("topic", EVENTS_PARKED).counter().count();
            assertThat(blindDepth)
                    .as("false-healthy: gauge frozen at 0 despite 1 parked record")
                    .isZero();
            assertThat(failures)
                    .as("the meta-signal must expose the blind poll")
                    .isGreaterThan(0);

            // Grant DESCRIBE on .parked (the Bundle-1 provision-acls.sh addition) — the same
            // poller instance must now see the record.
            superAdmin.createAcls(List.of(topicAcl("rpe-detection", EVENTS_PARKED, AclOperation.DESCRIBE)))
                    .all().get(TIMEOUT_SEC, TimeUnit.SECONDS);
            poller.refresh();
            assertThat(registry.get("rpe.dlt.depth").tag("topic", EVENTS_PARKED).gauge().value())
                    .as("with the DESCRIBE grant the parked record is visible")
                    .isEqualTo(1);
        }
    }

    // ── 4. relay txn-id ACL (ADR-06/20) ─────────────────────────────────────────────────────

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

    private static void createScramUser(String username) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
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

    /** The REAL production transport assembly — this is the fold under test (kafka-security.md §4). */
    private static Map<String, Object> clientProps(String username) {
        Map<String, Object> props = new HashMap<>(
                KafkaSecurity.transportProps("SASL_PLAINTEXT", MECHANISM, username, PW, "", ""));
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 12_000);
        return props;
    }

    private static Map<String, Object> producerProps(String username) {
        Map<String, Object> props = clientProps(username);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 12_000);
        return props;
    }

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

    private static AclBinding topicAcl(String user, String topic, AclOperation op) {
        return new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), allow(user, op));
    }

    private static AclBinding clusterAcl(String user, AclOperation op) {
        return new AclBinding(
                new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL), allow(user, op));
    }

    private static AccessControlEntry allow(String user, AclOperation op) {
        return new AccessControlEntry("User:" + user, "*", op, AclPermissionType.ALLOW);
    }
}
