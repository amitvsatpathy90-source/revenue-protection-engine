package io.rpe.triage.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the admin-client transport-security fold (kafka-security.md §4, ADR-20). Per-service copy
 * of the detection-service test (no shared jar — microservices.md §1.4).
 *
 * The shared {@code triageAdminClient} bean feeds both observability pollers
 * ({@code DltDepthMetrics}, {@code TriageLagMetrics}). Without the securityProps fold it
 * connects credential-less under SASL_SSL: every depth/lag poll fails inside the
 * {@code @BoundaryHandler} pollers and the gauges freeze at their boot values — false-healthy,
 * silently disarming the ADR-23 RpeDlt* alerts. The Testcontainers suites run PLAINTEXT, where
 * the fold is a no-op, so this test is the only CI guard on the SASL path.
 */
class KafkaAdminSecurityPropsTest {

    @Test
    void adminClientConfigCarriesSaslTransportProps() {
        KafkaTriageConfig config = new KafkaTriageConfig(
                "SASL_SSL", "SCRAM-SHA-512", "rpe-triage", "secret", "", "");
        KafkaAdmin kafkaAdmin = new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092"));

        Map<String, Object> props = config.adminProps(kafkaAdmin);

        assertThat(props)
                .as("autoconfigured admin props (bootstrap etc.) must be preserved")
                .containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        assertThat(props)
                .as("the admin client must authenticate exactly like every other factory "
                        + "(ADR-20) — credential-less = every depth/lag gauge frozen")
                .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
                .containsKey(SaslConfigs.SASL_JAAS_CONFIG);
        assertThat(String.valueOf(props.get(SaslConfigs.SASL_JAAS_CONFIG)))
                .contains("username=\"rpe-triage\"");
    }

    @Test
    void plaintextLabDefaultStaysNoOp() {
        KafkaTriageConfig config = new KafkaTriageConfig("PLAINTEXT", "SCRAM-SHA-512", "", "", "", "");
        KafkaAdmin kafkaAdmin = new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));

        Map<String, Object> props = config.adminProps(kafkaAdmin);

        assertThat(props)
                .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
                .doesNotContainKey(SaslConfigs.SASL_JAAS_CONFIG);
    }
}
