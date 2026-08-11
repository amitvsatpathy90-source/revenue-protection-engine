package io.rpe.config;

import org.junit.jupiter.api.Test;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runtime proof (not YAML-parse proof) that {@code RPE_KAFKA_SECURITY_SASL_MECHANISM} relaxed-binds
 * to {@code rpe.kafka.security.sasl-mechanism} via Spring's actual {@link SystemEnvironmentPropertySource}
 * — the same class {@code StandardEnvironment} wraps around a real {@code System.getenv()}. Also pins
 * the negative case: the old key name {@code RPE_KAFKA_SASL_MECHANISM} (missing the {@code SECURITY}
 * segment) does NOT bind, which is exactly why it was silently dead in {@code .env}/.env.example and
 * the canonical compose topology — the {@code @Value} default masked the disconnect.
 */
class KafkaSaslMechanismEnvBindingTest {

    @Test
    void correctedEnvVarNameRelaxedBindsToTheKafkaConfigProperty() {
        SystemEnvironmentPropertySource envSource = new SystemEnvironmentPropertySource(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                Map.of(
                        "RPE_KAFKA_SECURITY_PROTOCOL", "SASL_SSL",
                        "RPE_KAFKA_SECURITY_SASL_MECHANISM", "SCRAM-SHA-512"));

        assertThat(envSource.getProperty("rpe.kafka.security.protocol"))
                .as("sibling property — already known to work, control case")
                .isEqualTo("SASL_SSL");
        assertThat(envSource.getProperty("rpe.kafka.security.sasl-mechanism"))
                .as("the property KafkaConfig's @Value actually binds to")
                .isEqualTo("SCRAM-SHA-512");
    }

    @Test
    void originalBrokenEnvVarNameDidNotBind() {
        SystemEnvironmentPropertySource brokenEnvSource = new SystemEnvironmentPropertySource(
                "broken-env", Map.of("RPE_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"));

        assertThat(brokenEnvSource.getProperty("rpe.kafka.security.sasl-mechanism"))
                .as("RPE_KAFKA_SASL_MECHANISM is missing the SECURITY segment relaxed binding "
                        + "requires — this is why the .env key was dead before the rename")
                .isNull();
    }
}
