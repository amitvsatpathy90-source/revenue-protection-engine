package io.rpe.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.scram.ScramLoginModule;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka client transport-security properties (SASL_SSL + SCRAM) assembled from env ONLY.
 * The password is folded into the JAAS string in memory — never in application.yml, never
 * logged, never surfaced via /actuator/configprops (locked by ADR-19). Per-service copy
 * (no shared rpe-common jar, microservices.md §1.4), symmetric with Pii.
 *
 * <p>Fail-closed: a SASL protocol with missing credentials aborts startup rather than silently
 * connecting unauthenticated (ADR-20 — same discipline as DB_PASSWORD / ADR-19).
 *
 * <p>PLAINTEXT (explicit lab choice) returns only the protocol key — a behavioural no-op versus
 * today's implicit default, so the Testcontainers suites are unaffected.
 */
public final class KafkaSecurity {

    private KafkaSecurity() {}

    public static Map<String, Object> transportProps(
            String protocol, String mechanism,
            String username, String password,
            String truststoreLocation, String truststorePassword) {

        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);

        if (!protocol.startsWith("SASL_")) {
            return props; // PLAINTEXT / SSL-only (mTLS path) — nothing to assemble here
        }
        if (isBlank(username) || isBlank(password)) {
            throw new IllegalStateException(
                "Kafka security.protocol=" + protocol + " requires KAFKA_SASL_USERNAME/PASSWORD "
              + "(fail-closed — refusing to connect without credentials)");
        }
        props.put(SaslConfigs.SASL_MECHANISM, mechanism);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                "%s required username=\"%s\" password=\"%s\";",
                ScramLoginModule.class.getName(), username, password));

        if (!isBlank(truststoreLocation)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        }
        return props;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
