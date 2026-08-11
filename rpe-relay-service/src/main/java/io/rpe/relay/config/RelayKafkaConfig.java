package io.rpe.relay.config;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * Kafka configuration for the relay service — a single transactional producer.
 *
 * The relay does NOT consume from Kafka (it reads PENDING rows from the {@code outbox}
 * table); it only produces to {@code payment.alerts}. So there is no consumer factory
 * and no {@code @KafkaListener} here.
 *
 * Extracted verbatim from the core module's {@code KafkaConfig#relayKafkaTemplate}
 * during the ADR-17 §7 Stage 1 split.
 */
@Configuration
public class RelayKafkaConfig {

    /** Transport-security props (SASL_SSL/SCRAM) assembled from env once; folded into the
     *  transactional producer factory. PLAINTEXT (lab default) is a no-op (ADR-20). */
    private final Map<String, Object> securityProps;

    public RelayKafkaConfig(
            @Value("${rpe.kafka.security.protocol:PLAINTEXT}")           String protocol,
            @Value("${rpe.kafka.security.sasl-mechanism:SCRAM-SHA-512}") String mechanism,
            @Value("${KAFKA_SASL_USERNAME:}")       String saslUsername,
            @Value("${KAFKA_SASL_PASSWORD:}")       String saslPassword,
            @Value("${KAFKA_TRUSTSTORE_LOCATION:}") String truststoreLocation,
            @Value("${KAFKA_TRUSTSTORE_PASSWORD:}") String truststorePassword) {
        this.securityProps = KafkaSecurity.transportProps(
                protocol, mechanism, saslUsername, saslPassword,
                truststoreLocation, truststorePassword);
    }

    /**
     * Transactional KafkaTemplate used by {@link io.rpe.relay.AlertPublisher}.
     *
     * Value type is {@code JsonNode}: the relay publishes the outbox payload as an opaque
     * tree — it never re-deserializes to a typed DTO (ADR-11). Payload schema evolution
     * has zero relay impact.
     *
     * transaction-id-prefix is injected from spring.kafka.producer.transaction-id-prefix
     * (set from RELAY_INSTANCE_ID env var — unique per host/pod, ADR-06).
     * Two relay instances sharing a transactional.id → ProducerFencedException loop.
     */
    @Bean
    @Qualifier("relay")
    public KafkaTemplate<String, JsonNode> relayKafkaTemplate(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = kafkaProps.buildProducerProperties();
        props.putAll(securityProps);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        var factory = new DefaultKafkaProducerFactory<String, JsonNode>(props);
        String txPrefix = kafkaProps.getProducer().getTransactionIdPrefix();
        if (txPrefix != null) {
            factory.setTransactionIdPrefix(txPrefix);
        }
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        var template = new KafkaTemplate<>(factory);
        // Distributed tracing (ADR-25): emit a produce span and inject its W3C traceparent into
        // the payment.alerts record headers. AlertPublisher runs each send under the outbox row's
        // restored parent context, so this produce span is a continuous child of the originating
        // event's trace — and alert-service / triage auto-continue from the header downstream.
        template.setObservationEnabled(true);
        return template;
    }
}
