package io.rpe.alert.config;

import io.rpe.alert.domain.AlertMessage;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka configuration for the alert service — a {@code payment.alerts} consumer plus a
 * non-transactional DLT producer. Extracted from the core module's {@code KafkaConfig}
 * during the ADR-17 §7 Stage 2 split.
 */
@Configuration
public class AlertKafkaConfig {

    /** Transport-security props (SASL_SSL/SCRAM) assembled from env once; folded into the
     *  consumer + DLT producer factories. PLAINTEXT (lab default) is a no-op (ADR-20). */
    private final Map<String, Object> securityProps;

    public AlertKafkaConfig(
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

    /** Shared observability {@link Admin} client (see {@link #rpeAdminClient}); bounded close at
     *  {@link #closeAdminClient()}. */
    private Admin adminClient;

    /**
     * Shared {@link Admin} client for the observability pollers ({@code DltDepthMetrics}).
     * Per-service copy of the detection-service bean (no shared jar — microservices.md §1.4).
     *
     * <p>Built from Boot's autoconfigured admin properties PLUS the same transport-security props
     * as every factory in this class — the admin client is a Kafka client like any other
     * (kafka-security.md §4). Without this fold a SASL_SSL stack leaves the admin client
     * credential-less: every depth poll fails, the {@code @BoundaryHandler} poller absorbs the
     * failures, and every {@code rpe.dlt.depth} gauge freezes at its boot value 0 —
     * false-healthy, silently disarming the ADR-23 RpeDlt* alerts. Pinned by
     * {@code KafkaAdminSecurityPropsTest}.
     *
     * <p>{@code destroyMethod = ""} suppresses Spring's inferred no-arg {@code Admin.close()}
     * (unbounded wait); the bounded close lives in {@link #closeAdminClient()} (ADR-22). Bean
     * destruction is not ordered against the poller's last {@code @Scheduled} tick — the losing
     * refresh gets a bounded failure the poller absorbs (metrics-only surface; tolerated race).
     */
    @Bean(destroyMethod = "")
    public Admin rpeAdminClient(KafkaAdmin kafkaAdmin) {
        this.adminClient = Admin.create(adminProps(kafkaAdmin));
        return adminClient;
    }

    /** Package-private seam for {@code KafkaAdminSecurityPropsTest}: the exact config map the
     *  admin client is created from (autoconfigured admin props + transport-security fold). */
    Map<String, Object> adminProps(KafkaAdmin kafkaAdmin) {
        Map<String, Object> props = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        props.putAll(securityProps);
        return props;
    }

    @PreDestroy
    void closeAdminClient() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, AlertMessage> alertConsumerFactory(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = kafkaProps.buildConsumerProperties();
        props.putAll(securityProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rpe-alert-consumer");
        // CRITICAL: immutable constraint — required for Kafka transactional producer compatibility.
        // Without read_committed, this consumer reads uncommitted messages from aborted transactions,
        // silently breaking exactly-once delivery semantics (System Invariants).
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // Poll-lifecycle bound (RPE-03, same reasoning as detection/triage). consume() blocks up to
        // ~30s per record under a Postgres brown-out (Hikari 5s + 10s query timeout × FixedBackOff
        // 2 retries), concurrency 1. Left at the Kafka default (500 records / 300s), a brown-out over
        // ~11 records exceeds max.poll.interval.ms → consumer eviction → rebalance storm. Size for
        // REAL margin: 10 × ~30s = ~300s, comfortably under the 600s interval (~2×) — not 20 records,
        // which would sit AT the 600s boundary with no margin and still flap under sustained brown-out.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600_000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, AlertMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "io.rpe.alert.domain");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        var factory = new DefaultKafkaConsumerFactory<String, AlertMessage>(props);
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AlertMessage> alertListenerContainerFactory(
            DefaultKafkaConsumerFactory<String, AlertMessage> alertConsumerFactory,
            DeadLetterPublishingRecoverer dltRecoverer) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, AlertMessage>();
        factory.setConsumerFactory(alertConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Distributed tracing (ADR-25): continue the trace from the payment.alerts traceparent
        // header (relay-injected) so idempotent actioning into processed_alerts joins the same
        // trace. The container resolves the ObservationRegistry from the context automatically.
        factory.getContainerProperties().setObservationEnabled(true);
        var alertExec = new SimpleAsyncTaskExecutor("rpe-alert-consumer-");
        alertExec.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(alertExec);

        var errorHandler = new DefaultErrorHandler(dltRecoverer, new FixedBackOff(1000L, 2L));
        errorHandler.addNotRetryableExceptions(DeserializationException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Routes structurally broken alerts to {@code payment.alerts.DLT} — a poison alert
     * never stalls the consumer. -1 lets Kafka choose the DLT partition.
     *
     * <b>Byte-faithful poison records:</b> {@code ErrorHandlingDeserializer} failures restore the
     * original {@code byte[]} value; a lone {@code JsonSerializer} template would base64-wrap them
     * (Spring Kafka ref). The type-map routes {@code byte[]} through a {@code ByteArraySerializer}
     * template so {@code payment.alerts.DLT} stays byte-faithful for forensics (security.md).
     */
    @Bean
    public DeadLetterPublishingRecoverer dltRecoverer(
            @Qualifier("dlt")      KafkaTemplate<String, Object> dltTemplate,
            @Qualifier("dltBytes") KafkaTemplate<String, byte[]> dltBytesTemplate) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, dltBytesTemplate);   // deserialization failures → raw bytes
        templates.put(Object.class, dltTemplate);         // typed failures → JSON
        return new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
    }

    /**
     * Shared prop-building for the two non-transactional DLT templates below: security props,
     * transactional.id stripped (so send() works without an active transaction), string keys.
     * Only the value serializer differs per template.
     */
    private Map<String, Object> baseDltProducerProps(KafkaProperties kafkaProps) {
        Map<String, Object> props = kafkaProps.buildProducerProperties();
        props.putAll(securityProps);
        props.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    /**
     * Non-transactional KafkaTemplate for DLT publishing. The alert service produces only
     * to the DLT (it is otherwise a pure consumer + JDBC writer); no transactional.id.
     */
    @Bean
    @Qualifier("dlt")
    public KafkaTemplate<String, Object> dltKafkaTemplate(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = baseDltProducerProps(kafkaProps);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        var factory = new DefaultKafkaProducerFactory<String, Object>(props);
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        var template = new KafkaTemplate<>(factory);
        template.setObservationEnabled(true);   // trace DLT publishes too (ADR-25)
        return template;
    }

    /**
     * Byte-array DLT template for {@link #dltRecoverer}'s {@code byte[].class} route — serialises
     * a deserialization failure's original bytes verbatim so the DLT record stays byte-faithful
     * rather than base64-wrapped by JsonSerializer. Non-transactional, same as the JSON template.
     */
    @Bean
    @Qualifier("dltBytes")
    public KafkaTemplate<String, byte[]> dltBytesKafkaTemplate(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = baseDltProducerProps(kafkaProps);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        var factory = new DefaultKafkaProducerFactory<String, byte[]>(props);
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        var template = new KafkaTemplate<>(factory);
        template.setObservationEnabled(true);
        return template;
    }
}
