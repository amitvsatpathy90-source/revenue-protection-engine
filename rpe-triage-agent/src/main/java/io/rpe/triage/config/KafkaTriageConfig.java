package io.rpe.triage.config;

import io.rpe.triage.consumer.TriageValidationException;
import io.rpe.triage.domain.PaymentAlert;
import io.rpe.triage.domain.TriagedAlertMessage;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

@Configuration
public class KafkaTriageConfig {

    public static final String CONSUMER_GROUP = "rpe-triage-agent";

    /**
     * Sole-writer DLT for the triage agent's <em>input</em> consumption of {@code payment.alerts}
     * (ADR-18). It is NOT {@code payment.alerts.DLT} — that topic is owned by
     * {@code rpe-alert-service}. Spring Kafka's default {@code <source>.DLT} suffix resolves to the
     * wrong owner; the destination MUST be pinned to this constant. The matching ACL grants
     * {@code rpe-triage} WRITE on this topic only (kafka-security.md), so in a SASL stack a
     * misroute is broker-rejected — but in lab PLAINTEXT it would silently land on the wrong
     * topic, which is exactly what {@link #triageDltDestination} + its unit test guard against.
     */
    public static final String TRIAGE_DLT_TOPIC = "payment.alerts.triage.DLT";

    /**
     * Destination resolver for terminal triage-consumption failures. Fixed to
     * {@link #TRIAGE_DLT_TOPIC} regardless of the source record's topic — pinning, not suffixing,
     * is what keeps the single-writer invariant (ADR-18) correct. {@code -1} lets Kafka pick the
     * DLT partition. Package-visible so {@code KafkaTriageConfigTest} can assert the owner without
     * standing up a broker (the triage suite is container-free by design).
     */
    static TopicPartition triageDltDestination(
            org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record, Exception ex) {
        return new TopicPartition(TRIAGE_DLT_TOPIC, -1);
    }

    /** Transport-security props (SASL_SSL/SCRAM) assembled from env once; folded into the
     *  consumer + triaged producer factories. PLAINTEXT (lab default) is a no-op (ADR-20). */
    private final Map<String, Object> securityProps;

    public KafkaTriageConfig(
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

    /** Shared observability {@link Admin} client (see {@link #triageAdminClient}); bounded close
     *  at {@link #closeAdminClient()}. */
    private Admin adminClient;

    /**
     * Single shared {@link Admin} client for the observability pollers ({@code DltDepthMetrics},
     * {@code TriageLagMetrics}). {@code Admin} is documented thread-safe and meant to be reused
     * across concurrent callers. Moved here from {@code TriageAdminClientConfig} so it is built
     * from Boot's autoconfigured admin properties PLUS the same transport-security props as the
     * factories above — the admin client is a Kafka client like any other (kafka-security.md §4).
     * Without this fold a SASL_SSL stack leaves it credential-less: every depth/lag poll fails,
     * the {@code @BoundaryHandler} pollers absorb the failures, and the {@code rpe.dlt.depth} /
     * lag gauges freeze at their boot values — false-healthy, silently disarming the ADR-23
     * RpeDlt* alerts. Pinned by {@code KafkaAdminSecurityPropsTest}.
     *
     * <p>{@code destroyMethod = ""} suppresses Spring's inferred no-arg {@code Admin.close()}
     * (unbounded wait); the bounded (5s) close lives in {@link #closeAdminClient()} (ADR-22
     * discipline: every close is bounded). Shutdown note: bean destruction is not ordered against
     * the pollers' last {@code @Scheduled} tick, so a refresh can race this close during shutdown.
     * That race is deliberately tolerated rather than phase-laddered (ADR-22): the losing refresh
     * gets a bounded failure that each poller's {@code @BoundaryHandler} logs and absorbs — a
     * metrics-only surface never blocks or throws into the scheduler thread.
     */
    @Bean(destroyMethod = "")
    public Admin triageAdminClient(KafkaAdmin kafkaAdmin) {
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
    public DefaultKafkaConsumerFactory<String, PaymentAlert> triageConsumerFactory(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = kafkaProps.buildConsumerProperties();
        props.putAll(securityProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        // payment.alerts is written by a Kafka-transactional relay — read_committed is
        // mandatory or this consumer reads uncommitted aborted transactions (ADR-15 §3.1)
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentAlert.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "io.rpe.triage.domain");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        // Poll budget pinned against the inline LLM path (arch-audit F-03): worst case per record is
        // 3 attempts x 12s TimeLimiter + 2 x 1s retry backoff = 38s (triage.llm.time-limit-ms /
        // retry-max-attempts / retry-backoff-ms, application.yml). 5 records x 38s = 190s, under the
        // 300s max.poll.interval.ms default made explicit here so a future default change can't
        // silently reopen the gap. Without this bound, a backlog during an LLM brownout can exceed
        // max.poll.interval.ms before the CB's 20-call window trips, causing a rebalance mid-batch.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        var factory = new DefaultKafkaConsumerFactory<String, PaymentAlert>(props);
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentAlert> triageListenerContainerFactory(
            DefaultKafkaConsumerFactory<String, PaymentAlert> triageConsumerFactory,
            KafkaTemplate<String, TriagedAlertMessage> triagedKafkaTemplate,
            KafkaTemplate<String, byte[]> triagedDltBytesTemplate) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentAlert>();
        factory.setConsumerFactory(triageConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Distributed tracing (ADR-25): continue the trace from the payment.alerts traceparent
        // header (relay-injected). The LLM round-trip then appears as a gen_ai span under this
        // consume span (Spring AI observation). Registry resolved from the context automatically.
        factory.getContainerProperties().setObservationEnabled(true);
        // One consumer per payment.alerts partition (3): up to 3 concurrent triages,
        // inside the Bulkhead's 5-call ceiling
        factory.setConcurrency(3);
        var exec = new SimpleAsyncTaskExecutor("triage-consumer-");
        exec.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(exec);

        // Poison alerts must never stall the triage group. Routed to the triage agent's OWN
        // input-side DLT (ADR-18) — NOT payment.alerts.DLT, which rpe-alert-service solely owns.
        // Pinned via triageDltDestination(); see TRIAGE_DLT_TOPIC.
        //
        // Byte-faithful poison records: an ErrorHandlingDeserializer failure on payment.alerts
        // restores the original byte[]; a lone JsonSerializer<TriagedAlertMessage> template would
        // base64-wrap it (Spring Kafka ref). Route byte[] through a ByteArraySerializer template so
        // payment.alerts.triage.DLT stays byte-faithful; typed (validation) failures keep JSON.
        Map<Class<?>, KafkaOperations<?, ?>> dltTemplates = new LinkedHashMap<>();
        dltTemplates.put(byte[].class, triagedDltBytesTemplate);   // deserialization failures → raw bytes
        dltTemplates.put(Object.class, triagedKafkaTemplate);       // validation failures → JSON
        var recoverer = new DeadLetterPublishingRecoverer(dltTemplates,
                KafkaTriageConfig::triageDltDestination);
        var errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                TriageValidationException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Producer for {@code payment.alerts.triaged} (and DLT routing). Plain idempotent
     * producer — NOT transactional: the inbox row + confirmed send give the advisory
     * tier its at-least-once guarantee; exactly-once is reserved for the core relay.
     */
    @Bean
    public KafkaTemplate<String, TriagedAlertMessage> triagedKafkaTemplate(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = kafkaProps.buildProducerProperties();
        props.putAll(securityProps);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        var factory = new DefaultKafkaProducerFactory<String, TriagedAlertMessage>(props);
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        var template = new KafkaTemplate<>(factory);
        // Trace the payment.alerts.triaged produce (and DLT routing) under the triage consume
        // span, injecting traceparent so downstream consumers of triaged output continue (ADR-25).
        template.setObservationEnabled(true);
        return template;
    }

    /**
     * Byte-array template for the DLT recoverer's {@code byte[].class} route (poison
     * {@code payment.alerts} records that failed deserialization). Serialises the original bytes
     * verbatim so {@code payment.alerts.triage.DLT} stays byte-faithful rather than base64-wrapped
     * by the {@code JsonSerializer<TriagedAlertMessage>} template. Idempotent, non-transactional.
     */
    @Bean
    public KafkaTemplate<String, byte[]> triagedDltBytesTemplate(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = kafkaProps.buildProducerProperties();
        props.putAll(securityProps);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        var factory = new DefaultKafkaProducerFactory<String, byte[]>(props);
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        var template = new KafkaTemplate<>(factory);
        template.setObservationEnabled(true);
        return template;
    }
}
