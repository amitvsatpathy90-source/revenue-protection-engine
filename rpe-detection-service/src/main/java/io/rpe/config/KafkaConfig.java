package io.rpe.config;

import io.rpe.consumer.AlertNotDurableException;
import io.rpe.consumer.EventValidationException;
import io.rpe.domain.PaymentEvent;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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

@Configuration
public class KafkaConfig {

    /** Transport-security props (SASL_SSL/SCRAM) assembled from env once and folded into every
     *  client factory below. PLAINTEXT (lab default) is a behavioural no-op (ADR-20). */
    private final Map<String, Object> securityProps;

    public KafkaConfig(
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
     *
     * <p>Built from Boot's autoconfigured admin properties PLUS the same transport-security props
     * as every factory in this class — the admin client is a Kafka client like any other
     * (kafka-security.md §4: "a missed factory connects unauthenticated"). Without this fold a
     * SASL_SSL stack leaves the admin client credential-less: every depth poll fails, the
     * {@code @BoundaryHandler} poller absorbs the failures, and every {@code rpe.dlt.depth} gauge
     * freezes at its boot value 0 — false-healthy, silently disarming the ADR-23 RpeDlt* alerts.
     * Pinned by {@code KafkaAdminSecurityPropsTest}.
     *
     * <p>{@code destroyMethod = ""} suppresses Spring's inferred no-arg {@code Admin.close()}
     * (unbounded wait); the bounded close lives in {@link #closeAdminClient()} (ADR-22: every
     * close is bounded). Bean destruction is not ordered against the poller's last
     * {@code @Scheduled} tick — the losing refresh gets a bounded failure the poller's
     * {@code @BoundaryHandler} logs and absorbs (metrics-only surface; tolerated race).
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
    public DefaultKafkaConsumerFactory<String, PaymentEvent> paymentConsumerFactory(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = kafkaProps.buildConsumerProperties();
        props.putAll(securityProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rpe-payment-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "io.rpe.domain");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        var factory = new DefaultKafkaConsumerFactory<String, PaymentEvent>(props);
        // Hand-built factories do not get Boot's auto-wired metrics listener — without
        // this, consumer lag (kafka.consumer.fetch.manager.records.lag) is invisible.
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> paymentListenerContainerFactory(
            DefaultKafkaConsumerFactory<String, PaymentEvent> paymentConsumerFactory,
            DeadLetterPublishingRecoverer dltRecoverer) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentEvent>();
        factory.setConsumerFactory(paymentConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // MANDATORY pairing with the lane model: acks are issued from per-account lane VTs in
        // COMPLETION order, not offset order (a slow lane — rate-limit park, outbox backpressure,
        // CB buffering — lets a later offset ack first). Spring Kafka requires manual acks "in
        // order, because Kafka does not maintain state for each record, only a committed offset";
        // without asyncAcks an out-of-order ack advances the committed watermark PAST an
        // in-flight earlier event, and a crash then loses that event silently (no DLT, no
        // redelivery). asyncAcks defers commits until offset gaps fill — this is what makes
        // every "uncommitted offset ⇒ redelivery" claim (ADR-02 buffer, ADR-22 forced drain)
        // actually true. Cost: more duplicate deliveries after a failure, absorbed by the gate's
        // step-0 dedup pre-check. Pinned by KafkaConfigAckPropertiesTest.
        factory.getContainerProperties().setAsyncAcks(true);
        // Distributed tracing (ADR-25): start/continue a consume span per record and make it the
        // active context during the listener method. PaymentEventConsumer captures its W3C
        // traceparent on the listener thread and persists it on the outbox row. The container
        // resolves the ObservationRegistry from the application context automatically.
        factory.getContainerProperties().setObservationEnabled(true);
        var paymentExec = new SimpleAsyncTaskExecutor("rpe-payment-consumer-");
        paymentExec.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(paymentExec);

        var errorHandler = new DefaultErrorHandler(dltRecoverer, new FixedBackOff(1000L, 2L));
        // Deterministic failures thrown ON THE LISTENER THREAD — retrying cannot change the outcome;
        // route to DLT immediately. GateContractViolation is deliberately NOT listed: it is thrown
        // only inside the lane task (RedisGate.mapReply → processEvent), which the listener has
        // already returned from, so it can never reach this handler — the lane publishes it to the
        // DLT itself via publishToDltAndAck. Listing it here would be dead config that misleads a
        // reader into tuning DLT routing for gate violations in the wrong place (arch-audit).
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                EventValidationException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    // alertConsumerFactory + alertListenerContainerFactory moved to rpe-alert-service
    // (ADR-17 §7 Stage 2). Core no longer consumes payment.alerts — that, plus the
    // processed_alerts table, is the alert service's bounded context.

    /**
     * Shared DLT recoverer. Used in two distinct paths:
     *   1. By {@code DefaultErrorHandler} for exceptions thrown on the listener thread
     *      (deserialization, validation).
     *   2. Directly by {@code PaymentEventConsumer} for lane-task failures — exceptions
     *      inside a lane task never propagate to the error handler, so the lane publishes
     *      to DLT itself and then acks. A processing failure must never be a silent drop.
     *
     * Default routing: {@code <topic>.DLT}, same partition count assumed; -1 lets Kafka
     * choose the partition (DLT topics have fewer partitions than payment.events).
     *
     * <b>Byte-faithful poison records:</b> when {@code ErrorHandlingDeserializer} fails, the
     * recoverer restores the ORIGINAL {@code byte[]} as the record value. A lone {@code JsonSerializer}
     * template would base64-wrap those bytes (Spring Kafka ref, annotation-error-handling), corrupting
     * the DLT record for forensics. The type-map routes {@code byte[]} through a {@code ByteArraySerializer}
     * template; typed values that failed validation/gate (successfully deserialized) keep the JSON
     * template. See security.md (Deserialization + DLT).
     */
    @Bean
    public DeadLetterPublishingRecoverer dltRecoverer(
            @Qualifier("dlt")      KafkaTemplate<String, Object> dltTemplate,
            @Qualifier("dltBytes") KafkaTemplate<String, byte[]> dltBytesTemplate) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, dltBytesTemplate);   // deserialization failures → raw bytes
        templates.put(Object.class, dltTemplate);         // validation/gate failures → typed JSON
        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(record.topic() + ".DLT", -1));
        // Wait for the send result and throw on failure, so PaymentEventConsumer.publishToDltAndAck
        // acks an offset only after the DLT publish is confirmed — a broker outage, a missing
        // single-writer ACL (ADR-20), or RecordTooLargeException must leave the offset uncommitted
        // (redelivery), never destroy the event silently. This is the terminal path of EVERY
        // lane-side failure, including ADR-26's fired-but-undurable alert.
        //
        // NOTE: DeadLetterPublishingRecoverer already DEFAULTS failIfSendResultIsError to true in
        // spring-kafka 4.1.0 (verified: `iconst_1 putfield` in the ctor), so these two calls are
        // behaviourally redundant TODAY — they are a deliberate PIN, not a bug fix. Spring has
        // flipped this default before (it was false in 2.x/3.x); pinning it in code means a future
        // dependency bump cannot silently reintroduce fire-and-forget. Bounded wait: the lane VT
        // parks in accept() without pinning a carrier, and 5s caps a wedged broker.
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(5));
        // A post-gate failure reaches the DLT with the event's dedup key ALREADY marked (gate
        // step 4 runs before Java sees the result), so a re-drive is dedup-blocked and skips the
        // detectors — the DLT record must carry the detection outcome itself or the alert that
        // was firing is silently lost on re-drive. When the failure is a fired-but-undurable
        // alert (outbox saturated), stamp the outcome headers; PaymentEventConsumer reconstructs
        // the alert intent from them on the re-driven copy (deterministic UUIDv5 — no gate
        // re-run, no state pollution). Asserted by RedriveReconstructionTest.
        recoverer.setHeadersFunction((record, ex) -> {
            Throwable t = ex;
            while (t != null && !(t instanceof AlertNotDurableException)) {
                t = t.getCause();
            }
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            if (t instanceof AlertNotDurableException notDurable) {
                headers.add(AlertNotDurableException.OUTCOME_HEADER,
                        AlertNotDurableException.OUTCOME_ALERT_UNDURABLE.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                headers.add(AlertNotDurableException.RULE_HEADER,
                        notDurable.ruleName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                if (notDurable.reason() != null) {
                    headers.add(AlertNotDurableException.REASON_HEADER,
                            notDurable.reason().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
            }
            return headers;
        });
        return recoverer;
    }

    /**
     * Dedicated non-transactional KafkaTemplate for DLT publishing.
     *
     * Core is no longer a Kafka producer for {@code payment.alerts} — the transactional
     * relay producer moved to {@code rpe-relay-service} (ADR-17 §7 Stage 1). This template
     * exists only for the DeadLetterPublishingRecoverer, which runs in error-handling paths
     * outside any transaction; it explicitly strips transactional.id so send() works without
     * an active transaction.
     */
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

    @Bean
    @Qualifier("dlt")
    public KafkaTemplate<String, Object> dltKafkaTemplate(
            KafkaProperties kafkaProps, MeterRegistry meterRegistry) {
        Map<String, Object> props = baseDltProducerProps(kafkaProps);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        var factory = new DefaultKafkaProducerFactory<String, Object>(props);
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        var template = new KafkaTemplate<>(factory);
        // Trace DLT publishes too (ADR-25) — a poison-routing produce span under the consume span.
        template.setObservationEnabled(true);
        return template;
    }

    /**
     * Byte-array DLT template for {@link #dltRecoverer}'s {@code byte[].class} route. A
     * deserialization failure carries the original raw bytes; this template serialises them
     * verbatim (ByteArraySerializer) so {@code payment.events.DLT} stays byte-faithful rather
     * than base64-wrapped by JsonSerializer. Non-transactional, same as the JSON DLT template.
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
