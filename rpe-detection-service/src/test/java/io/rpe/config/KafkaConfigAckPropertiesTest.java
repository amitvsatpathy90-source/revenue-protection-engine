package io.rpe.config;

import io.rpe.domain.PaymentEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Pins the payment container's offset-commit contract.
 *
 * Acks are issued from per-account lane VTs in COMPLETION order, not offset order (a slow
 * lane — rate-limit park, outbox backpressure — lets a later offset ack first). The Spring
 * Kafka reference is explicit: with {@code AckMode.MANUAL_IMMEDIATE} "the acknowledgments
 * must be acknowledged in order, because Kafka does not maintain state for each record,
 * only a committed offset for each group/partition". Without {@code asyncAcks}, an
 * out-of-order ack advances the committed watermark PAST an in-flight earlier event; a
 * crash then loses that event silently and permanently — no DLT record, no redelivery,
 * no metric. {@code asyncAcks=true} makes the container defer commits until offset gaps
 * fill, which is what restores every "uncommitted offset ⇒ redelivery" safety claim in
 * ADR-02/ADR-22. Pinned like DetectorPrecedenceTest pins detector order: this is part of
 * the correctness contract, not tuning.
 */
class KafkaConfigAckPropertiesTest {

    @Test
    void paymentContainerCommitsWithAsyncAcksGapManagement() {
        KafkaConfig config = new KafkaConfig("PLAINTEXT", "SCRAM-SHA-512", "", "", "", "");
        var consumerFactory = new DefaultKafkaConsumerFactory<String, PaymentEvent>(Map.of());
        ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> factory =
                config.paymentListenerContainerFactory(
                        consumerFactory, mock(DeadLetterPublishingRecoverer.class));

        ContainerProperties containerProps = factory.getContainerProperties();
        assertThat(containerProps.getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        assertThat(containerProps.isAsyncAcks())
                .as("lane VTs ack in completion order; without asyncAcks the committed "
                        + "watermark can pass an in-flight event — crash = silent permanent loss")
                .isTrue();
    }
}
