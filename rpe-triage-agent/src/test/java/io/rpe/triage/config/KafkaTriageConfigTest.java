package io.rpe.triage.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the ADR-18 single-writer invariant for the triage agent's input-side DLT.
 *
 * The triage suite is container-free by design (no Mockito/Byte Buddy, no Testcontainers — see
 * TriageTestSupport), so this asserts the {@code DeadLetterPublishingRecoverer} destination
 * resolver directly instead of standing up a broker. That is sufficient: the resolver is the only
 * thing that decides the DLT topic, and the historical bug was a wrong resolver
 * ({@code record.topic() + ".DLT"} → payment.alerts.DLT, the alert-service's sole-writer topic).
 *
 * Mandated by ai-triage-rules §7 ("assert exactly one record on payment.alerts.triage.DLT; assert
 * zero records on payment.alerts.DLT").
 */
class KafkaTriageConfigTest {

    private static ConsumerRecord<String, Object> alertRecord() {
        return new ConsumerRecord<>("payment.alerts", 0, 0L, "acct-1", new Object());
    }

    @Test
    @DisplayName("terminal failure on payment.alerts routes to payment.alerts.triage.DLT (the owned DLT)")
    void routesToOwnedTriageDlt() {
        TopicPartition dest = KafkaTriageConfig.triageDltDestination(alertRecord(), new RuntimeException("boom"));

        assertThat(dest.topic()).isEqualTo("payment.alerts.triage.DLT");
        assertThat(dest.topic()).isEqualTo(KafkaTriageConfig.TRIAGE_DLT_TOPIC);
        assertThat(dest.partition()).isEqualTo(-1); // -1 = broker picks the DLT partition
    }

    @Test
    @DisplayName("never routes to payment.alerts.DLT — that is rpe-alert-service's sole-writer topic (ADR-18)")
    void neverRoutesToAlertServiceDlt() {
        TopicPartition dest = KafkaTriageConfig.triageDltDestination(alertRecord(), new RuntimeException("boom"));

        assertThat(dest.topic()).isNotEqualTo("payment.alerts.DLT");
    }

    @Test
    @DisplayName("destination is pinned, not suffixed — independent of the source record's topic")
    void destinationIsPinnedNotDerivedFromSourceTopic() {
        // Even if the agent ever consumed a differently-named topic, the DLT owner must not drift.
        var other = new ConsumerRecord<String, Object>("some.other.topic", 2, 99L, "k", new Object());
        TopicPartition dest = KafkaTriageConfig.triageDltDestination(other, new RuntimeException("boom"));

        assertThat(dest.topic()).isEqualTo("payment.alerts.triage.DLT");
    }
}
