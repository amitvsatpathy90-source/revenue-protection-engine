package io.rpe.alert.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * ADR-17 §7 Stage 4 — per-consumer schema contract test for {@code payment.alerts}
 * (consumed by rpe-alert-service).
 *
 * The alert schema is intentionally duplicated across services — detection's {@code AlertMessage},
 * triage's {@code PaymentAlert}, and this local {@link AlertMessage} — and kept compatible by
 * schema, not shared code (microservices.md §2). {@code @JsonIgnoreProperties(ignoreUnknown = true)}
 * is what lets the relay (producer) add fields to the alert envelope without breaking this consumer.
 *
 * Uses the Jackson 2 {@link ObjectMapper} (same flavour as Spring Kafka's {@code JsonDeserializer});
 * {@code findAndRegisterModules()} pulls in jsr310 for {@code Instant}.
 */
class AlertMessageContractTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void fullPayloadBindsAllKnownFields() throws Exception {
        UUID alertId = UUID.randomUUID();
        String json = """
            {"alertId":"%s","eventId":"evt-1","accountId":"acct-9",
             "ruleName":"velocity","reason":"velocity threshold breach",
             "producedAt":"2026-06-18T00:00:00Z"}""".formatted(alertId);

        AlertMessage alert = mapper.readValue(json, AlertMessage.class);

        assertThat(alert.alertId()).isEqualTo(alertId);
        assertThat(alert.eventId()).isEqualTo("evt-1");
        assertThat(alert.accountId()).isEqualTo("acct-9");
        assertThat(alert.ruleName()).isEqualTo("velocity");
        assertThat(alert.reason()).isEqualTo("velocity threshold breach");
        assertThat(alert.producedAt().toString()).isEqualTo("2026-06-18T00:00:00Z");
    }

    @Test
    void unknownFieldFromNewerProducerIsIgnored() {
        // Forward compatibility (ADR-11): a newer relay may stamp the envelope with extra
        // fields (e.g. a schemaVersion, or severity). An older alert-service must ignore them.
        String json = """
            {"alertId":"%s","eventId":"evt-2","accountId":"acct-9",
             "ruleName":"geo","reason":"impossible travel",
             "producedAt":"2026-06-18T00:00:00Z",
             "schemaVersion":"2","severity":"HIGH"}""".formatted(UUID.randomUUID());

        assertThatCode(() -> mapper.readValue(json, AlertMessage.class))
                .doesNotThrowAnyException();
    }

    @Test
    void missingOptionalFreeTextFieldDefaultsToNull() {
        // A leaner producer that omits the free-text reason must not be rejected — the
        // consumer defaults the absent field rather than failing the record.
        String json = """
            {"alertId":"%s","eventId":"evt-3","accountId":"acct-9",
             "ruleName":"zscore","producedAt":"2026-06-18T00:00:00Z"}""".formatted(UUID.randomUUID());

        assertThatCode(() -> {
            AlertMessage alert = mapper.readValue(json, AlertMessage.class);
            assertThat(alert.reason()).isNull();
        }).doesNotThrowAnyException();
    }
}
