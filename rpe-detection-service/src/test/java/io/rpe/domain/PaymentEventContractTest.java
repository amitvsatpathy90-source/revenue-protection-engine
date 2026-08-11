package io.rpe.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * ADR-17 §7 Stage 4 — per-consumer schema contract test for {@code payment.events}
 * (consumed by rpe-detection-service).
 *
 * The contract between this service and its producer is a SCHEMA, not shared code
 * (microservices.md §2): {@code @JsonIgnoreProperties(ignoreUnknown = true)} on
 * {@link PaymentEvent} is what makes schema evolution additive-safe (ADR-11). This test
 * pins that tolerance so a future deploy can add fields to {@code payment.events} without
 * crashing this consumer on buffered/older events.
 *
 * Uses the Jackson 2 {@link ObjectMapper} (same flavour Spring Kafka's {@code JsonDeserializer}
 * uses); {@code findAndRegisterModules()} pulls in jsr310 for {@code Instant}.
 */
class PaymentEventContractTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void fullPayloadBindsAllKnownFields() throws Exception {
        String json = """
            {"eventId":"evt-1","accountId":"acct-9","amount":42.50,
             "lat":37.7749,"lon":-122.4194,"timestamp":"2026-06-18T00:00:00Z",
             "schemaVersion":"1"}""";

        PaymentEvent event = mapper.readValue(json, PaymentEvent.class);

        assertThat(event.eventId()).isEqualTo("evt-1");
        assertThat(event.accountId()).isEqualTo("acct-9");
        assertThat(event.amount()).isEqualByComparingTo(new BigDecimal("42.50"));
        assertThat(event.lat()).isEqualByComparingTo(new BigDecimal("37.7749"));
        assertThat(event.lon()).isEqualByComparingTo(new BigDecimal("-122.4194"));
        assertThat(event.timestamp().toString()).isEqualTo("2026-06-18T00:00:00Z");
        assertThat(event.schemaVersion()).isEqualTo("1");
    }

    @Test
    void unknownFieldFromNewerProducerIsIgnored() {
        // Forward compatibility (ADR-11): a producer that adds a field must not break an
        // older consumer. This is the whole point of @JsonIgnoreProperties on the DTO.
        String json = """
            {"eventId":"evt-2","accountId":"acct-9","amount":10.00,
             "lat":0.0,"lon":0.0,"timestamp":"2026-06-18T00:00:00Z",
             "schemaVersion":"2","merchantCategory":"5411","riskBand":"LOW"}""";

        assertThatCode(() -> mapper.readValue(json, PaymentEvent.class))
                .doesNotThrowAnyException();
    }

    @Test
    void missingOptionalSchemaVersionDefaultsToNull() {
        // schemaVersion is the additive-evolution marker and is itself optional — a leaner
        // or older producer may omit it. The consumer must default it, not reject the event.
        String json = """
            {"eventId":"evt-3","accountId":"acct-9","amount":10.00,
             "lat":0.0,"lon":0.0,"timestamp":"2026-06-18T00:00:00Z"}""";

        assertThatCode(() -> {
            PaymentEvent event = mapper.readValue(json, PaymentEvent.class);
            assertThat(event.schemaVersion()).isNull();
        }).doesNotThrowAnyException();
    }
}
