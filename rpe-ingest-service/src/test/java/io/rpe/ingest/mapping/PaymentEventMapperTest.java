package io.rpe.ingest.mapping;

import io.rpe.ingest.mapping.PaymentEventMapper.GeoPoint;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaymentEventMapperTest {

    private static final long EVENT_EPOCH = 1_691_735_748L;
    private static final Instant ON_TIME = Instant.ofEpochSecond(EVENT_EPOCH + 60);
    private static final GeoPoint DELHI = new GeoPoint(new BigDecimal("28.6139"), new BigDecimal("77.2090"));
    private static final GeoPoint MUMBAI = new GeoPoint(new BigDecimal("19.0760"), new BigDecimal("72.8777"));
    private static final GeoPoint BENGALURU = new GeoPoint(new BigDecimal("12.9716"), new BigDecimal("77.5946"));
    private static final Duration MAX_EVENT_AGE = Duration.ofMinutes(15);
    private static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(5);

    private final PaymentEventMapper mapper = new PaymentEventMapper(
            new PaymentEventMapper.Config(MAX_EVENT_AGE, MAX_FUTURE_SKEW, List.of(DELHI, MUMBAI, BENGALURU), "INR"));

    private static CapturedPayment payment(String account, String lat, String lon) {
        return new CapturedPayment(
                "pay_DESlfW9H8K9uqM", "INR", 29_900L, account, lat, lon, EVENT_EPOCH);
    }

    @Test
    void forwardsValidPayment() {
        var out = assertInstanceOf(MappingOutcome.Forward.class, mapper.map(payment("acc-1", null, null), ON_TIME));
        var e = out.event();
        assertEquals("pay_DESlfW9H8K9uqM", e.eventId());          // eventId is the payment id
        assertEquals("acc-1", e.accountId());
        assertEquals(new BigDecimal("299.00"), e.amount());       // 29900 paise -> 299.00
        assertEquals(Instant.ofEpochSecond(EVENT_EPOCH), e.timestamp());
        assertEquals("1", e.schemaVersion());
    }

    @Test
    void homeLocationIsStableAndConfigured() {
        var first = ((MappingOutcome.Forward) mapper.map(payment("acc-stable", null, null), ON_TIME)).event();
        var second = ((MappingOutcome.Forward) mapper.map(payment("acc-stable", null, null), ON_TIME)).event();
        assertEquals(first.lat(), second.lat());
        assertEquals(first.lon(), second.lon());
        assertTrue(List.of(DELHI, MUMBAI, BENGALURU).contains(new GeoPoint(first.lat(), first.lon())));
    }

    @Test
    void usesGeoOverrideWhenBothNotesValid() {
        var e = ((MappingOutcome.Forward) mapper.map(payment("acc-1", "51.5072", "-0.1276"), ON_TIME)).event();
        assertEquals(new BigDecimal("51.5072"), e.lat());
        assertEquals(new BigDecimal("-0.1276"), e.lon());
    }

    @Test
    void rejectsPartialOrInvalidGeoOverride() {
        assertEquals(new MappingOutcome.Rejected("invalid-geo-override"),
                mapper.map(payment("acc-1", "10", null), ON_TIME));
        assertEquals(new MappingOutcome.Rejected("invalid-geo-override"),
                mapper.map(payment("acc-1", null, "10"), ON_TIME));
        assertEquals(new MappingOutcome.Rejected("invalid-geo-override"),
                mapper.map(payment("acc-1", "abc", "10"), ON_TIME));
        assertEquals(new MappingOutcome.Rejected("invalid-geo-override"),
                mapper.map(payment("acc-1", "91", "10"), ON_TIME));
        assertEquals(new MappingOutcome.Rejected("invalid-geo-override"),
                mapper.map(payment("acc-1", "10", "-181"), ON_TIME));
    }

    @Test
    void rejectsInvalidPayments() {
        assertEquals(new MappingOutcome.Rejected("missing-account-id"), mapper.map(payment(null, null, null), ON_TIME));
        assertEquals(new MappingOutcome.Rejected("missing-account-id"), mapper.map(payment("  ", null, null), ON_TIME));
        var usd = new CapturedPayment("pay_1", "USD", 100L, "acc-1", null, null, EVENT_EPOCH);
        assertEquals(new MappingOutcome.Rejected("unsupported-currency"), mapper.map(usd, ON_TIME));
        var negative = new CapturedPayment("pay_1", "INR", -1L, "acc-1", null, null, EVENT_EPOCH);
        assertEquals(new MappingOutcome.Rejected("invalid-amount"), mapper.map(negative, ON_TIME));
        var noId = new CapturedPayment(" ", "INR", 100L, "acc-1", null, null, EVENT_EPOCH);
        assertEquals(new MappingOutcome.Rejected("missing-payment-id"), mapper.map(noId, ON_TIME));
    }

    @Test
    void staleOnlyBeyondMaxAge() {
        var atBound = Instant.ofEpochSecond(EVENT_EPOCH).plus(Duration.ofMinutes(15));
        assertInstanceOf(MappingOutcome.Forward.class, mapper.map(payment("acc-1", null, null), atBound));
        var past = atBound.plusSeconds(1);
        var stale = assertInstanceOf(MappingOutcome.Stale.class, mapper.map(payment("acc-1", null, null), past));
        assertEquals(Duration.ofMinutes(15).plusSeconds(1), stale.age());
    }

    @Test
    void invalidInputWinsOverStaleness() {
        var late = Instant.ofEpochSecond(EVENT_EPOCH).plus(Duration.ofHours(2));
        assertEquals(new MappingOutcome.Rejected("missing-account-id"), mapper.map(payment(null, null, null), late));
    }

    @Test
    void configRejectsBadValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        Duration.ZERO, MAX_FUTURE_SKEW, List.of(DELHI), "INR"));

        assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        Duration.ofMinutes(1), MAX_FUTURE_SKEW, List.of(), "INR"));

        assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        Duration.ofMinutes(1), MAX_FUTURE_SKEW, List.of(DELHI), " "));
    }

    @Test
    void configRejectsOutOfRangeHomeLocation() {
        var latFailure = assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        MAX_EVENT_AGE, MAX_FUTURE_SKEW,
                        List.of(new GeoPoint(new BigDecimal("91"), BigDecimal.ZERO)),
                        "INR"));

        assertEquals("home location latitude out of range at index 0",
                latFailure.getMessage());

        var lonFailure = assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        MAX_EVENT_AGE, MAX_FUTURE_SKEW,
                        List.of(new GeoPoint(BigDecimal.ZERO, new BigDecimal("181"))),
                        "INR"));

        assertEquals("home location longitude out of range at index 0",
                lonFailure.getMessage());
    }

    @Test
    void configRejectsNullHomeLocation() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        MAX_EVENT_AGE, MAX_FUTURE_SKEW,
                        java.util.Collections.singletonList(null),
                        "INR"));

        assertEquals("null home location at index 0", failure.getMessage());
    }

    @Test
    void configRejectsNullHomeCoordinates() {
        assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        MAX_EVENT_AGE, MAX_FUTURE_SKEW,
                        List.of(new GeoPoint(null, BigDecimal.ZERO)), "INR"));

        assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        MAX_EVENT_AGE, MAX_FUTURE_SKEW,
                        List.of(new GeoPoint(BigDecimal.ZERO, null)), "INR"));
    }

    @Test
    void configRejectsInvalidFutureSkew() {
        assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        MAX_EVENT_AGE, null, List.of(DELHI), "INR"));

        assertThrows(IllegalArgumentException.class,
                () -> new PaymentEventMapper.Config(
                        MAX_EVENT_AGE, Duration.ofSeconds(-1),
                        List.of(DELHI), "INR"));
    }

    @Test
    void allowsFutureTimestampAtToleranceBoundary() {
        Instant eventTime = Instant.ofEpochSecond(EVENT_EPOCH);
        Instant receivedAt = eventTime.minus(MAX_FUTURE_SKEW);

        assertInstanceOf(
                MappingOutcome.Forward.class,
                mapper.map(payment("acc-1", null, null), receivedAt));
    }

    @Test
    void rejectsFutureTimestampBeyondTolerance() {
        Instant eventTime = Instant.ofEpochSecond(EVENT_EPOCH);
        Instant receivedAt = eventTime.minus(MAX_FUTURE_SKEW).minusSeconds(1);

        assertEquals(
                new MappingOutcome.Rejected("future-dated"),
                mapper.map(payment("acc-1", null, null), receivedAt));
    }

    @Test
    void rejectsNullMapperArgumentsImmediately() {
        assertThrows(NullPointerException.class,
                () -> mapper.map(null, ON_TIME));

        assertThrows(NullPointerException.class,
                () -> mapper.map(payment("acc-1", null, null), null));
    }
}
