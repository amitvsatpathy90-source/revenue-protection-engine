package io.rpe.ingest.mapping;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Maps a captured payment to detection's input contract (ADR-33 decisions 5-7). Pure and deterministic:
 * the caller supplies the receive time, so there is no clock dependency.
 *
 * <p>Geo: the gateway carries no coordinates, so each account gets a stable home location from configuration;
 * {@code notes.lat}/{@code notes.lon} override it for scenario injection. Results are synthetic geography.
 */
public final class PaymentEventMapper {

    private static final BigDecimal LAT_LIMIT = new BigDecimal("90");
    private static final BigDecimal LON_LIMIT = new BigDecimal("180");
    private static final String SCHEMA_VERSION = "1";
    private static final int MINOR_UNIT_SCALE = 2; // INR: 100 paise = 1 rupee

    /**
     * One configured coordinate pair.
     */
    public record GeoPoint(BigDecimal lat, BigDecimal lon) {
    }

    /**
     * @param maxEventAge   maximum delivery age before an event is stale
     * @param maxFutureSkew maximum permitted lead of provider event time over receipt time
     * @param homeLocations non-empty list used to choose each account's home location
     * @param currency      only accepted currency; others would corrupt detection statistics
     */
    public record Config(Duration maxEventAge, Duration maxFutureSkew, List<GeoPoint> homeLocations, String currency) {

        public Config {
            if (maxEventAge == null || maxEventAge.isZero() || maxEventAge.isNegative()) {
                throw new IllegalArgumentException("max-event-age must be positive");
            }
            if (maxFutureSkew == null || maxFutureSkew.isNegative()) {
                throw new IllegalArgumentException("max-future-skew must be zero or positive");
            }
            if (homeLocations == null || homeLocations.isEmpty()) {
                throw new IllegalArgumentException("home-locations must not be empty");
            }
            validateHomeLocations(homeLocations);
            if (currency == null || currency.isBlank()) {
                throw new IllegalArgumentException("currency must be set");
            }

            // Prevent callers from mutating the configuration after validation.
            homeLocations = List.copyOf(homeLocations);
        }

        private static void validateHomeLocations(List<GeoPoint> homes) {
            for (int i = 0; i < homes.size(); i++) {
                GeoPoint home = homes.get(i);

                if (home == null) {
                    throw new IllegalArgumentException("null home location at index " + i);
                }
                if (home.lat() == null || home.lon() == null) {
                    throw new IllegalArgumentException("null home coordinates at index " + i);
                }

                // Coordinate values must never appear in exception messages (ADR-14).
                if (home.lat().abs().compareTo(LAT_LIMIT) > 0) {
                    throw new IllegalArgumentException(
                            "home location latitude out of range at index " + i);
                }
                if (home.lon().abs().compareTo(LON_LIMIT) > 0) {
                    throw new IllegalArgumentException(
                            "home location longitude out of range at index " + i);
                }
            }
        }
    }

    private final Config config;

    public PaymentEventMapper(Config config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * @param payment    extracted payment fields
     * @param receivedAt server-side receive time of the webhook
     * @return Forward, Rejected (invalid input) or Stale (delivered too late); validity is checked before age
     */
    public MappingOutcome map(CapturedPayment payment, Instant receivedAt) {
        Objects.requireNonNull(payment, "payment");
        Objects.requireNonNull(receivedAt, "receivedAt");

        if (isBlank(payment.paymentId())) {
            return new MappingOutcome.Rejected("missing-payment-id");
        }
        if (isBlank(payment.accountId())) {
            return new MappingOutcome.Rejected("missing-account-id");
        }
        if (!config.currency().equals(payment.currency())) {
            return new MappingOutcome.Rejected("unsupported-currency");
        }
        if (payment.amountMinor() < 0) {
            return new MappingOutcome.Rejected("invalid-amount");
        }

        GeoPoint geo;
        if (payment.latNote() == null && payment.lonNote() == null) {
            geo = homeLocation(payment.accountId());
        } else {
            geo = parseOverride(payment.latNote(), payment.lonNote());
            if (geo == null) {
                return new MappingOutcome.Rejected("invalid-geo-override");
            }
        }

        Instant eventTime = Instant.ofEpochSecond(payment.eventCreatedAtEpochSec());
        Duration age = Duration.between(eventTime, receivedAt);

        // PaymentEvent.timestamp becomes AlertMessage.producedAt.
        // Bound future skew to protect downstream business-time semantics,
        // not Kafka partition ordering.
        if (age.isNegative()
                && age.negated().compareTo(config.maxFutureSkew()) > 0) {
            return new MappingOutcome.Rejected("future-dated");
        }

        if (age.compareTo(config.maxEventAge()) > 0) {
            return new MappingOutcome.Stale(age);
        }

        return new MappingOutcome.Forward(new OutboundPaymentEvent(
                payment.paymentId(),
                payment.accountId(),
                BigDecimal.valueOf(payment.amountMinor(), MINOR_UNIT_SCALE),
                geo.lat(),
                geo.lon(),
                eventTime,
                SCHEMA_VERSION));
    }

    /**
     * Stable per account: String.hashCode is specified by the JLS, so the choice survives restarts.
     */
    private GeoPoint homeLocation(String accountId) {
        List<GeoPoint> homes = config.homeLocations();
        return homes.get(Math.floorMod(accountId.hashCode(), homes.size()));
    }

    /**
     * Both notes must be present, numeric and within coordinate range; otherwise null.
     */
    private static GeoPoint parseOverride(String latNote, String lonNote) {
        if (latNote == null || lonNote == null) {
            return null;
        }

        try {
            BigDecimal lat = new BigDecimal(latNote.trim());
            BigDecimal lon = new BigDecimal(lonNote.trim());

            if (lat.abs().compareTo(LAT_LIMIT) > 0
                    || lon.abs().compareTo(LON_LIMIT) > 0) {
                return null;
            }

            return new GeoPoint(lat, lon);
        } catch (NumberFormatException notNumeric) {
            return null;
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
