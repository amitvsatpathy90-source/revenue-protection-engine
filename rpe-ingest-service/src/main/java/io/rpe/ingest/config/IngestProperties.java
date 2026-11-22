package io.rpe.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Typed {@code ingest.*} configuration.
 *
 * @param razorpay      gateway settings; the webhook secret is mandatory
 * @param maxEventAge   delivery age above which a captured event is stale and not forwarded
 * @param maxFutureSkew maximum permitted lead of provider event time over receipt time
 * @param currency      the only accepted currency
 * @param homeLocations coordinate pairs the per-account home location is chosen from
 */
@ConfigurationProperties(prefix = "ingest")
public record IngestProperties(
        Razorpay razorpay,
        @DefaultValue("15m") Duration maxEventAge,
        @DefaultValue("5m") Duration maxFutureSkew,
        @DefaultValue("INR") String currency,
        List<Home> homeLocations) {

    /**
     * @param webhookSecret gateway webhook secret; blank or missing fails startup
     */
    public record Razorpay(String webhookSecret) {
    }

    /**
     * One synthetic home location.
     */
    public record Home(BigDecimal lat, BigDecimal lon) {
    }
}
