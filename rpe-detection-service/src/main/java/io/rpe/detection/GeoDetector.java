package io.rpe.detection;

import io.rpe.config.RpeProperties;
import io.rpe.consumer.AccountClassificationService;
import io.rpe.domain.PaymentEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** Precedence 30 (last). See {@link VelocityDetector} — detector order is part of the alert-id contract. */
@Component
@Order(30)
public class GeoDetector implements Detector {

    static final String RULE_NAME = "geo";

    private static final double EARTH_RADIUS_KM = 6_371.0;

    private final RpeProperties props;
    private final AccountClassificationService classificationService;
    private final Counter exemptSkips;

    public GeoDetector(
            RpeProperties props,
            AccountClassificationService classificationService,
            MeterRegistry meterRegistry) {
        this.props = props;
        this.classificationService = classificationService;
        // Bounded counter (no account tag — PII/cardinality rule). Geo bypass must be
        // observable, never silent (ADR-04).
        this.exemptSkips = meterRegistry.counter("rpe.geo.exempt.skip");
    }

    @Override
    public String ruleName() { return RULE_NAME; }

    @Override
    public DetectionResult evaluate(LuaGateResult result, PaymentEvent event, long brokerIngestMs) {
        // Explicit opt-in bypass for sharded/composite-key accounts where per-account
        // ordering is intentionally relaxed (ADR-04). Classification is cached 60s.
        if (classificationService.isGeoExempt(event.accountId())) {
            exemptSkips.increment();
            return DetectionResult.clean("geo:exempt");
        }

        // One null check covers all three co-dependent geo fields (ADR-14).
        // GeoSnapshot.lat/lon/brokerMs are unconditionally valid when record is non-null.
        if (result.geoPrev() == null) {
            return DetectionResult.clean("geo:no-prior-location");
        }

        GeoSnapshot prev = result.geoPrev();
        double distKm = haversineKm(
                prev.lat(), prev.lon(),
                event.lat().doubleValue(), event.lon().doubleValue());

        // Use broker ingest timestamps for time-delta — NOT event.timestamp() (payload drift)
        long deltaMs = brokerIngestMs - prev.brokerMs();
        if (deltaMs <= 0) {
            return DetectionResult.clean("geo:non-positive-time-delta");
        }

        double speedKmh   = distKm / (deltaMs / 3_600_000.0);
        double maxSpeedKmh = props.detection().geo().maxSpeedKmh();

        if (speedKmh > maxSpeedKmh) {
            return DetectionResult.alert(RULE_NAME,
                    "geo: speed=%.1f km/h > max=%.1f km/h (dist=%.1f km, delta=%d ms)"
                            .formatted(speedKmh, maxSpeedKmh, distKm, deltaMs));
        }
        return DetectionResult.clean("geo:ok");
    }

    private static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a    = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 2 * EARTH_RADIUS_KM * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
