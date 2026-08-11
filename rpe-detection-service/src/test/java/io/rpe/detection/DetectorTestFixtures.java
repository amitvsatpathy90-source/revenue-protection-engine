package io.rpe.detection;

import io.rpe.config.RpeProperties;
import io.rpe.domain.PaymentEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Shared builders for detector unit tests. */
final class DetectorTestFixtures {

    private DetectorTestFixtures() {}

    /** Detection-only properties; non-detection sections are unused by detectors. */
    static RpeProperties props(int velocityMaxEvents, double zScoreThreshold, double geoMaxSpeedKmh) {
        return new RpeProperties(
                new RpeProperties.ResilienceProperties(new RpeProperties.RedisResilienceProperties(500)),
                new RpeProperties.RateLimitingProperties(500, 2000, 60_000, 5_000),
                new RpeProperties.DetectionProperties(
                        new RpeProperties.VelocityProperties(60_000, velocityMaxEvents, 120_000),
                        new RpeProperties.ZScoreProperties(zScoreThreshold),
                        new RpeProperties.GeoProperties(geoMaxSpeedKmh, 86_400_000),
                        new RpeProperties.WelfordProperties(10_000),
                        new RpeProperties.DedupProperties(300),
                        new RpeProperties.StatsProperties(2_592_000_000L)));
    }

    static PaymentEvent event(double amount, double lat, double lon) {
        return new PaymentEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                BigDecimal.valueOf(amount),
                BigDecimal.valueOf(lat),
                BigDecimal.valueOf(lon),
                Instant.now(),
                "1");
    }

    static LuaGateResult gateResult(long priorCount, long welfordCount, double mean, double m2,
                                    GeoSnapshot geoPrev) {
        return new LuaGateResult(false, priorCount, welfordCount, mean, m2, geoPrev);
    }
}
