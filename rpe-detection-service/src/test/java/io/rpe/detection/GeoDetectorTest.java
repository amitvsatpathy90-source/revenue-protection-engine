package io.rpe.detection;

import io.rpe.consumer.AccountClassificationService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.rpe.detection.DetectorTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

class GeoDetectorTest {

    /** Hand-rolled stub — no Redis, no Mockito/Byte Buddy JVM-version coupling. */
    static final class StubClassification extends AccountClassificationService {
        boolean geoExempt = false;

        StubClassification() {
            super(null, null); // redis + laneScheduler unused — all methods overridden
        }

        @Override
        public boolean isGeoExempt(String accountId) {
            return geoExempt;
        }

        @Override
        public boolean isElevated(String accountId) {
            return false;
        }
    }

    private StubClassification classification;
    private SimpleMeterRegistry meterRegistry;
    private GeoDetector detector;

    @BeforeEach
    void setUp() {
        classification = new StubClassification();
        meterRegistry = new SimpleMeterRegistry();
        detector = new GeoDetector(props(100, 3.0, 900.0), classification, meterRegistry);
    }

    @Test
    void geoExemptAccountIsSkippedAndCounted() {
        classification.geoExempt = true;

        var dr = detector.evaluate(
                gateResult(0, 1, 0, 0, new GeoSnapshot(0, 0, 1L)), event(10, 50, 50), 2L);

        assertThat(dr.isAlert()).isFalse();
        assertThat(dr.reason()).isEqualTo("geo:exempt");
        assertThat(meterRegistry.counter("rpe.geo.exempt.skip").count()).isEqualTo(1.0);
    }

    @Test
    void noPriorLocationIsClean() {
        var dr = detector.evaluate(gateResult(0, 1, 0, 0, null), event(10, 50, 50), 2L);
        assertThat(dr.isAlert()).isFalse();
        assertThat(dr.reason()).isEqualTo("geo:no-prior-location");
    }

    @Test
    void impossibleTravelSpeedAlerts() {
        // SF → NYC (~4,130 km) in 1 minute, measured by broker timestamps
        long prevBrokerMs = 1_700_000_000_000L;
        var prev = new GeoSnapshot(37.7749, -122.4194, prevBrokerMs);

        var dr = detector.evaluate(
                gateResult(0, 1, 0, 0, prev),
                event(10, 40.7128, -74.0060),
                prevBrokerMs + 60_000);

        assertThat(dr.isAlert()).isTrue();
        assertThat(dr.ruleName()).isEqualTo("geo");
    }

    @Test
    void plausibleTravelSpeedIsClean() {
        // ~111 km (1 degree of latitude) in 2 hours ≈ 55 km/h
        long prevBrokerMs = 1_700_000_000_000L;
        var prev = new GeoSnapshot(37.0, -122.0, prevBrokerMs);

        var dr = detector.evaluate(
                gateResult(0, 1, 0, 0, prev),
                event(10, 38.0, -122.0),
                prevBrokerMs + 2 * 3_600_000);

        assertThat(dr.isAlert()).isFalse();
    }

    @Test
    void nonPositiveTimeDeltaIsClean() {
        // Broker clock anomaly / same-ms ingest: speed is undefined, never alert
        long prevBrokerMs = 1_700_000_000_000L;
        var prev = new GeoSnapshot(37.0, -122.0, prevBrokerMs);

        var dr = detector.evaluate(
                gateResult(0, 1, 0, 0, prev), event(10, 40.0, -74.0), prevBrokerMs);

        assertThat(dr.isAlert()).isFalse();
        assertThat(dr.reason()).isEqualTo("geo:non-positive-time-delta");
    }
}
