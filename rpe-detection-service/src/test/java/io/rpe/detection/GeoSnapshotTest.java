package io.rpe.detection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the GeoSnapshot constructor guards (ADR-14 / lua-gate.md).
 *
 * Guard ORDER is load-bearing: NaN before range (NaN comparisons are silently false,
 * so the range check alone passes NaN through). Exception messages must describe the
 * violation shape (boolean flags) — never coordinate values (PII rule).
 */
class GeoSnapshotTest {

    @Test
    void validSnapshotConstructs() {
        var snap = new GeoSnapshot(37.7749, -122.4194, 1_700_000_000_000L);
        assertThat(snap.lat()).isEqualTo(37.7749);
        assertThat(snap.lon()).isEqualTo(-122.4194);
        assertThat(snap.brokerMs()).isEqualTo(1_700_000_000_000L);
    }

    @Test
    void boundaryCoordinatesAreValid() {
        new GeoSnapshot(90.0, 180.0, 1L);
        new GeoSnapshot(-90.0, -180.0, 1L);
    }

    @Test
    void nanLatThrowsWithShapeOnlyMessage() {
        assertThatThrownBy(() -> new GeoSnapshot(Double.NaN, 10.0, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("latNaN=true")
                .hasMessageContaining("lonNaN=false");
    }

    @Test
    void nanGuardRunsBeforeRangeGuard() {
        // NaN lat + out-of-range lon: the NaN guard must fire, not the range guard.
        // If the order were reversed, NaN comparisons (silently false) would let
        // NaN through the range check entirely.
        assertThatThrownBy(() -> new GeoSnapshot(Double.NaN, 999.0, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NaN coordinate");
    }

    @Test
    void outOfRangeLatThrowsWithoutLeakingValue() {
        assertThatThrownBy(() -> new GeoSnapshot(91.5, 10.0, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("latOk=false")
                .hasMessageContaining("lonOk=true")
                // PII rule: the offending coordinate value never appears in the message
                .hasMessageNotContaining("91.5")
                .hasMessageNotContaining("10.0");
    }

    @Test
    void infinityIsCaughtByRangeGuard() {
        assertThatThrownBy(() -> new GeoSnapshot(Double.POSITIVE_INFINITY, 0.0, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
        assertThatThrownBy(() -> new GeoSnapshot(0.0, Double.NEGATIVE_INFINITY, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void sentinel999IsCaughtByRangeGuard() {
        assertThatThrownBy(() -> new GeoSnapshot(0.0, 999.0, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void nonPositiveBrokerMsThrows() {
        // Kafka's -1 no-timestamp sentinel and epoch-0 are both invalid
        assertThatThrownBy(() -> new GeoSnapshot(0.0, 0.0, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-positive brokerMs");
        assertThatThrownBy(() -> new GeoSnapshot(0.0, 0.0, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-positive brokerMs");
    }
}
