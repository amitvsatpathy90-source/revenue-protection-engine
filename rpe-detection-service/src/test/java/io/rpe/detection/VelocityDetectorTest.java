package io.rpe.detection;

import org.junit.jupiter.api.Test;

import static io.rpe.detection.DetectorTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

class VelocityDetectorTest {

    private final VelocityDetector detector = new VelocityDetector(props(3, 3.0, 900.0));

    @Test
    void belowThresholdIsClean() {
        var dr = detector.evaluate(gateResult(2, 1, 0, 0, null), event(10, 0, 0), 1L);
        assertThat(dr.isAlert()).isFalse();
    }

    @Test
    void atThresholdAlerts() {
        // velocityPriorCount is the count BEFORE this event: prior == max means this
        // event is the (max+1)th in the window
        var dr = detector.evaluate(gateResult(3, 1, 0, 0, null), event(10, 0, 0), 1L);
        assertThat(dr.isAlert()).isTrue();
        assertThat(dr.ruleName()).isEqualTo("velocity");
    }

    @Test
    void aboveThresholdAlerts() {
        var dr = detector.evaluate(gateResult(10, 1, 0, 0, null), event(10, 0, 0), 1L);
        assertThat(dr.isAlert()).isTrue();
    }
}
