package io.rpe.detection;

import org.junit.jupiter.api.Test;

import static io.rpe.detection.DetectorTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

class ZScoreDetectorTest {

    private final ZScoreDetector detector = new ZScoreDetector(props(100, 3.0, 900.0));

    @Test
    void insufficientHistoryIsClean() {
        // welfordCount is POST-update: 29 means fewer than 30 samples including this one
        var dr = detector.evaluate(gateResult(0, 29, 50.0, 1000.0, null), event(500, 0, 0), 1L);
        assertThat(dr.isAlert()).isFalse();
        assertThat(dr.reason()).contains("insufficient-history");
    }

    @Test
    void exactlyThirtySamplesIsEvaluated() {
        // Boundary: the 30th event must be evaluated (post-update count gate, not pre-gate
        // read — a pre-gate read would see 29 and miss the first valid evaluation)
        var dr = detector.evaluate(gateResult(0, 30, 50.0, 30.0, null), event(50.0, 0, 0), 1L);
        assertThat(dr.reason()).doesNotContain("insufficient-history");
    }

    @Test
    void zeroStddevIsCleanNotDivideByZero() {
        // M2 = 0 → stddev = 0: all amounts identical; division would yield Infinity/NaN
        var dr = detector.evaluate(gateResult(0, 50, 100.0, 0.0, null), event(100.0, 0, 0), 1L);
        assertThat(dr.isAlert()).isFalse();
        assertThat(dr.reason()).contains("stddev=0");
    }

    @Test
    void outlierAmountAlerts() {
        // mean=100, M2=400, count=100 → stddev=2; amount=200 → z=50 >> 3.0
        var dr = detector.evaluate(gateResult(0, 100, 100.0, 400.0, null), event(200.0, 0, 0), 1L);
        assertThat(dr.isAlert()).isTrue();
        assertThat(dr.ruleName()).isEqualTo("zscore");
    }

    @Test
    void typicalAmountIsClean() {
        // mean=100, stddev=2; amount=103 → z=1.5 < 3.0
        var dr = detector.evaluate(gateResult(0, 100, 100.0, 400.0, null), event(103.0, 0, 0), 1L);
        assertThat(dr.isAlert()).isFalse();
    }

    @Test
    void pastTheSampleCapStddevNormalisesByTheCapNotTheTrueCount() {
        // ADR-08 amendment (arch-audit): the Java half of a matched pair with gate.lua. Past the
        // 10,000 cap the gate decays M2 on the same 1/cap schedule as the mean, so M2 is scaled to
        // ~cap samples — the divisor here must be min(n, cap), not n.
        //
        // count=1,000,000, M2=40,000, mean=100.
        //   correct: stddev = sqrt(40,000 / 10,000) = 2   → z = 3/2   = 1.5  → CLEAN
        //   n-divisor: stddev = sqrt(40,000 / 1e6) = 0.2  → z = 3/0.2 = 15   → ALERT (false positive)
        // So this assertion fails if the divisor regresses to the true count.
        var dr = detector.evaluate(gateResult(0, 1_000_000, 100.0, 40_000.0, null), event(103.0, 0, 0), 1L);
        assertThat(dr.isAlert())
                .as("stddev must be normalised by min(count, sampleCap); dividing a cap-scaled M2 by "
                        + "the true count shrinks stddev without bound → runaway false positives")
                .isFalse();

        // The cap changes the divisor, not the detector's job: a real outlier still fires.
        // amount=200 → z = 100/2 = 50 >> 3.0
        var alert = detector.evaluate(gateResult(0, 1_000_000, 100.0, 40_000.0, null), event(200.0, 0, 0), 1L);
        assertThat(alert.isAlert()).isTrue();
    }
}
