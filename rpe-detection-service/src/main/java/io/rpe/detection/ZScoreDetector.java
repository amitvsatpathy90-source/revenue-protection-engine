package io.rpe.detection;

import io.rpe.config.RpeProperties;
import io.rpe.domain.PaymentEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** Precedence 20. See {@link VelocityDetector} — detector order is part of the alert-id contract. */
@Component
@Order(20)
public class ZScoreDetector implements Detector {

    static final String RULE_NAME = "zscore";

    private static final long MIN_SAMPLES = 30;

    private final RpeProperties props;

    public ZScoreDetector(RpeProperties props) {
        this.props = props;
    }

    @Override
    public String ruleName() { return RULE_NAME; }

    @Override
    public DetectionResult evaluate(LuaGateResult result, PaymentEvent event, long brokerIngestMs) {
        // Use POST-update count (after this event is folded in).
        // A pre-gate read sees N-1 for the Nth event — off-by-one; misses first valid evaluation.
        if (result.welfordCount() < MIN_SAMPLES) {
            return DetectionResult.clean("zscore:insufficient-history");
        }

        // Normalise by the EFFECTIVE count — min(n, cap) — matching the window the gate keeps M2 in.
        // ADR-08 amendment (arch-audit): past the cap, gate.lua decays M2 on the same 1/cap schedule
        // as the mean, so M2 is scaled to ~cap samples, NOT to n. Dividing by the true n here would
        // shrink stddev without bound as n grows => runaway false positives. Below the cap this is a
        // no-op (min(n, cap) == n) and the arithmetic is exact Welford, unchanged.
        long effectiveCount = Math.min(result.welfordCount(), props.detection().welford().sampleCap());
        double stddev = Math.sqrt(result.welfordM2() / effectiveCount);
        if (stddev == 0.0) {
            return DetectionResult.clean("zscore:stddev=0,all-amounts-identical");
        }

        double z = Math.abs(event.amount().doubleValue() - result.welfordMean()) / stddev;
        double threshold = props.detection().zScore().threshold();

        if (z > threshold) {
            return DetectionResult.alert(RULE_NAME,
                    "zscore: z=%.2f > threshold=%.1f".formatted(z, threshold));
        }
        return DetectionResult.clean("zscore:ok");
    }
}
