package io.rpe.detection;

import io.rpe.config.RpeProperties;
import io.rpe.domain.PaymentEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Precedence 10 (first). When an event trips more than one rule, {@code PaymentEventConsumer}
 * takes the FIRST firing detector in the injected {@code List<Detector>} and its {@code ruleName}
 * fixes the {@code alert_id} (UUIDv5). That order MUST be pinned: an unordered bean list resolves
 * to Spring's unspecified registration order, so a reorder would silently change the winning rule
 * — and therefore {@code alert_id} — for the same event, breaking {@code processed_alerts} dedup
 * on replay (lua-gate.md). Precedence is part of the alert-id contract; assert it in a test.
 */
@Component
@Order(10)
public class VelocityDetector implements Detector {

    // Immutable after first deployment — changing this corrupts alert_id dedup
    static final String RULE_NAME = "velocity";

    private final RpeProperties props;

    public VelocityDetector(RpeProperties props) {
        this.props = props;
    }

    @Override
    public String ruleName() { return RULE_NAME; }

    @Override
    public DetectionResult evaluate(LuaGateResult result, PaymentEvent event, long brokerIngestMs) {
        long max = props.detection().velocity().maxEvents();
        // velocityPriorCount = ZCOUNT BEFORE adding this event (sliding window, exclusive lower bound)
        if (result.velocityPriorCount() >= max) {
            return DetectionResult.alert(RULE_NAME,
                    "velocity: %d events in window (max=%d)".formatted(
                            result.velocityPriorCount(), max));
        }
        return DetectionResult.clean("velocity:ok");
    }
}
