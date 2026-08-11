package io.rpe.detection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the RULE_NAME constants for each Detector implementation.
 *
 * These strings are used verbatim in UUIDv5(NAMESPACE, eventId + ":" + ruleName()).
 * Changing a RULE_NAME after first deployment permanently breaks alert_id dedup for
 * previously-generated alert_ids: the same event now produces a different UUID,
 * and processed_alerts no longer catches the duplicate (replay safety broken).
 *
 * Failing these tests means the change is a correctness regression, not a style preference.
 */
class DetectorRuleNameTest {

    @Test
    void velocityRuleNameIsImmutable() {
        assertThat(VelocityDetector.RULE_NAME).isEqualTo("velocity");
    }

    @Test
    void zScoreRuleNameIsImmutable() {
        assertThat(ZScoreDetector.RULE_NAME).isEqualTo("zscore");
    }

    @Test
    void geoRuleNameIsImmutable() {
        assertThat(GeoDetector.RULE_NAME).isEqualTo("geo");
    }
}
