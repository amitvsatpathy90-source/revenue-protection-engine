package io.rpe.detection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DetectionResultTest {

    @Test
    void cleanResultIsNotAlert() {
        var result = DetectionResult.clean("velocity:ok");
        assertThat(result.isAlert()).isFalse();
        assertThat(result.verdict()).isEqualTo(DetectionResult.Verdict.CLEAN);
        assertThat(result.ruleName()).isNull();
    }

    @Test
    void alertResultIsAlert() {
        var result = DetectionResult.alert("velocity", "velocity: 5 events in window");
        assertThat(result.isAlert()).isTrue();
        assertThat(result.verdict()).isEqualTo(DetectionResult.Verdict.ALERT);
        assertThat(result.ruleName()).isEqualTo("velocity");
    }

    @Test
    void alertWithNullRuleNameThrows() {
        // null ruleName would corrupt UUIDv5 alert_id — must be caught early
        assertThatThrownBy(() -> DetectionResult.alert(null, "reason"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("ruleName null corrupts UUIDv5");
    }
}
