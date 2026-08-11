package io.rpe.detection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the raw-reply mapping contract (ADR-14).
 *
 * The load-bearing behaviour: PARTIAL geo state throws — it is never silently coerced
 * to a null geoPrev. Silent coercion turns a Lua regression into invisible missed
 * detections with no signal. Geo violations are IllegalArgumentException; reply-shape
 * violations are IllegalStateException (distinct types drive the violation counter's
 * reason tag without string matching).
 */
class LuaGateResultTest {

    private static List<Object> reply(Object... elements) {
        return Arrays.asList(elements);
    }

    @Test
    void fullReplyWithGeoMapsAllFields() {
        var result = LuaGateResult.from(reply(
                0L, 5L, 42L, "100.5", "250.75", "37.7749", "-122.4194", "1700000000000"));

        assertThat(result.dedupBlocked()).isFalse();
        assertThat(result.velocityPriorCount()).isEqualTo(5L);
        assertThat(result.welfordCount()).isEqualTo(42L);
        assertThat(result.welfordMean()).isEqualTo(100.5);
        assertThat(result.welfordM2()).isEqualTo(250.75);
        assertThat(result.geoPrev()).isNotNull();
        assertThat(result.geoPrev().lat()).isEqualTo(37.7749);
        assertThat(result.geoPrev().lon()).isEqualTo(-122.4194);
        assertThat(result.geoPrev().brokerMs()).isEqualTo(1_700_000_000_000L);
    }

    @Test
    void dedupBlockedMapsToTrue() {
        var result = LuaGateResult.from(reply(1L, 0L, 1L, "10.0", "0.0", null, null, null));
        assertThat(result.dedupBlocked()).isTrue();
    }

    @Test
    void allNullGeoMeansNoPriorLocation() {
        var result = LuaGateResult.from(reply(0L, 0L, 1L, "10.0", "0.0", null, null, null));
        assertThat(result.geoPrev()).isNull();
    }

    @Test
    void truncatedReplyMeansNoPriorLocation() {
        // Redis stops converting a Lua table at the first nil — trailing geo nils can
        // arrive as a 5-element reply. Equivalent to explicit nulls.
        var result = LuaGateResult.from(reply(0L, 0L, 1L, "10.0", "0.0"));
        assertThat(result.geoPrev()).isNull();
    }

    @Test
    void partialGeoStateThrowsInsteadOfCoercingToNull() {
        assertThatThrownBy(() ->
                LuaGateResult.from(reply(0L, 0L, 1L, "10.0", "0.0", "37.0", null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partial geo state")
                .hasMessageContaining("latPresent=true")
                .hasMessageContaining("lonPresent=false");
    }

    @Test
    void missingTimestampWithCoordinatesThrows() {
        assertThatThrownBy(() ->
                LuaGateResult.from(reply(0L, 0L, 1L, "10.0", "0.0", "37.0", "-122.0", null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tsPresent=false");
    }

    @Test
    void outOfRangeGeoValueThrowsGeoViolation() {
        // GeoSnapshot guards propagate through the mapper as IllegalArgumentException
        assertThatThrownBy(() ->
                LuaGateResult.from(reply(0L, 0L, 1L, "10.0", "0.0", "999.0", "-122.0", "1700000000000")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void malformedWelfordFieldThrowsShapeViolation() {
        // Non-geo parse failures are IllegalStateException (reason=shape), and the
        // message names the exception type — never the raw value
        assertThatThrownBy(() ->
                LuaGateResult.from(reply(0L, 0L, 1L, "not-a-number", "0.0", null, null, null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("gate reply shape invalid")
                .hasMessageNotContaining("not-a-number");
    }

    @Test
    void wrongTypeInCountFieldThrowsShapeViolation() {
        assertThatThrownBy(() ->
                LuaGateResult.from(reply(0L, 0L, "not-a-long", "10.0", "0.0", null, null, null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("gate reply shape invalid");
    }

    @Test
    void emptyReplyThrowsShapeViolation() {
        assertThatThrownBy(() -> LuaGateResult.from(reply()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("gate reply shape invalid");
    }
}
