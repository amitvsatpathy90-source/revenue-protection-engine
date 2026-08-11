package io.rpe.detection;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Detector precedence is part of the {@code alert_id} contract (lua-gate.md).
 *
 * {@code PaymentEventConsumer} takes the FIRST firing detector in the injected {@code List<Detector>}
 * and its {@code ruleName} fixes the UUIDv5 {@code alert_id}. Spring orders an injected {@code List<T>}
 * with {@link AnnotationAwareOrderComparator} ({@code @Order}/{@code Ordered}); this test drives that
 * exact comparator — container-free — to pin the order {@code velocity < zscore < geo}. An unordered
 * bean list would resolve to Spring's unspecified registration order, so a reorder could silently
 * change which rule wins a multi-fire event and break {@code processed_alerts} dedup on replay.
 * That regression fails here first.
 */
class DetectorPrecedenceTest {

    @Test
    void detectorsAreOrderedVelocityThenZscoreThenGeo() {
        // Constructor args are irrelevant to ruleName()/ordering. GeoDetector's constructor
        // registers a counter, so it needs a real (empty) MeterRegistry; the others just store props.
        List<Detector> detectors = new ArrayList<>(List.of(
                new GeoDetector(null, null, new SimpleMeterRegistry()),
                new ZScoreDetector(null),
                new VelocityDetector(null)));

        // Exactly how Spring sorts an injected List<Detector> at the injection point.
        AnnotationAwareOrderComparator.sort(detectors);

        assertThat(detectors)
                .extracting(Detector::ruleName)
                .containsExactly("velocity", "zscore", "geo");
    }
}
