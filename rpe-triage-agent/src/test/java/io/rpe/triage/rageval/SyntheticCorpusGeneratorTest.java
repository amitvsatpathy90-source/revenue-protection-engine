package io.rpe.triage.rageval;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyntheticCorpusGeneratorTest {

    private static final Map<String, List<String>> CLUSTER_MARKERS = Map.of(
            "velocity", List.of(
                    "one-minute transaction burst",
                    "payment retry",
                    "low-value authorization",
                    "single-merchant",
                    "multiple merchants",
                    "rate baseline",
                    "near-duplicate payment"),
            "zscore", List.of(
                    "large-purchase z-score",
                    "rolling-mean deviation",
                    "daily-spend",
                    "merchant-baseline",
                    "below-baseline z-score",
                    "repeated-amount",
                    "variance-shift"),
            "geo", List.of(
                    "impossible-travel",
                    "cross-border location",
                    "distant-merchant",
                    "ip-location mismatch",
                    "home-region",
                    "travel-speed",
                    "same-card travel"));

    @Test
    void generatedClusterLabelsMatchTheirSemanticTemplates() {
        var corpus = new SyntheticCorpusGenerator().generate();

        assertEquals(300, corpus.size());

        for (var entry : CLUSTER_MARKERS.entrySet()) {
            String rule = entry.getKey();
            var ruleChunks = corpus.stream().filter(c -> c.ruleName().equals(rule)).toList();

            assertEquals(100, ruleChunks.size(), rule);
            assertEquals(95, ruleChunks.stream().filter(c -> c.relevanceGrade() > 0).count(), rule);
            assertEquals(5, ruleChunks.stream().filter(c -> c.relevanceGrade() == 0).count(), rule);

            for (int i = 0; i < entry.getValue().size(); i++) {
                String clusterId = rule + "-c" + i;
                String marker = entry.getValue().get(i);
                var clusteredChunks = ruleChunks.stream()
                        .filter(c -> c.clusterId().equals(clusterId))
                        .toList();

                assertFalse(clusteredChunks.isEmpty(), "Missing cluster " + clusterId);
                assertTrue(
                        clusteredChunks.stream().allMatch(c ->
                                c.content().toLowerCase(Locale.ROOT).contains(marker)),
                        "Cluster " + clusterId + " must consistently use theme marker: " + marker);
            }
        }
    }
}
