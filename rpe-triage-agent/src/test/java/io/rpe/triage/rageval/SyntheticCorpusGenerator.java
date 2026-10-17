package io.rpe.triage.rageval;

import java.util.*;
import java.util.function.Function;


/**
 * Deterministic synthetic historical-alert corpus — same output every run, seed=42.
 *
 * <p>This generator creates evaluation-only data for the RAG benchmark. The generated
 * cluster and relevance values are ground truth for measuring retrieval/selection quality.
 */
final class SyntheticCorpusGenerator {

    private static final long SEED = 42L;

    /*
     * Map<String, List<Function<Random, String>>>
     *
     * key   = detector rule name
     * value = list of text-generation functions for that rule
     *
     */
    private static final Map<String, List<Function<Random, String>>> TEMPLATES = Map.of(
            "velocity", List.of(
                    rng -> "Account " + acct(rng) + " exceeded " + rng.nextInt(50, 500) + " transactions in 60s, burst pattern.",
                    rng -> "Rapid-fire charges on " + acct(rng) + ": " + rng.nextInt(50, 500) + " events within the velocity window."),
            "zscore", List.of(
                    rng -> "Account " + acct(rng) + " amount " + amt(rng) + " deviates " + sigma(rng) + "\u03c3 from rolling mean.",
                    rng -> "Z-score anomaly: " + acct(rng) + " spent " + amt(rng) + ", " + sigma(rng) + "\u03c3 above baseline."),
            "geo", List.of(
                    rng -> "Account " + acct(rng) + ": charges " + rng.nextInt(100, 9000) + "km apart within " + rng.nextInt(1, 30) + "min.",
                    rng -> "Impossible-travel flag on " + acct(rng) + ", " + rng.nextInt(100, 9000) + "km / " + rng.nextInt(1, 30) + "min.")
    );

    /**
     * Generates the seeded synthetic corpus.
     *
     * <p>Per rule: 95 clustered rows + 5 distractor rows = 100 rows.
     * Three rules produce 300 rows in total.
     */
    List<CorpusChunk> generate() {

        // One Random instance seeded once so the generated values are reproducible.
        var rng = new Random(SEED);

        // Mutable list that will hold the generated corpus rows.
        var chunks = new ArrayList<CorpusChunk>();

        // Sequential identifier used to make every generated chunk unique.
        int id = 0;

        for (var rule : TEMPLATES.keySet()) {

            // Partition this rule's 95 examples into seven labeled clusters.
            int clusters = 7;
            int perCluster = 95 / clusters;  // 13 rows per cluster before remainder distribution
            int remainder = 95 % clusters;   // 4 extra rows go to the first four clusters

            for (int c = 0; c < clusters; c++) {

                // Example: velocity-c0, velocity-c1, ..., geo-c6.
                String clusterId = rule + "-c" + c;

                // Distribute the remainder: four clusters get 14 rows; three get 13.
                int clusterSize = perCluster + (c < remainder ? 1 : 0);

                for (int m = 0; m < clusterSize; m++) {
                    chunks.add(new CorpusChunk(
                            "chunk-" + id++,
                            rule,
                            clusterId,
                            render(rule, rng),
                            2)); // Evaluation-only relevance label; not used by retrieval/selection.
                }
            }

            // Five additional distractor rows are generated for each rule.
            // distractors: same rule's template, own cluster, irrelevant to *other* rules' queries
            for (int i = 0; i < 5; i++) {

                chunks.add(new CorpusChunk(
                        "distractor-" + id++,
                        rule,
                        rule + "-distractor",
                        render(rule, rng),
                        0)); // Evaluation-only relevance label; not used by retrieval/selection.
            }
        }

        // Return the completed synthetic corpus.
        return chunks;
    }

    /*
     * Select one of the predefined text templates for the requested rule
     * and execute it using the supplied Random instance.
     */
    private String render(String rule, Random rng) {
        var templates = TEMPLATES.get(rule);
        return templates.get(rng.nextInt(templates.size())).apply(rng);
    }

    /*
     * Helper that generates a synthetic account identifier.
     *
     */
    private static String acct(Random rng) {
        return "acct-" + rng.nextInt(10000, 99999);
    }

    /*
     * Helper that generates a synthetic monetary value with two decimal places.
     */
    private static String amt(Random rng) {
        return String.format("%.2f", rng.nextDouble(50, 5000));
    }

    /*
     * Helper that generates a synthetic z-score value with one decimal place.
     */
    private static String sigma(Random rng) {
        return String.format("%.1f", rng.nextDouble(2, 6));
    }
}
