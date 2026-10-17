package io.rpe.triage.rageval;

import java.util.*;
import java.util.function.Function;

/**
 * Deterministic synthetic historical-alert corpus — seed=42.
 *
 * <p>This generator creates evaluation-only data. Every positive cluster has its own
 * semantic theme, and each row's text is generated from that cluster's templates.
 * The cluster labels therefore represent meaningful subtopics for the label-based
 * diversity metric; they are no longer arbitrary partitions of otherwise interchangeable
 * examples.
 */
final class SyntheticCorpusGenerator {

    private static final long SEED = 42L;

    // Explicit iteration order keeps the seeded corpus stable across JVM runs.
    private static final List<String> RULES = List.of("velocity", "zscore", "geo");

    /*
     * Rule -> seven semantic subtopics -> two wording variants per subtopic.
     * All templates within one subtopic share a stable theme phrase, which is also checked
     * by SyntheticCorpusGeneratorTest.
     */
    private static final Map<String, List<List<Function<Random, String>>>> CLUSTER_TEMPLATES =
            Map.of(
                    "velocity", List.of(
                            variants(
                                    rng -> "Account " + acct(rng) + " triggered a one-minute transaction burst: "
                                            + rng.nextInt(50, 500) + " transactions arrived in 60 seconds.",
                                    rng -> "One-minute transaction burst for " + acct(rng) + ": "
                                            + rng.nextInt(50, 500) + " payments arrived above the normal rate."),
                            variants(
                                    rng -> "Payment retry anomaly for " + acct(rng) + ": "
                                            + rng.nextInt(8, 40) + " declined attempts repeated within 60 seconds.",
                                    rng -> "Payment retry spike on " + acct(rng) + ": multiple declined payment attempts "
                                            + "were retried rapidly in one minute."),
                            variants(
                                    rng -> "Low-value authorization testing on " + acct(rng) + ": "
                                            + rng.nextInt(20, 100) + " small authorization checks arrived within 60 seconds.",
                                    rng -> "Card-testing burst for " + acct(rng) + " with "
                                            + rng.nextInt(20, 100) + " low-value authorization requests in one minute."),
                            variants(
                                    rng -> "Single-merchant velocity anomaly for " + acct(rng) + ": "
                                            + rng.nextInt(10, 60) + " purchases at one merchant in two minutes.",
                                    rng -> "Account " + acct(rng) + " sent "
                                            + rng.nextInt(10, 60) + " consecutive charges to a single-merchant destination "
                                            + "within 120 seconds."),
                            variants(
                                    rng -> "Multiple merchants received rapid payments from " + acct(rng) + ": "
                                            + rng.nextInt(10, 60) + " purchases arrived within 90 seconds.",
                                    rng -> "A multiple merchants burst was detected for " + acct(rng)
                                            + " after rapid purchases across separate merchants in one minute."),
                            variants(
                                    rng -> "Rate baseline alert for " + acct(rng) + ": transaction velocity rose from "
                                            + rng.nextInt(1, 10) + " per hour to " + rng.nextInt(20, 100)
                                            + " in the latest minute.",
                                    rng -> "Transaction rate baseline deviation for " + acct(rng)
                                            + ": the recent count exceeds its rolling hourly average."),
                            variants(
                                    rng -> "Near-duplicate payment requests for " + acct(rng) + ": "
                                            + rng.nextInt(3, 15) + " almost identical charge payloads arrived within seconds.",
                                    rng -> "Near-duplicate payment burst on " + acct(rng)
                                            + ": repeated payment payloads indicate rapid duplicate-charge attempts.")),
                    "zscore", List.of(
                            variants(
                                    rng -> "Large-purchase z-score alert for " + acct(rng) + ": amount "
                                            + amt(rng) + " is " + sigma(rng) + " standard deviations above its baseline.",
                                    rng -> "Large-purchase z-score anomaly: " + acct(rng) + " made a "
                                            + amt(rng) + " purchase far above the account's normal amount."),
                            variants(
                                    rng -> "Rolling-mean deviation for " + acct(rng) + ": transaction amount "
                                            + amt(rng) + " differs by " + sigma(rng) + " standard deviations.",
                                    rng -> "A rolling-mean deviation z-score flagged " + acct(rng) + " because amount "
                                            + amt(rng) + " is several standard deviations from its recent average."),
                            variants(
                                    rng -> "Daily-spend z-score anomaly for " + acct(rng) + ": total spend "
                                            + amt(rng) + " is " + sigma(rng) + " standard deviations above its daily norm.",
                                    rng -> "Daily-spend deviation for " + acct(rng) + ": accumulated charges exceed the "
                                            + "expected daily amount by " + sigma(rng) + " standard deviations."),
                            variants(
                                    rng -> "Merchant-baseline deviation for " + acct(rng) + ": a purchase of "
                                            + amt(rng) + " is unusually large for this merchant relationship.",
                                    rng -> "Merchant-baseline z-score for " + acct(rng) + " shows amount "
                                            + amt(rng) + " well outside the usual merchant-specific range."),
                            variants(
                                    rng -> "Below-baseline z-score for " + acct(rng) + ": amount "
                                            + amt(rng) + " is " + sigma(rng) + " standard deviations below the rolling mean.",
                                    rng -> "Below-baseline z-score anomaly for " + acct(rng) + ": transaction amount "
                                            + amt(rng) + " is far below its normal range."),
                            variants(
                                    rng -> "Repeated-amount anomaly for " + acct(rng) + ": several transactions of "
                                            + amt(rng) + " differ from the account's expected amount distribution.",
                                    rng -> "Repeated-amount z-score alert for " + acct(rng)
                                            + ": a recurring amount is unusually far from the learned baseline."),
                            variants(
                                    rng -> "Variance-shift z-score for " + acct(rng) + ": transaction amounts have "
                                            + "moved sharply away from the historical distribution.",
                                    rng -> "Amount variance-shift alert for " + acct(rng) + ": recent values deviate "
                                            + "by " + sigma(rng) + " standard deviations from historical behavior.")),
                    "geo", List.of(
                            variants(
                                    rng -> "Impossible-travel alert for " + acct(rng) + ": charges "
                                            + rng.nextInt(1000, 9000) + " km apart occurred within "
                                            + rng.nextInt(1, 20) + " minutes.",
                                    rng -> "Impossible-travel pattern: " + acct(rng) + " appeared thousands of kilometres "
                                            + "apart inside a short transaction window."),
                            variants(
                                    rng -> "Cross-border location anomaly for " + acct(rng) + ": the same session contains "
                                            + "authorizations from two countries within minutes.",
                                    rng -> "Cross-border location jump on " + acct(rng)
                                            + ": payment activity changed countries faster than travel permits."),
                            variants(
                                    rng -> "Distant-merchant locations for " + acct(rng) + ": two merchants "
                                            + rng.nextInt(500, 8000) + " km apart authorized charges within "
                                            + rng.nextInt(2, 25) + " minutes.",
                                    rng -> "Distant-merchant anomaly: " + acct(rng)
                                            + " had near-simultaneous charges at geographically separated merchants."),
                            variants(
                                    rng -> "IP-location mismatch for " + acct(rng) + ": device IP geography conflicts "
                                            + "with the transaction's reported location.",
                                    rng -> "IP-location mismatch alert: " + acct(rng) + " has an IP region inconsistent "
                                            + "with the point of sale."),
                            variants(
                                    rng -> "Home-region deviation for " + acct(rng) + ": a charge originated far outside "
                                            + "the account's usual geographic region.",
                                    rng -> "Home-region anomaly for " + acct(rng) + ": current transaction geography "
                                            + "differs sharply from its established location pattern."),
                            variants(
                                    rng -> "Travel-speed anomaly for " + acct(rng) + ": "
                                            + rng.nextInt(700, 9000) + " km of apparent travel occurred in "
                                            + rng.nextInt(3, 35) + " minutes.",
                                    rng -> "Travel-speed check failed for " + acct(rng)
                                            + ": the distance between locations is impossible in the elapsed time."),
                            variants(
                                    rng -> "Same-card travel anomaly for " + acct(rng) + ": the same card was used in "
                                            + "two distant cities within " + rng.nextInt(2, 30) + " minutes.",
                                    rng -> "Same-card travel conflict: " + acct(rng)
                                            + " shows geographically incompatible purchases in one session.")));

    /*
     * Hard negatives model benign activity for each rule. They retain enough rule vocabulary
     * to be plausible candidates but explicitly describe normal behavior.
     */
    private static final Map<String, List<Function<Random, String>>> DISTRACTOR_TEMPLATES = Map.of(
            "velocity", List.of(
                    rng -> "Normal transaction rate for " + acct(rng) + ": "
                            + rng.nextInt(1, 5) + " purchases spread across a full hour, within expected limits.",
                    rng -> "Routine payment activity for " + acct(rng)
                            + " remains below the transaction velocity threshold.",
                    rng -> "Account " + acct(rng) + " shows ordinary transaction frequency with no unusual payment burst."),
            "zscore", List.of(
                    rng -> "Normal spending for " + acct(rng) + ": amount " + amt(rng)
                            + " is within one standard deviation of the rolling mean.",
                    rng -> "Routine amount baseline for " + acct(rng) + " is stable; recent purchases match the usual range.",
                    rng -> "Account " + acct(rng) + " shows expected transaction amounts with no material z-score deviation."),
            "geo", List.of(
                    rng -> "Normal location history for " + acct(rng) + ": charges "
                            + rng.nextInt(10, 300) + " km apart occurred over several hours.",
                    rng -> "Routine geographic activity for " + acct(rng)
                            + " matches the account's established travel pattern.",
                    rng -> "Account " + acct(rng) + " has consistent device and transaction locations with no geo anomaly."));

    /**
     * Generates 300 chunks: 95 relevant rows across seven semantic clusters and five
     * benign hard negatives per rule.
     */
    List<CorpusChunk> generate() {
        var rng = new Random(SEED);
        var chunks = new ArrayList<CorpusChunk>();
        int id = 0;

        for (var rule : RULES) {
            var clustersForRule = CLUSTER_TEMPLATES.get(rule);

            int perCluster = 95 / clustersForRule.size();
            int remainder = 95 % clustersForRule.size();

            for (int c = 0; c < clustersForRule.size(); c++) {
                String clusterId = rule + "-c" + c;
                int clusterSize = perCluster + (c < remainder ? 1 : 0);

                for (int m = 0; m < clusterSize; m++) {
                    chunks.add(new CorpusChunk(
                            "chunk-" + id++,
                            rule,
                            clusterId,
                            renderCluster(rule, c, rng),
                            2));
                }
            }

            for (int i = 0; i < 5; i++) {
                chunks.add(new CorpusChunk(
                        "distractor-" + id++,
                        rule,
                        rule + "-distractor",
                        renderDistractor(rule, rng),
                        0));
            }
        }

        return chunks;
    }

    private String renderCluster(String rule, int clusterIndex, Random rng) {
        var variants = CLUSTER_TEMPLATES.get(rule).get(clusterIndex);
        return variants.get(rng.nextInt(variants.size())).apply(rng);
    }

    private String renderDistractor(String rule, Random rng) {
        var templates = DISTRACTOR_TEMPLATES.get(rule);
        return templates.get(rng.nextInt(templates.size())).apply(rng);
    }

    private static List<Function<Random, String>> variants(
            Function<Random, String> first,
            Function<Random, String> second) {
        return List.of(first, second);
    }

    private static String acct(Random rng) {
        return "acct-" + rng.nextInt(10000, 99999);
    }

    private static String amt(Random rng) {
        return String.format(Locale.ROOT, "%.2f", rng.nextDouble(50, 5000));
    }

    private static String sigma(Random rng) {
        return String.format(Locale.ROOT, "%.1f", rng.nextDouble(2, 6));
    }
}
