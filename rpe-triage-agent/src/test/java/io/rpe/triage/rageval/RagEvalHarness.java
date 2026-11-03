package io.rpe.triage.rageval;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Eval harness that returns scorecards for evaluating RAG retrieval and evidence selection.
 *
 * <p>Each selector receives the same candidate pool for a given query and K.
 * Evaluation labels are used for diagnostics and scoring only, never for
 * candidate filtering or selector decisions.</p>
 */
final class RagEvalHarness {

    private static final List<String> QUERIES =
            List.of("velocity", "zscore", "geo");

    // Expand the same rule-scoped pool to test candidate-pool starvation.
    private static final List<Integer> CANDIDATE_KS =
            List.of(20, 40, 60);

    // Unscoped pool size: no rule filter, cross-rule chunks compete.
    private static final List<Integer> PARITY_KS = List.of(20, 40);

    record Scorecard(
            String pool,
            int candidateK,
            String rule,
            String selector,
            double precision,
            double ndcg,
            double gradeZeroRate,
            double subtopicRecall,
            double redundancy,
            double meanPairwiseCos,
            List<CorpusChunk> selected,
            List<CorpusChunk> candidatePool) {}

    static final int FINAL_K = 5;

    List<Scorecard> run(
            List<CorpusChunk> corpus,
            CachedEmbeddingClient embeddings,
            OfflineCandidateRetriever retriever,
            Map<String, EvidenceSelector> selectors) {

        var scorecards = new ArrayList<Scorecard>();

        for (String rule : QUERIES) {
            float[] queryVector = embeddings.embed(rule);

            long ruleCorpusSize = corpus.stream()
                    .filter(c -> c.ruleName().equals(rule))
                    .count();

            for (int candidateK : CANDIDATE_KS) {
                var candidates = retriever.retrieveCandidatesForRule(
                        queryVector, rule, candidateK);

                int expectedSize = (int) Math.min(
                        candidateK, ruleCorpusSize);

                // Incomplete pool must not pass as a full K.
                if (candidates.size() != expectedSize) {
                    throw new IllegalStateException(
                            "Unexpected candidate count for rule=" + rule
                                    + ", K=" + candidateK
                                    + ": expected=" + expectedSize
                                    + ", actual=" + candidates.size());
                }

                reportCandidatePool(rule, candidateK, candidates);

                scorecards.addAll(evaluatePool(
                        "scoped", rule, candidateK, candidates,
                        embeddings, selectors));
            }

            // Prod-shaped parity: no rule filter.
            for (int parityK : PARITY_KS) {
                var unscoped = retriever.retrieveCandidates(
                        queryVector, parityK);

                int expectedUnscoped = Math.min(parityK, corpus.size());

                if (unscoped.size() != expectedUnscoped) {
                    throw new IllegalStateException(
                            "Unexpected unscoped candidate count for rule="
                                    + rule + ", K=" + parityK
                                    + ": expected=" + expectedUnscoped
                                    + ", actual=" + unscoped.size());
                }

                scorecards.addAll(evaluatePool(
                        "unscoped", rule, parityK, unscoped,
                        embeddings, selectors));
            }
        }

        return scorecards;
    }

    private static List<Scorecard> evaluatePool(
            String poolMode,
            String rule,
            int candidateK,
            List<ScoredChunk> candidates,
            CachedEmbeddingClient embeddings,
            Map<String, EvidenceSelector> selectors) {

        var pool = candidates.stream().map(ScoredChunk::chunk).toList();
        var rows = new ArrayList<Scorecard>();

        // All selectors receive exactly this same pool.
        for (var entry : selectors.entrySet()) {
            var selected = entry.getValue().select(candidates, FINAL_K);

            // Ground truth is inspected only after selection.
            var row = new Scorecard(
                    poolMode,
                    candidateK,
                    rule,
                    entry.getKey(),
                    RetrievalMetrics.precisionAtK(selected, FINAL_K, rule),
                    RetrievalMetrics.ndcgAtK(selected, pool, rule, FINAL_K),
                    RetrievalMetrics.gradeZeroRate(selected, rule),
                    RetrievalMetrics.subtopicRecall(
                            selected, pool, rule, FINAL_K),
                    RetrievalMetrics.redundancyRatio(selected),
                    RetrievalMetrics.meanPairwiseCosineSimilarity(
                            selected, embeddings),
                    selected,
                    pool);

            rows.add(row);

            System.out.printf(
                    Locale.ROOT,
                    "scorecard pool=%-8s K=%d query=%-9s selector=%-26s "
                            + "n=%d/%d P@5=%.2f nDCG@5=%.2f grade0=%.2f "
                            + "subtopicRecall=%.2f posClusters=%d redundancy=%.2f "
                            + "cos=%.3f clusters=%s%n",
                    poolMode,
                    candidateK,
                    rule,
                    entry.getKey(),
                    selected.size(),
                    FINAL_K,
                    row.precision(),
                    row.ndcg(),
                    row.gradeZeroRate(),
                    row.subtopicRecall(),
                    RetrievalMetrics.distinctPositiveClusters(row.selected(), rule),
                    row.redundancy(),
                    row.meanPairwiseCos(),
                    selected.stream().map(CorpusChunk::clusterId).toList());
        }

        return rows;
    }

    /**
     * Dumps the actual candidate ranking and summarizes score separation.
     * Relevance grades appear in diagnostic output only.
     */
    private static void reportCandidatePool(
            String rule,
            int candidateK,
            List<ScoredChunk> candidates) {

        System.out.printf(
                Locale.ROOT,
                "%n=== Candidate pool query=%s K=%d rows=%d ===%n",
                rule,
                candidateK,
                candidates.size());

        for (int i = 0; i < candidates.size(); i++) {
            ScoredChunk candidate = candidates.get(i);
            CorpusChunk chunk = candidate.chunk();

            System.out.printf(
                    Locale.ROOT,
                    "pool query=%-9s K=%d rank=%02d id=%s "
                            + "sim=%.6f cluster=%s grade=%d%n",
                    rule,
                    candidateK,
                    i + 1,
                    chunk.id(),
                    candidate.similarity(),
                    chunk.clusterId(),
                    chunk.relevanceGrade());
        }

        List<ScoredChunk> positives = candidates.stream()
                .filter(c -> c.chunk().ruleName().equals(rule))
                .filter(c -> c.chunk().relevanceGrade() > 0)
                .toList();

        List<ScoredChunk> gradeZero = candidates.stream()
                .filter(c -> c.chunk().ruleName().equals(rule))
                .filter(c -> c.chunk().relevanceGrade() == 0)
                .toList();

        double gradeZeroRate = candidates.isEmpty()
                ? 0.0
                : gradeZero.size() / (double) candidates.size();

        String gap = "n/a";

        if (!positives.isEmpty() && !gradeZero.isEmpty()) {
            double minPositive = positives.stream()
                    .mapToDouble(ScoredChunk::similarity)
                    .min()
                    .orElseThrow();

            double maxGradeZero = gradeZero.stream()
                    .mapToDouble(ScoredChunk::similarity)
                    .max()
                    .orElseThrow();

            gap = String.format(
                    Locale.ROOT,
                    "%.6f",
                    minPositive - maxGradeZero);
        }

        System.out.printf(
                Locale.ROOT,
                "poolSummary query=%-9s K=%d positive=%d grade0=%d "
                        + "positiveRange=%s grade0Range=%s "
                        + "minPositiveMinusMaxGrade0=%s grade0Rate=%.2f%n",
                rule,
                candidateK,
                positives.size(),
                gradeZero.size(),
                similarityRange(positives),
                similarityRange(gradeZero),
                gap,
                gradeZeroRate);
    }

    private static String similarityRange(List<ScoredChunk> candidates) {
        if (candidates.isEmpty()) {
            return "n/a";
        }

        double min = candidates.stream()
                .mapToDouble(ScoredChunk::similarity)
                .min()
                .orElseThrow();

        double max = candidates.stream()
                .mapToDouble(ScoredChunk::similarity)
                .max()
                .orElseThrow();

        return String.format(Locale.ROOT, "[%.6f,%.6f]", min, max);
    }
}
