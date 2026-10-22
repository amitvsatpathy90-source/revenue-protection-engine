package io.rpe.triage.rageval;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Report-only harness for evaluating RAG retrieval and evidence selection.
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

    private static final int FINAL_K = 5;

    void run(
            List<CorpusChunk> corpus,
            CachedEmbeddingClient embeddings,
            OfflineCandidateRetriever retriever,
            Map<String, EvidenceSelector> selectors) {

        for (String rule : QUERIES) {
            // Reuse the same query vector for every candidate-pool size.
            float[] queryVector = embeddings.embed(rule);

            long ruleCorpusSize = corpus.stream()
                    .filter(c -> c.ruleName().equals(rule))
                    .count();

            for (int candidateK : CANDIDATE_KS) {
                var candidates = retriever.retrieveCandidatesForRule(
                        queryVector, rule, candidateK);

                int expectedSize = (int) Math.min(
                        candidateK, ruleCorpusSize);

                // Prevent an incomplete pool from being mistaken for a full K.
                if (candidates.size() != expectedSize) {
                    throw new IllegalStateException(
                            "Unexpected candidate count for rule=" + rule
                                    + ", K=" + candidateK
                                    + ": expected=" + expectedSize
                                    + ", actual=" + candidates.size());
                }

                reportCandidatePool(rule, candidateK, candidates);

                // All selectors below receive exactly this same pool.
                for (var entry : selectors.entrySet()) {
                    String selectorName = entry.getKey();
                    EvidenceSelector selector = entry.getValue();

                    var selected = selector.select(candidates, FINAL_K);

                    // Ground truth is inspected only after selection.
                    long positiveCount = selected.stream()
                            .filter(c -> c.ruleName().equals(rule))
                            .filter(c -> c.relevanceGrade() > 0)
                            .count();

                    long distinctPositiveClusters = selected.stream()
                            .filter(c -> c.ruleName().equals(rule))
                            .filter(c -> c.relevanceGrade() > 0)
                            .map(CorpusChunk::clusterId)
                            .distinct()
                            .count();

                    double selectedGradeZeroRate = selected.isEmpty()
                            ? 0.0
                            : selected.stream()
                            .filter(c -> c.relevanceGrade() == 0)
                            .count() / (double) selected.size();

                    double meanPairwiseCosine =
                            RetrievalMetrics.meanPairwiseCosineSimilarity(
                                    selected, embeddings);

                    List<String> selectedClusters = selected.stream()
                            .map(CorpusChunk::clusterId)
                            .toList();

                    System.out.printf(
                            Locale.ROOT,
                            "result K=%d query=%-9s selector=%-26s "
                                    + "selected=%d/%d positives@5=%d "
                                    + "distinctPositiveClusters@5=%d "
                                    + "selectedGrade0=%.2f "
                                    + "meanPairwiseCos=%.3f "
                                    + "selectedClusters=%s%n",
                            candidateK,
                            rule,
                            selectorName,
                            selected.size(),
                            FINAL_K,
                            positiveCount,
                            distinctPositiveClusters,
                            selectedGradeZeroRate,
                            meanPairwiseCosine,
                            selectedClusters);
                }
            }
        }
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
