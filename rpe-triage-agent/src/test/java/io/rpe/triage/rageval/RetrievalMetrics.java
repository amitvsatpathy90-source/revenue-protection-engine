package io.rpe.triage.rageval;

import java.util.*;

/**
 * Evaluation-only metrics for the synthetic RAG benchmark.
 *
 * <p>These metrics describe retrieval and selection quality; they do not affect production
 * triage behavior.
 */
final class RetrievalMetrics {

    /**
     * Precision@K: relevant results in the topK divided by K.
     *
     * <p>Example: 4 relevant results in top 5 → 4 / 5 = 0.80.
     */
    static double precisionAtK(List<CorpusChunk> selected, int k) {
        return selected.stream()
                .limit(k)
                .filter(c -> c.relevanceGrade() > 0)
                .count() / (double) k;
    }

    /**
     * Recall@K: relevant results found in the topK divided by all relevant results in the corpus.
     *
     * <p>Example: 4 relevant results found, 20 relevant results available → 4 / 20 = 0.20.
     */
    static double recallAtK(
            List<CorpusChunk> selected,
            long totalRelevantInCorpus,
            int k) {

        if (totalRelevantInCorpus == 0) return 0.0;

        // Number of relevant chunks recovered in the first K results.
        var hit = selected.stream()
                .limit(k)
                .filter(c -> c.relevanceGrade() > 0)
                .count();

        return hit / (double) totalRelevantInCorpus;
    }

    /**
     * MRR = Mean Reciprocal Rank.
     *
     * <p>reciprocal of the 1-based rank of the first relevant result.
     *
     * <p>Example: first relevant result at rank 1 → 1 / 1 = 1.0;
     * rank 2 → 1 / 2 = 0.50; rank 3 → 1 / 3 ≈ 0.33.
     */
    static double mrr(List<CorpusChunk> ranked) {
        for (int i = 0; i < ranked.size(); i++) {
            if (ranked.get(i).relevanceGrade() > 0) {
                return 1.0 / (i + 1);
            }
        }
        return 0.0;
    }

    /**
     * nDCG@K = Normalized Discounted Cumulative Gain.
     *
     * <p>Measures graded ranking quality: higher relevance grades contribute more, especially
     * when ranked earlier.
     *
     * <p>Example with K=3:
     * actual grades = [2, 3, 0]
     * ideal grades  = [3, 2, 0]
     *
     * <p>For grade 3, {@code 2^3 - 1 = 7}; for grade 2, {@code 2^2 - 1 = 3}.
     * The rank discount is {@code log2(rank + 1)}, so rank 1 has no discount,
     * rank 2 is discounted by {@code log2(3)}, and rank 3 by {@code log2(4)}.
     */
    static double ndcgAtK(List<CorpusChunk> ranked, int k) {
        double dcg = 0;
        double idcg = 0;

        /*
         * Put the same results into their ideal relevance order:
         * [2, 3, 0] → [3, 2, 0]
         */
        var byGrade = ranked.stream()
                // Highest relevance grade first gives the ideal ordering.
                .sorted(Comparator.comparingInt(CorpusChunk::relevanceGrade).reversed())
                .toList();

        /*
         * DCG scores the actual ranking.
         *
         * Example for [2, 3, 0]:
         * rank 1, grade 2 → (2^2 - 1) / log2(2) = 3 / 1
         * rank 2, grade 3 → (2^3 - 1) / log2(3) = 7 / log2(3)
         * rank 3, grade 0 → (2^0 - 1) / log2(4) = 0
         */
        for (int i = 0; i < Math.min(k, ranked.size()); i++) {

            // i is zero-based, so i + 1 is the result rank; i + 2 forms log2(rank + 1).
            dcg += (Math.pow(2, ranked.get(i).relevanceGrade()) - 1)
                    / (Math.log(i + 2) / Math.log(2));
        }

        /*
         * IDCG scores the ideal ordering [3, 2, 0].
         *
         * Example:
         * rank 1, grade 3 → 7 / 1
         * rank 2, grade 2 → 3 / log2(3)
         * rank 3, grade 0 → 0
         */
        for (int i = 0; i < Math.min(k, byGrade.size()); i++) {

            // Same calculation as DCG, but using the ideal relevance ordering.
            idcg += (Math.pow(2, byGrade.get(i).relevanceGrade()) - 1)
                    / (Math.log(i + 2) / Math.log(2));
        }

        // Normalize the actual score against the ideal score.
        return idcg == 0 ? 0.0 : dcg / idcg;
    }

    /**
     * Redundancy ratio: repeated evidence clusters divided by the selected set size minus one.
     *
     * <p>Example: 5 selections from 5 clusters → 0.0; one repeated cluster → 1 / 4 = 0.25.
     */
    static double redundancyRatio(List<CorpusChunk> selected) {
        if (selected.size() < 2) return 0.0;

        var seen = new HashSet<String>();

        // A chunk is a duplicate when its cluster was already selected.
        long dupes = selected.stream()
                .filter(c -> !seen.add(c.clusterId()))
                .count();

        return dupes / (double) (selected.size() - 1);
    }

    /**
     * Measures whether the bare rule-name query partitions the retrieved candidates by rule
     * before evidence selection runs.
     *
     * <p>Example: 24 velocity chunks in 30 candidates → purity = 24 / 30 = 0.80.
     */
    static double retrievalPurity(
            List<CorpusChunk> candidates,
            String queryRuleName) {

        if (candidates.isEmpty()) return 0.0;

        // Count candidates belonging to the queried rule family.
        return candidates.stream()
                .filter(c -> c.ruleName().equals(queryRuleName))
                .count() / (double) candidates.size();
    }
}
