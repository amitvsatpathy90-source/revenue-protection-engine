package io.rpe.triage.rageval;

import java.util.*;

/**
 * Evaluation-only metrics for the synthetic RAG benchmark.
 *
 * <p>These metrics describe retrieval and selection quality; they do not affect production
 * triage behavior.
 */
final class RetrievalMetrics {

    static double precisionAtK(List<CorpusChunk> selected, int k) {
        return selected.stream()
                .limit(k)
                .filter(c -> c.relevanceGrade() > 0)
                .count() / (double) k;
    }

    static double recallAtK(
            List<CorpusChunk> selected,
            long totalRelevantInCorpus,
            int k) {

        if (totalRelevantInCorpus == 0) return 0.0;

        var hit = selected.stream()
                .limit(k)
                .filter(c -> c.relevanceGrade() > 0)
                .count();

        return hit / (double) totalRelevantInCorpus;
    }

    static double mrr(List<CorpusChunk> ranked) {
        for (int i = 0; i < ranked.size(); i++) {
            if (ranked.get(i).relevanceGrade() > 0) {
                return 1.0 / (i + 1);
            }
        }
        return 0.0;
    }

    static double ndcgAtK(List<CorpusChunk> ranked, int k) {
        double dcg = 0;
        double idcg = 0;

        var byGrade = ranked.stream()
                .sorted(Comparator.comparingInt(CorpusChunk::relevanceGrade).reversed())
                .toList();

        for (int i = 0; i < Math.min(k, ranked.size()); i++) {
            dcg += (Math.pow(2, ranked.get(i).relevanceGrade()) - 1)
                    / (Math.log(i + 2) / Math.log(2));
        }

        for (int i = 0; i < Math.min(k, byGrade.size()); i++) {
            idcg += (Math.pow(2, byGrade.get(i).relevanceGrade()) - 1)
                    / (Math.log(i + 2) / Math.log(2));
        }

        return idcg == 0 ? 0.0 : dcg / idcg;
    }

    /**
     * Mean cosine similarity across all unordered pairs in the selected set.
     *
     * <p>This is an embedding-level diversity measure independent of the synthetic cluster
     * labels. Lower values mean the selected embeddings are, on average, less similar.
     */
    static double meanPairwiseCosineSimilarity(
            List<CorpusChunk> selected,
            CachedEmbeddingClient embeddings) {

        if (selected.size() < 2) return 0.0;

        double total = 0.0;
        int pairs = 0;

        for (int i = 0; i < selected.size() - 1; i++) {
            float[] left = embeddings.embed(selected.get(i).content());

            for (int j = i + 1; j < selected.size(); j++) {
                float[] right = embeddings.embed(selected.get(j).content());
                total += cosineSimilarity(left, right);
                pairs++;
            }
        }

        return pairs == 0 ? 0.0 : total / pairs;
    }

    private static double cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Embedding dimensions do not match");
        }

        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }

        double denominator = Math.sqrt(normA) * Math.sqrt(normB);
        return denominator == 0.0 ? 0.0 : dot / denominator;
    }

    /**
     * Repetition ratio: duplicate semantic cluster selections divided by selected-set size - 1.
     *
     * <p>Cluster IDs are generated from distinct content themes by SyntheticCorpusGenerator.
     */
    static double redundancyRatio(List<CorpusChunk> selected) {
        if (selected.size() < 2) return 0.0;

        var seen = new HashSet<String>();
        long duplicates = selected.stream()
                .filter(c -> !seen.add(c.clusterId()))
                .count();

        return duplicates / (double) (selected.size() - 1);
    }

    static double retrievalPurity(
            List<CorpusChunk> candidates,
            String queryRuleName) {

        if (candidates.isEmpty()) return 0.0;

        return candidates.stream()
                .filter(c -> c.ruleName().equals(queryRuleName))
                .count() / (double) candidates.size();
    }
}
