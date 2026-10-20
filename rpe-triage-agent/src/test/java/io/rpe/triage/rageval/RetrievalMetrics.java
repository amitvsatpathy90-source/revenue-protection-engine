package io.rpe.triage.rageval;

import java.util.*;

/**
 * Evaluation-only metrics for the synthetic RAG benchmark.
 *
 * <p>These metrics describe retrieval and selection quality; they do not affect production
 * triage behavior.
 */
final class RetrievalMetrics {

    static double precisionAtK(
            List<CorpusChunk> selected,
            int k,
            String queryRuleName) {

        return selected.stream()
                .limit(k)
                .filter(c -> relevanceGradeForQuery(c, queryRuleName) > 0)
                .count() / (double) k;
    }

    static double recallAtK(
            List<CorpusChunk> selected,
            long totalRelevantInCorpus,
            int k,
            String queryRuleName) {

        if (totalRelevantInCorpus == 0) return 0.0;

        long hits = selected.stream()
                .limit(k)
                .filter(c -> relevanceGradeForQuery(c, queryRuleName) > 0)
                .count();

        return hits / (double) totalRelevantInCorpus;
    }

    static double mrr(
            List<CorpusChunk> ranked,
            String queryRuleName) {

        for (int i = 0; i < ranked.size(); i++) {
            if (relevanceGradeForQuery(ranked.get(i), queryRuleName) > 0) {
                return 1.0 / (i + 1);
            }
        }

        return 0.0;
    }

    static double ndcgAtK(
            List<CorpusChunk> ranked,
            List<CorpusChunk> candidatePool,
            String queryRuleName,
            int k) {

        double dcg = dcgAtK(ranked, queryRuleName, k);

        var idealRanking = candidatePool.stream()
                .sorted(Comparator.comparingInt(
                                (CorpusChunk c) ->
                                        relevanceGradeForQuery(c, queryRuleName))
                        .reversed())
                .toList();

        double idcg = dcgAtK(idealRanking, queryRuleName, k);

        return idcg == 0.0 ? 0.0 : dcg / idcg;
    }

    private static double dcgAtK(
            List<CorpusChunk> ranked,
            String queryRuleName,
            int k) {

        double dcg = 0.0;

        for (int i = 0; i < Math.min(k, ranked.size()); i++) {
            int grade = relevanceGradeForQuery(ranked.get(i), queryRuleName);

            dcg += (Math.pow(2, grade) - 1)
                    / (Math.log(i + 2) / Math.log(2));
        }

        return dcg;
    }

    private static int relevanceGradeForQuery(
            CorpusChunk chunk,
            String queryRuleName) {

        return chunk.ruleName().equals(queryRuleName)
                ? chunk.relevanceGrade()
                : 0;
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
