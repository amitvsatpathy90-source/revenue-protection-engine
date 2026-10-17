package io.rpe.triage.rageval;

import java.util.List;

/**
 * Report-only RAG evaluation harness.
 *
 * <p>This run produces retrieval and selection metrics; it does not fail the build based on
 * quality thresholds. Regression thresholds are deferred until the baseline is trustworthy.
 */
final class RagEvalHarness {

    private static final List<String> QUERIES = List.of("velocity", "zscore", "geo");
    private static final int CANDIDATE_K = 20;
    private static final int FINAL_K = 5;

    void run(
            List<CorpusChunk> corpus,
            CachedEmbeddingClient embeddings,
            OfflineCandidateRetriever retriever,
            EvidenceSelector selector) {

        for (String rule : QUERIES) {
            float[] queryVector = embeddings.embed(rule);
            var candidates = retriever.retrieveCandidates(queryVector, CANDIDATE_K);

            double purity = RetrievalMetrics.retrievalPurity(
                    candidates.stream().map(ScoredChunk::chunk).toList(),
                    rule);

            var selected = selector.select(candidates, FINAL_K);

            long totalRelevant = corpus.stream()
                    .filter(c -> c.ruleName().equals(rule))
                    .filter(c -> c.relevanceGrade() > 0)
                    .count();

            double precision = RetrievalMetrics.precisionAtK(selected, FINAL_K);
            double recall = RetrievalMetrics.recallAtK(selected, totalRelevant, FINAL_K);
            double mrr = RetrievalMetrics.mrr(selected);
            double ndcg = RetrievalMetrics.ndcgAtK(selected, FINAL_K);
            double clusterRedundancy = RetrievalMetrics.redundancyRatio(selected);
            double pairwiseCosine = RetrievalMetrics.meanPairwiseCosineSimilarity(selected, embeddings);
            List<String> selectedClusters = selected.stream().map(CorpusChunk::clusterId).toList();

            System.out.printf(
                    "query=%-9s purity@%d=%.2f  P@%d=%.2f  R@%d=%.2f  "
                            + "MRR=%.2f  nDCG@%d=%.2f  clusterRedundancy=%.2f  "
                            + "meanPairwiseCos=%.3f  selectedClusters=%s%n",
                    rule,
                    CANDIDATE_K,
                    purity,
                    FINAL_K,
                    precision,
                    FINAL_K,
                    recall,
                    mrr,
                    FINAL_K,
                    ndcg,
                    clusterRedundancy,
                    pairwiseCosine,
                    selectedClusters);
        }
    }
}
