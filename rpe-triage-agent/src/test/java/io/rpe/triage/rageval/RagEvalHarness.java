package io.rpe.triage.rageval;

import java.util.List;

/**
 * Report-only RAG evaluation harness.
 *
 * <p>This run produces baseline retrieval/selection metrics; it does not fail the build.
 * Regression thresholds are deliberately deferred until the baseline numbers are known.
 */
final class RagEvalHarness {

    /*
     * These are the same fixed-field query words used by the evaluation design.
     *
     * Example:
     *   "velocity" -> embedding -> pgvector candidate retrieval
     *   "zscore"  -> embedding -> pgvector candidate retrieval
     *   "geo"     -> embedding -> pgvector candidate retrieval
     *
     * Using the literal rule name lets us measure whether the embedding space naturally
     * separates the three rule families before introducing more sophisticated query construction.
     */
    private static final List<String> QUERIES = List.of(
            "velocity",
            "zscore",
            "geo");

    /*
     * Number of nearest corpus chunks retrieved before evidence selection.
     *
     * 20 is currently an evaluation placeholder; it is not being presented as the final
     * production retrieval setting.
     */
    private static final int CANDIDATE_K = 20;

    /*
     * Number of chunks the selector is allowed to return as final evidence.
     *
     * Example:
     *   20 retrieved candidates -> TopKSelector -> 5 final evidence chunks.
     */
    private static final int FINAL_K = 5;

    /**
     * Runs the evaluation once for each detector rule and prints the baseline metrics.
     *
     * <p>The stages are intentionally separated:
     *
     * <pre>
     * query text
     *     -> embedding
     *     -> candidate retrieval
     *     -> retrieval purity
     *     -> evidence selection
     *     -> precision / recall / MRR / nDCG / redundancy
     * </pre>
     *
     * <p>This separation lets us tell whether a weakness originates in candidate retrieval
     * or in the subsequent evidence-selection step.
     */
    void run(
            List<CorpusChunk> corpus,
            CachedEmbeddingClient embeddings,
            OfflineCandidateRetriever retriever,
            EvidenceSelector selector) {

        for (String rule : QUERIES) {

            /*
             * The query is deliberately just the rule name.
             *
             * This matches the production fixed-field query shape used by the RAG evaluation:
             * the benchmark is testing how well that bare query partitions the synthetic corpus.
             */
            float[] queryVector = embeddings.embed(rule);

            /*
             * Retrieve the nearest CANDIDATE_K corpus rows using pgvector cosine similarity.
             *
             * Ground-truth relevance is not used here; retrieval sees only the query vector
             * and stored embeddings.
             */
            var candidates = retriever.retrieveCandidates(queryVector, CANDIDATE_K);

            /*
             * Purity is measured before selection.
             *
             * Example:
             *   20 candidates
             *   16 belong to the queried rule
             *   purity = 16 / 20 = 0.80
             *
             * A low value indicates the problem is already in candidate retrieval rather than
             * in TopKSelector/MMR-style evidence selection.
             */
            double purity = RetrievalMetrics.retrievalPurity(
                    candidates.stream()
                            .map(ScoredChunk::chunk)
                            .toList(),
                    rule);

            /*
             * Select the final evidence independently of the ground-truth labels.
             *
             * The current baseline is similarity-only Top-K.
             */
            var selected = selector.select(candidates, FINAL_K);

            /*
             * Ground truth is used only after retrieval/selection to calculate evaluation metrics.
             *
             * Positive relevance means relevanceGrade > 0. The generator currently creates
             * 91 relevant clustered chunks and 5 grade-0 distractors per rule.
             */
            long totalRelevant = corpus.stream()
                    .filter(c -> c.ruleName().equals(rule))
                    .filter(c -> c.relevanceGrade() > 0)
                    .count();

            /*
             * Print one report row per query.
             *
             * Retrieval metric:
             *   purity@20
             *
             * Selection metrics:
             *   P@5, R@5, MRR, nDCG@5, redundancy
             *
             * The output is intentionally report-only for this baseline run.
             */
            System.out.printf(
                    "query=%-9s purity@%d=%.2f  P@%d=%.2f  R@%d=%.2f  "
                            + "MRR=%.2f  nDCG@%d=%.2f  redundancy=%.2f%n",
                    rule,
                    CANDIDATE_K,
                    purity,
                    FINAL_K,
                    RetrievalMetrics.precisionAtK(selected, FINAL_K),
                    FINAL_K,
                    RetrievalMetrics.recallAtK(selected, totalRelevant, FINAL_K),
                    RetrievalMetrics.mrr(selected),
                    FINAL_K,
                    RetrievalMetrics.ndcgAtK(selected, FINAL_K),
                    RetrievalMetrics.redundancyRatio(selected));
        }
    }
}
