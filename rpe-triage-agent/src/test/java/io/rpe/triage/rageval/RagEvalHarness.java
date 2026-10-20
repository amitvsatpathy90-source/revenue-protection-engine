package io.rpe.triage.rageval;

import java.util.List;
import java.util.Map;

/**
 * Report-only harness for evaluating RAG retrieval and evidence selection.
 *
 * <p>Retrieval is performed once per query. Each selector receives the same
 * candidate pool, so differences in the final metrics reflect the selection
 * strategy rather than differences in retrieval.</p>
 *
 * <p>Relevance grades and cluster IDs are evaluation ground truth. They are
 * used to score results only; they must never be used to filter candidates
 * before selection.</p>
 *
 * <p>The harness reports metrics without enforcing quality thresholds.
 * Regression gates should be added once the evaluation baseline is trusted.</p>
 */
final class RagEvalHarness {

    // Representative queries corresponding to RPE's three detection rules.
    private static final List<String> QUERIES =
            List.of("velocity", "zscore", "geo");

    // Retrieve this many candidates before comparing selection strategies.
    private static final int CANDIDATE_K = 20;

    // Evaluate the final evidence set of at most five chunks.
    private static final int FINAL_K = 5;

    void run(
            List<CorpusChunk> corpus,
            CachedEmbeddingClient embeddings,
            OfflineCandidateRetriever retriever,
            Map<String, EvidenceSelector> selectors) {

        for (String rule : QUERIES) {

            // Embed the rule query and retrieve candidates exactly once.
            // All selectors below will reuse this same scored candidate pool.
            float[] queryVector = embeddings.embed(rule);

            // Test rule-scoped retrieval while retaining all grade-zero distractors.
            var candidates = retriever.retrieveCandidatesForRule(
                    queryVector, rule, CANDIDATE_K);

            // Extract chunks for metrics that need to inspect the full candidate pool.
            // Keep grade-0 distractors and cross-rule results: the benchmark must measure
            // how the retrieval and selection strategies handle them.
            List<CorpusChunk> candidateChunks = candidates.stream()
                    .map(ScoredChunk::chunk)
                    .toList();

            // Retrieval purity: fraction of candidates whose rule matches the query.
            // This measures rule matching, not the relevance of every individual chunk.
            double purity =
                    RetrievalMetrics.retrievalPurity(candidateChunks, rule);

            // Candidate grade-zero rate: fraction labelled irrelevant by the
            // synthetic evaluation data. Report it; do not use it to filter candidates.
            double gradeZeroRate = candidateChunks.isEmpty()
                    ? 0.0
                    : candidateChunks.stream()
                    .filter(c -> c.relevanceGrade() == 0)
                    .count() / (double) candidateChunks.size();

            // Recall denominator: all positive-grade chunks for this rule in the corpus.
            // These labels are used only for evaluation, never by a selector.
            long totalRelevant = corpus.stream()
                    .filter(c -> c.ruleName().equals(rule))
                    .filter(c -> c.relevanceGrade() > 0)
                    .count();

            // Compare every selector on the same candidates for this query.
            for (var entry : selectors.entrySet()) {

                String selectorName = entry.getKey();
                EvidenceSelector selector = entry.getValue();

                // Select up to FINAL_K chunks. The selector receives no ground-truth
                // filtering; it uses retrieval scores and, for MMR, embedding similarity.
                var selected = selector.select(candidates, FINAL_K);

                // Precision@K: fraction of the final K judged relevant to this query.
                double precision =
                        RetrievalMetrics.precisionAtK(selected, FINAL_K, rule);

                // Recall@K: fraction of all relevant corpus chunks recovered in the final K.
                double recall = RetrievalMetrics.recallAtK(
                        selected, totalRelevant, FINAL_K, rule);

                // MRR: reciprocal rank of the first query-relevant selected chunk.
                double mrr = RetrievalMetrics.mrr(selected, rule);

                // nDCG@K: ranking quality compared with an ideal ordering of the
                // retrieved candidate pool, using query-specific relevance grades.
                double ndcg = RetrievalMetrics.ndcgAtK(
                        selected, candidateChunks, rule, FINAL_K);

                // Fraction of selected chunks labelled grade 0.
                // A rise means the selector is choosing more known benign distractors.
                double selectedGradeZeroRate = selected.isEmpty()
                        ? 0.0
                        : selected.stream()
                        .limit(FINAL_K)
                        .filter(c -> c.relevanceGrade() == 0)
                        .count() / (double) selected.size();

                // Cluster redundancy: repeated cluster selections relative to set size.
                // Lower values generally mean more cluster-level diversity.
                double clusterRedundancy =
                        RetrievalMetrics.redundancyRatio(selected);

                // Mean cosine similarity across selected chunks.
                // Lower values generally mean the selected embeddings are less similar.
                double pairwiseCosine =
                        RetrievalMetrics.meanPairwiseCosineSimilarity(
                                selected, embeddings);

                // Print cluster IDs so we can inspect which themes were selected.
                List<String> selectedClusters = selected.stream()
                        .map(CorpusChunk::clusterId)
                        .toList();

                // Report-only output: compare relevance and diversity together.
                System.out.printf(
                        "query=%-9s selector=%-12s "
                                + "purity@%d=%.2f grade0@%d=%.2f "
                                + "P@%d=%.2f R@%d=%.2f MRR=%.2f "
                                + "nDCG@%d=%.2f selectedGrade0@%d=%.2f "
                                + "clusterRedundancy=%.2f meanPairwiseCos=%.3f "
                                + "selectedClusters=%s%n",
                        rule,
                        selectorName,
                        CANDIDATE_K,
                        purity,
                        CANDIDATE_K,
                        gradeZeroRate,
                        FINAL_K,
                        precision,
                        FINAL_K,
                        recall,
                        mrr,
                        FINAL_K,
                        ndcg,
                        FINAL_K,
                        selectedGradeZeroRate,
                        clusterRedundancy,
                        pairwiseCosine,
                        selectedClusters);
            }
        }
    }
}
