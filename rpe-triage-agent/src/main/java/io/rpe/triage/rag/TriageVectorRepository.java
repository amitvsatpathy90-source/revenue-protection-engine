package io.rpe.triage.rag;

import java.util.List;

/**
 * Corpus access for RAG retrieval. v1 uses {@link #findByRuleName} (exact key, ADR-30 D3);
 * {@link #similaritySearch} is the raw pgvector query kept for the v2 similarity corpus.
 * */
public interface TriageVectorRepository {

    /**
     * @param queryEmbedding the alert's embedded fixed-field query text (never raw
     *                       alert free-text — enforced by the caller, ADR-30)
     * @param topK           max rows to return
     * @param similarityThreshold cosine-similarity floor (0.0-1.0); rows below
     *                            this are excluded, not just deprioritized
     * @return chunks ordered by similarity, descending; empty if nothing clears the threshold
     */
    List<RetrievedChunk> similaritySearch(float[] queryEmbedding, int topK, double similarityThreshold);

    /**
     * Exact rule_name lookup (ADR-30 D3); similarity fixed at 1.0.
     *
     * @param limit max rows, ordered by id
     */
    List<RetrievedChunk> findByRuleName(String ruleName, int limit);

    /**
     * One retrieved corpus row plus its similarity score for evidence-grounding in the verdict.
     */
    record RetrievedChunk(String ruleName, String content, double similarity) {}
}
