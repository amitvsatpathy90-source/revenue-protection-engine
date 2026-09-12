package io.rpe.triage.rag;

import java.util.List;

/**
 * Raw pgvector similarity search — the deliberate bypass of Spring AI's
 * {@code VectorStore} (ADR-30 §2 Decision). Exists as its own interface,
 * separate from {@code RagRetrievalClient}, so the R4j decorator chain
 * (client) and the SQL (repository) stay independently testable —
 * same separation TriageLlmClient/ChatModel already models for the LLM boundary.
 */
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

    /** One retrieved corpus row plus its similarity score for evidence-grounding in the verdict. */
    record RetrievedChunk(String ruleName, String content, double similarity) {}
}
