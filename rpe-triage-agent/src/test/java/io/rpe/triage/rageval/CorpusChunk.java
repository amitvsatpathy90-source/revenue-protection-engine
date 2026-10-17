package io.rpe.triage.rageval;

/**
 * Synthetic historical-alert chunk used only by the RAG evaluation harness.
 *
 * <p>{@code clusterId} and {@code relevanceGrade} are evaluation ground truth and are not
 * inputs to retrieval or selection.
 */
record CorpusChunk(
        String id,
        String ruleName,
        String clusterId,
        String content,
        int relevanceGrade) {}
