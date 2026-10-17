package io.rpe.triage.rageval;

import java.util.Comparator;
import java.util.List;

/**
 * Selects the final evidence chunks from the retrieved candidate pool.
 *
 * <p>Implementations receive only retrieval results; evaluation ground truth stays inside
 * {@link CorpusChunk} and is not used by the selector.
 */
interface EvidenceSelector {

    /**
     * Selects at most {@code finalK} chunks from the retrieved candidates.
     */
    List<CorpusChunk> select(List<ScoredChunk> candidates, int finalK);
}

/**
 * Candidate chunk with its cosine similarity to the query.
 */
record ScoredChunk(
        CorpusChunk chunk,
        double similarity) {}

/**
 * Similarity-only baseline: returns the highest-scoring candidates.
 *
 * <p>Example: similarities {@code [0.91, 0.87, 0.84, 0.79]} with {@code finalK=2}
 * returns the chunks scoring {@code 0.91} and {@code 0.87}.
 */
final class TopKSelector implements EvidenceSelector {

    @Override
    public List<CorpusChunk> select(
            List<ScoredChunk> candidates,
            int finalK) {

        // Rank candidates by query similarity, highest score first, then keep the top K.
        return candidates.stream()
                .sorted(Comparator.comparingDouble(ScoredChunk::similarity).reversed())
                .limit(finalK)
                .map(ScoredChunk::chunk)
                .toList();
    }
}
