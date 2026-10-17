package io.rpe.triage.rageval;

import java.util.ArrayList;
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
        double similarity) {
}

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

/**
 * Maximal Marginal Relevance: greedily picks the candidate maximizing
 * {@code lambda * relevance - (1 - lambda) * maxSimilarityToAlreadySelected}.
 *
 * <p>lambda=1.0 degenerates to {@link TopKSelector}; lambda=0.0 ignores query
 * relevance entirely and only maximizes diversity. Targets the redundancy
 * baseline measured by TopKSelector (0.50 on velocity/geo, 0.00 on zscore).
 *
 * <p>Reuses {@link CachedEmbeddingClient} for pairwise chunk similarity — every
 * candidate's embedding is already cached from corpus insertion, so this adds
 * no new Ollama calls.
 */
final class MMRSelector implements EvidenceSelector {

    private final CachedEmbeddingClient embeddings;
    private final double lambda;

    MMRSelector(CachedEmbeddingClient embeddings, double lambda) {
        this.embeddings = embeddings;
        this.lambda = lambda;
    }

    @Override
    public List<CorpusChunk> select(
            List<ScoredChunk> candidates,
            int finalK) {

        var remaining = new ArrayList<>(candidates);
        var selected = new ArrayList<ScoredChunk>();

        // Greedy: each round picks the candidate with the best relevance/diversity trade-off.
        while (!remaining.isEmpty() && selected.size() < finalK) {
            ScoredChunk best = null;
            double bestScore = Double.NEGATIVE_INFINITY;

            for (var candidate : remaining) {

                // Diversity penalty: how similar is this candidate to what's already picked.
                double maxSimToSelected = selected.stream()
                        .mapToDouble(s -> cosineSimilarity(candidate.chunk(), s.chunk()))
                        .max()
                        .orElse(0.0);

                double mmrScore = lambda * candidate.similarity() - (1 - lambda) * maxSimToSelected;

                if (mmrScore > bestScore) {
                    bestScore = mmrScore;
                    best = candidate;
                }
            }

            selected.add(best);
            remaining.remove(best);
        }

        return selected.stream().map(ScoredChunk::chunk).toList();
    }

    private double cosineSimilarity(CorpusChunk a, CorpusChunk b) {
        float[] va = embeddings.embed(a.content());
        float[] vb = embeddings.embed(b.content());

        double dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < va.length; i++) {
            dot += va[i] * vb[i];
            normA += va[i] * va[i];
            normB += vb[i] * vb[i];
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
