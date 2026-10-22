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
 */
final class TopKSelector implements EvidenceSelector {

    @Override
    public List<CorpusChunk> select(
            List<ScoredChunk> candidates,
            int finalK) {

        return candidates.stream()
                .sorted(Comparator.comparingDouble(ScoredChunk::similarity).reversed())
                .limit(finalK)
                .map(ScoredChunk::chunk)
                .toList();
    }
}

/**
 * Greedy query-similarity baseline with a near-duplicate cutoff.
 *
 * <p>Only query similarity and embedding cosine similarity influence selection.
 * Relevance grades and cluster labels are never read.</p>
 */
final class TopKDedupSelector implements EvidenceSelector {

    private final CachedEmbeddingClient embeddings;
    private final double similarityThreshold;

    TopKDedupSelector(
            CachedEmbeddingClient embeddings,
            double similarityThreshold) {

        if (!Double.isFinite(similarityThreshold)
                || similarityThreshold < -1.0
                || similarityThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "Cosine similarity threshold must be in [-1, 1]");
        }

        this.embeddings = embeddings;
        this.similarityThreshold = similarityThreshold;
    }

    @Override
    public List<CorpusChunk> select(
            List<ScoredChunk> candidates,
            int finalK) {

        if (finalK <= 0 || candidates.isEmpty()) {
            return List.of();
        }

        var ranked = candidates.stream()
                .sorted(Comparator.comparingDouble(
                        ScoredChunk::similarity).reversed())
                .toList();

        var selected = new ArrayList<ScoredChunk>();

        for (var candidate : ranked) {
            boolean nearDuplicate = selected.stream()
                    .anyMatch(existing ->
                            cosineSimilarity(
                                    candidate.chunk(),
                                    existing.chunk())
                                    >= similarityThreshold);

            if (!nearDuplicate) {
                selected.add(candidate);
            }

            if (selected.size() >= finalK) {
                break;
            }
        }

        // Returning fewer than finalK is intentional when the cutoff
        // excludes the remaining candidates.
        return selected.stream()
                .map(ScoredChunk::chunk)
                .toList();
    }

    private double cosineSimilarity(CorpusChunk a, CorpusChunk b) {
        float[] va = embeddings.embed(a.content());
        float[] vb = embeddings.embed(b.content());

        if (va.length != vb.length) {
            throw new IllegalArgumentException(
                    "Embedding dimensions do not match");
        }

        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < va.length; i++) {
            dot += va[i] * vb[i];
            normA += va[i] * va[i];
            normB += vb[i] * vb[i];
        }

        double denominator = Math.sqrt(normA) * Math.sqrt(normB);
        return denominator == 0.0 ? 0.0 : dot / denominator;
    }
}

/**
 * Maximal Marginal Relevance: greedily picks the candidate maximizing
 * {@code lambda * relevance - (1 - lambda) * maxSimilarityToAlreadySelected}.
 *
 * <p>{@code lambda=1.0} degenerates to {@link TopKSelector}; {@code lambda=0.0} ignores
 * query relevance entirely and only maximizes diversity.
 *
 * <p>Reuses {@link CachedEmbeddingClient} for pairwise chunk similarity. The cache is populated
 * during corpus insertion, so this adds no new Ollama calls after corpus embeddings are cached.
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

        while (!remaining.isEmpty() && selected.size() < finalK) {
            ScoredChunk best = null;
            double bestScore = Double.NEGATIVE_INFINITY;

            for (var candidate : remaining) {
                double maxSimToSelected = selected.stream()
                        .mapToDouble(s -> cosineSimilarity(candidate.chunk(), s.chunk()))
                        .max()
                        .orElse(0.0);

                double mmrScore = lambda * candidate.similarity()
                        - (1 - lambda) * maxSimToSelected;

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

        if (va.length != vb.length) {
            throw new IllegalArgumentException("Embedding dimensions do not match");
        }

        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < va.length; i++) {
            dot += va[i] * vb[i];
            normA += va[i] * va[i];
            normB += vb[i] * vb[i];
        }

        double denominator = Math.sqrt(normA) * Math.sqrt(normB);
        return denominator == 0.0 ? 0.0 : dot / denominator;
    }
}
