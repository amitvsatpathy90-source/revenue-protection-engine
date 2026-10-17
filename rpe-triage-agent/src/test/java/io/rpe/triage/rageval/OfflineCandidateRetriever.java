package io.rpe.triage.rageval;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * Retrieves the RAG evaluation candidate pool directly from the throwaway evaluation schema.
 *
 * <p>Production retrieval thresholds and {@code RagRetrievalClient} behavior are intentionally
 * excluded so candidate retrieval can be measured separately from evidence selection.
 */
final class OfflineCandidateRetriever {

    private final JdbcTemplate jdbc;

    OfflineCandidateRetriever(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Retrieves the highest-similarity candidates for the supplied query embedding.
     *
     * <p>Example: {@code candidateK=30} returns the 30 closest corpus embeddings by cosine
     * distance; no production similarity threshold is applied.
     */
    List<ScoredChunk> retrieveCandidates(float[] queryVector, int candidateK) {

        // pgvector accepts the parameter as a vector literal, e.g. [0.1,0.2,0.3].
        String v = toVectorLiteral(queryVector);

        return jdbc.query("""
                SELECT id, rule_name, cluster_id, content, relevance_grade,
                       1 - (embedding <=> ?::vector) AS similarity
                FROM rag_eval_corpus
                ORDER BY embedding <=> ?::vector
                LIMIT ?""",

                // Map each database row into the evaluation chunk plus its query similarity.
                (rs, n) -> new ScoredChunk(
                        new CorpusChunk(
                                rs.getString("id"),
                                rs.getString("rule_name"),
                                rs.getString("cluster_id"),
                                rs.getString("content"),
                                rs.getInt("relevance_grade")),
                        rs.getDouble("similarity")),

                // The same query vector is used for similarity calculation and ordering.
                v,
                v,
                candidateK);
    }

    /**
     * Converts an embedding array to the pgvector literal format.
     *
     * <p>Example: {@code [0.1f, 0.2f, 0.3f]} becomes {@code "[0.1,0.2,0.3]"}.
     */
    private String toVectorLiteral(float[] v) {
        var sb = new StringBuilder("[");

        // Append comma-separated vector components in their original order.
        for (int i = 0; i < v.length; i++) {
            sb.append(i > 0 ? "," : "").append(v[i]);
        }

        return sb.append("]").toString();
    }
}
