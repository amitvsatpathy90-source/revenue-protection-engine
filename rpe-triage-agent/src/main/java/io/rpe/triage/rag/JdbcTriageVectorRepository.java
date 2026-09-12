package io.rpe.triage.rag;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * pgvector cosine-similarity query via plain {@link JdbcTemplate} — no ORM, no
 * {@code VectorStore} (ADR-30).
 *
 * <p><b>Cosine distance vs. similarity:</b> pgvector's {@code <=>} operator returns
 * cosine DISTANCE (0 = identical, 2 = opposite), not similarity. Converted to
 * similarity (1 - distance) so callers/config work in 0-1-higher-is-better space.
 *
 * <p><b>VT-pinning:</b> PgJDBC's internal {@code synchronized} blocks pin the calling
 * virtual thread's carrier for the query duration — a known JDK 21 virtual-thread
 * limitation (synchronized pins the carrier until JEP 491 / JDK 24), applying to any
 * JDBC call in this module, not new here. Not treated as needing detection-core's
 * dedicated platform-pool discipline at alert-only volume — flag if real load
 * suggests otherwise.
 *
 * <p><b>UNVERIFIED:</b> not run against a live pgvector instance this session —
 * verify before merging.
 */
@Repository
public class JdbcTriageVectorRepository implements TriageVectorRepository {

    private final JdbcTemplate jdbcTemplate;
    private final AtomicLong lastIngestedAtEpochSeconds = new AtomicLong(0);

    public JdbcTriageVectorRepository(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        // Corpus is hand-seeded once via V3 Flyway migration, not a running ingestion
        // pipeline — cached at startup, not polled per-scrape (ADR-30).
        Long maxCreatedAt = jdbcTemplate.queryForObject(
                "SELECT EXTRACT(EPOCH FROM MAX(created_at))::bigint FROM triage_rag_corpus", Long.class);
        lastIngestedAtEpochSeconds.set(maxCreatedAt != null ? maxCreatedAt : 0);
        Gauge.builder("rag.corpus.last_ingested_at", lastIngestedAtEpochSeconds, AtomicLong::get)
                .description("Unix epoch seconds of the most recent triage_rag_corpus row; 0 = empty corpus")
                .register(meterRegistry);
    }

    @Override
    public List<RetrievedChunk> similaritySearch(float[] queryEmbedding, int topK, double similarityThreshold) {
        String vectorLiteral = toVectorLiteral(queryEmbedding);

        // Same literal repeated 3x (SELECT/WHERE/ORDER BY) — no named-param reuse in a
        // plain PreparedStatement. A CTE would compute distance once but adds a query-shape
        // layer for a ~4-row-scale hand-authored corpus. Revisit if corpus scale grows.
        String sql = """
                SELECT rule_name, content, 1 - (embedding <=> ?::vector) AS similarity
                FROM triage_rag_corpus
                WHERE 1 - (embedding <=> ?::vector) >= ?
                ORDER BY embedding <=> ?::vector
                LIMIT ?
                """;

        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new RetrievedChunk(
                        rs.getString("rule_name"),
                        rs.getString("content"),
                        rs.getDouble("similarity")),
                vectorLiteral, vectorLiteral, similarityThreshold, vectorLiteral, topK);
    }

    /**
     * Bracketed CSV cast to ::vector — avoids the separate pgvector-java driver-type
     * dependency purely to save one dependency. Costs a string build per call; revisit
     * if profiling ever shows this is a hot path (unlikely at ~1% alert-only volume).
     */
    private static String toVectorLiteral(float[] embedding) {
        return "[" + IntStream.range(0, embedding.length)
                .mapToObj(i -> Float.toString(embedding[i]))
                .collect(Collectors.joining(",")) + "]";
    }
}
