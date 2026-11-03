package io.rpe.triage.rageval;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.ollama.OllamaEmbeddingModel;
import org.springframework.ai.ollama.api.OllamaApi;
import org.springframework.ai.ollama.api.OllamaEmbeddingOptions;
import org.springframework.http.client.ReactorClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PostgreSQL + pgvector integration test for the offline RAG benchmark.
 *
 * <p>Uses the repository's PostgreSQL Docker build and a separate evaluation
 * table. The 1,024-dimensional vectors match the evaluation-only Ollama model,
 * not the production OpenAI embedding model.</p>
 *
 * <p>Compares TopK with multiple MMR lambda values. Each query's candidate
 * pool is retrieved once and shared across selectors, isolating the impact
 * of the selection strategy.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class RagEvalPgVectorIntegrationTest {

    // Build context for the same PostgreSQL/pgvector image used by the project.
    private static final Path POSTGRES_BUILD_CONTEXT =
            Path.of("../deploy/postgres").toAbsolutePath().normalize();

    // Evaluation-only embedding model; its output has 1,024 dimensions.
    private static final String OLLAMA_MODEL = "mxbai-embed-large";

    /*
     * MMR lambda controls the relevance/diversity trade-off:
     * 0.5 = equal weighting; higher values favor query relevance;
     * 1.0 = similarity-only ranking, equivalent to the TopK baseline.
     */
    private static final List<Double> MMR_LAMBDAS =
            List.of(0.5, 0.6, 0.7, 0.8, 0.9, 1.0);

    // Initial experimental cutoff, to be assessed against the observed cosine distribution.
    private static final double DEDUP_COSINE_THRESHOLD = 0.90;

    // Proposed soft-gate thresholds; report-only until agreed.
    private static final double GATE_MIN_PRECISION = 0.80;
    private static final double GATE_MAX_GRADE_ZERO = 0.20;
    private static final double GATE_MIN_SUBTOPIC_RECALL = 0.50;

    /**
     * Isolated evaluation database, built from the project's PostgreSQL Dockerfile.
     * JIT is disabled to match the project's canonical database startup.
     */
    @Container
    static final GenericContainer<?> POSTGRES = new GenericContainer<>(
            new ImageFromDockerfile("rpe-postgres:16.3-pgvector", false)
                    .withFileFromPath(".", POSTGRES_BUILD_CONTEXT))
            .withEnv("POSTGRES_DB", "rpe")
            .withEnv("POSTGRES_USER", "rpe")
            .withEnv("POSTGRES_PASSWORD", "rpe_test")
            .withExposedPorts(5432)
            .withCommand("postgres", "-c", "jit=off")
            .waitingFor(Wait.forListeningPort());

    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        // Use Testcontainers' dynamically mapped port, not a fixed localhost port.
        String jdbcUrl = "jdbc:postgresql://"
                + POSTGRES.getHost()
                + ":"
                + POSTGRES.getMappedPort(5432)
                + "/rpe";

        var dataSource = new DriverManagerDataSource(
                jdbcUrl,
                "rpe",
                "rpe_test");

        jdbc = new JdbcTemplate(dataSource);

        // Enable pgvector's vector type and cosine-distance operations.
        jdbc.execute("CREATE EXTENSION IF NOT EXISTS vector");

        // Evaluation-only schema; do not write to the production RAG table.
        jdbc.execute("""
                CREATE TABLE rag_eval_corpus (
                    id TEXT PRIMARY KEY,
                    rule_name TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    content TEXT NOT NULL,
                    relevance_grade INT NOT NULL,
                    embedding vector(1024) NOT NULL)
                """);

        // Match the benchmark's cosine-distance retrieval strategy.
        jdbc.execute("""
                CREATE INDEX ON rag_eval_corpus
                USING hnsw (embedding vector_cosine_ops)
                """);
    }

    @Test
    void runHarness() throws Exception {
        // Allow local or remote Ollama configuration without editing the test.
        String baseUrl = System.getProperty(
                "ollama.base-url",
                System.getenv().getOrDefault(
                        "OLLAMA_BASE_URL",
                        "http://localhost:11434"));

        /*
         * This is an opt-in integration benchmark, not a regular build gate.
         * Skip it when Ollama is unreachable because no embedding measurements
         * can be produced. A skipped test is not a successful benchmark run.
         */
        Assumptions.assumeTrue(
                ollamaReachable(baseUrl),
                "Ollama not reachable at " + baseUrl + " — skipping eval harness");

        /*
         * Increase the test-scoped HTTP read timeout for slow model startup
         * or CPU-bound inference. This does not change production HTTP settings.
         */
        var requestFactory = new ReactorClientHttpRequestFactory();
        requestFactory.setReadTimeout(Duration.ofMinutes(2));

        var ollamaApi = OllamaApi.builder()
                .baseUrl(baseUrl)
                .restClientBuilder(
                        RestClient.builder().requestFactory(requestFactory))
                .build();

        var embeddingModel = OllamaEmbeddingModel.builder()
                .ollamaApi(ollamaApi)
                .options(OllamaEmbeddingOptions.builder()
                        .model(OLLAMA_MODEL)
                        .build())
                .build();

        /*
         * Reuse vectors between benchmark runs using the local JSON cache.
         * The cache key includes the model identifier and text, so different
         * configured model identifiers do not intentionally share embeddings.
         */
        var embeddings = new CachedEmbeddingClient(
                embeddingModel,
                OLLAMA_MODEL,
                Path.of("target/rageval-embedding-cache.json"));

        var corpus = new SyntheticCorpusGenerator().generate();

        try {
            /*
             * Embed and insert the synthetic corpus into pgvector.
             * Expected data: 300 chunks — 100 per rule, including five
             * grade-zero distractors for each rule.
             *
             * Repeated text/model combinations use CachedEmbeddingClient's cache.
             */
            insertCorpus(corpus, embeddings);

            /*
             * Register selectors in the order we want to compare them.
             * LinkedHashMap preserves output order: TopK first, then each MMR lambda.
             */
            var selectors = new LinkedHashMap<String, EvidenceSelector>();

            // Similarity-only baseline.
            selectors.put("TopK", new TopKSelector());

            // Query-similarity-first baseline with near-duplicate suppression.
            selectors.put(
                    "TopK+dedup(0.90)",
                    new TopKDedupSelector(
                            embeddings,
                            DEDUP_COSINE_THRESHOLD));

            for (double delta : new double[] {0.02, 0.05, 0.10}) {
                selectors.put("Floor(" + delta + ")",
                        new RelativeFloorSelector(delta, new TopKSelector()));
                selectors.put("Floor(" + delta + ")+dedup(0.90)",
                        new RelativeFloorSelector(delta,
                                new TopKDedupSelector(embeddings, DEDUP_COSINE_THRESHOLD)));
            }

            // Sweep lambda to see how relevance/diversity changes with the weighting.
            for (double lambda : MMR_LAMBDAS) {
                selectors.put(
                        "MMR(" + lambda + ")",
                        new MMRSelector(embeddings, lambda));
            }

            System.out.println(
                    "=== Scoped pools + unscoped parity: "
                            + "TopK vs TopK+dedup vs MMR ===");

            var harness = new RagEvalHarness();
            var retriever = new OfflineCandidateRetriever(jdbc);

            var first = harness.run(corpus, embeddings, retriever, selectors);

            assertInvariants(first);
            reportGates(first);

            // Second pass over the same data must match exactly.
            var second = harness.run(corpus, embeddings, retriever, selectors);

            assertEquals(first, second,
                    "scorecards differ between two in-JVM passes");

        } finally {
            /*
             * Persist the embedding cache even if insertion or evaluation fails,
             * allowing the next run to reuse work already completed.
             */
            embeddings.flush();
        }
    }

    /**
     * Embeds each synthetic chunk and stores its text, evaluation labels and vector.
     *
     * <p>Relevance grades and cluster IDs are persisted as evaluation ground truth.
     * They must not be used to filter candidates before the selector runs.</p>
     */
    private void insertCorpus(
            List<CorpusChunk> corpus,
            CachedEmbeddingClient embeddings) {

        for (var chunk : corpus) {
            jdbc.update(
                    """
                    INSERT INTO rag_eval_corpus (
                        id,
                        rule_name,
                        cluster_id,
                        content,
                        relevance_grade,
                        embedding)
                    VALUES (?, ?, ?, ?, ?, ?::vector)
                    """,
                    chunk.id(),
                    chunk.ruleName(),
                    chunk.clusterId(),
                    chunk.content(),
                    chunk.relevanceGrade(),
                    toVectorLiteral(embeddings.embed(chunk.content())));
        }
    }

    /**
     * Converts a Java float array into pgvector's bracketed vector literal format.
     */
    private static String toVectorLiteral(float[] vector) {
        var sb = new StringBuilder("[");

        for (int i = 0; i < vector.length; i++) {
            sb.append(i > 0 ? "," : "")
                    .append(vector[i]);
        }

        return sb.append("]").toString();
    }

    /**
     * Checks whether the Ollama HTTP endpoint is reachable.
     *
     * <p>This does not verify that the configured embedding model is installed;
     * a missing model will fail when the first embedding is requested.</p>
     */
    private boolean ollamaReachable(String baseUrl) {
        try {
            var conn = (HttpURLConnection) URI.create(
                            baseUrl + "/api/tags")
                    .toURL()
                    .openConnection();

            conn.setConnectTimeout(500);
            conn.setReadTimeout(500);

            return conn.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    // Hard invariants: any violation is a selector or harness bug.
    private static void assertInvariants(
            List<RagEvalHarness.Scorecard> cards) {

        var idsByCell = new HashMap<String, Map<String, List<String>>>();

        for (var c : cards) {
            var selectedIds = c.selected().stream()
                    .map(CorpusChunk::id)
                    .toList();
            var poolIds = c.candidatePool().stream()
                    .map(CorpusChunk::id)
                    .collect(Collectors.toSet());
            String where = c.pool() + " K=" + c.candidateK()
                    + " " + c.rule() + " " + c.selector();

            assertTrue(selectedIds.size() <= RagEvalHarness.FINAL_K,
                    "over finalK: " + where);
            assertEquals(selectedIds.size(), new HashSet<>(selectedIds).size(),
                    "duplicate chunk selected: " + where);
            assertTrue(poolIds.containsAll(selectedIds),
                    "selected chunk not in pool: " + where);

            idsByCell
                    .computeIfAbsent(
                            c.pool() + "|" + c.candidateK() + "|" + c.rule(),
                            k -> new HashMap<>())
                    .put(c.selector(), selectedIds);
        }

        // MMR(1.0) is similarity-only and must reproduce TopK exactly.
        idsByCell.forEach((cell, bySelector) -> assertEquals(
                bySelector.get("TopK"),
                bySelector.get("MMR(1.0)"),
                "MMR(1.0) differs from TopK at " + cell));
    }

    // Soft gates: printed per row and per selector, never fail the build.
    private static void reportGates(List<RagEvalHarness.Scorecard> cards) {
        var passBySelector = new LinkedHashMap<String, Integer>();
        var totalBySelector = new LinkedHashMap<String, Integer>();

        for (var c : cards) {
            boolean relevance = c.precision() >= GATE_MIN_PRECISION;
            boolean exposure = c.gradeZeroRate() <= GATE_MAX_GRADE_ZERO;
            boolean diversity =
                    c.subtopicRecall() >= GATE_MIN_SUBTOPIC_RECALL;

            System.out.printf(
                    Locale.ROOT,
                    "gate pool=%-8s K=%d query=%-9s selector=%-26s "
                            + "relevance=%s exposure=%s diversity=%s%n",
                    c.pool(),
                    c.candidateK(),
                    c.rule(),
                    c.selector(),
                    relevance ? "PASS" : "FAIL",
                    exposure ? "PASS" : "FAIL",
                    diversity ? "PASS" : "FAIL");

            totalBySelector.merge(c.selector(), 1, Integer::sum);
            if (relevance && exposure && diversity) {
                passBySelector.merge(c.selector(), 1, Integer::sum);
            }
        }

        totalBySelector.forEach((selector, total) -> System.out.printf(
                Locale.ROOT,
                "gateSummary selector=%-26s allPass=%d/%d%n",
                selector,
                passBySelector.getOrDefault(selector, 0),
                total));
    }
}
