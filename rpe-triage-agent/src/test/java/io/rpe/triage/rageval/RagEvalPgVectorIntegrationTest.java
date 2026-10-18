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
import java.util.List;

/**
 * PostgreSQL + pgvector integration harness for the offline RAG evaluation.
 *
 * <p>The container is built from the same {@code deploy/postgres/Dockerfile} used by the
 * deployment stack so the pgvector/PostgreSQL image build stays aligned with the RPE runtime.
 *
 * <p>The evaluation schema is separate from the production schema. The
 * {@code vector(1024)} dimension matches the evaluation embedding model used by the benchmark,
 * not the production OpenAI embedding dimension.
 */
@Testcontainers(disabledWithoutDocker = true)
class RagEvalPgVectorIntegrationTest {

    /*
     * Build from the same deploy/postgres directory used by the production Docker image.
     *
     * The directory contains the Dockerfile that pins PostgreSQL/pgvector versions.
     */
    private static final Path POSTGRES_BUILD_CONTEXT =
            Path.of("../deploy/postgres").toAbsolutePath().normalize();

    /** Embedding model used only by the evaluation harness. */
    private static final String OLLAMA_MODEL = "mxbai-embed-large";

    /** Standard MMR default; 0.5 weighs relevance and diversity equally. */
    private static final double MMR_LAMBDA = 0.5;

    /**
     * Same PostgreSQL + pgvector image build as the deployment stack.
     *
     * <p>JIT is disabled here because that is also how the canonical deployment starts
     * PostgreSQL.
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
        /*
         * Testcontainers maps container port 5432 to a host port dynamically.
         * Always use the mapped host/port rather than assuming localhost:5432.
         */
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

        // pgvector provides PostgreSQL's `vector` type and cosine-distance operators.
        jdbc.execute("CREATE EXTENSION IF NOT EXISTS vector");

        /*
         * Evaluation-only table.
         *
         * 1024 dimensions match mxbai-embed-large.
         * This is intentionally not the production RAG vector dimension.
         */
        jdbc.execute("""
                CREATE TABLE rag_eval_corpus (
                    id TEXT PRIMARY KEY,
                    rule_name TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    content TEXT NOT NULL,
                    relevance_grade INT NOT NULL,
                    embedding vector(1024) NOT NULL)
                """);

        /*
         * Match the evaluation retrieval strategy:
         * cosine distance over an HNSW pgvector index.
         */
        jdbc.execute("""
                CREATE INDEX ON rag_eval_corpus
                USING hnsw (embedding vector_cosine_ops)
                """);
    }

    @Test
    void runHarness() throws Exception {
        String baseUrl = System.getProperty(
                "ollama.base-url",
                System.getenv().getOrDefault(
                        "OLLAMA_BASE_URL",
                        "http://localhost:11434"));

        /*
         * This is an optional evaluation test, not a normal build gate.
         *
         * No Ollama means there is no RAG measurement to produce, so skip the test instead
         * of failing the suite.
         */
        Assumptions.assumeTrue(
                ollamaReachable(baseUrl),
                "Ollama not reachable at " + baseUrl + " — skipping eval harness");

        /*
         * Construct the evaluation embedding client directly.
         *
         * The endpoint comes from configuration so the same test can target local Ollama or
         * a remote Ollama endpoint without changing source code.
         */
        /*
         * Spring Framework's Reactor-backed request factory defaults to a 10-second read
         * timeout. CPU-only Ollama cold-start/inference can exceed that (the local logs
         * recorded a successful embedding request taking over 12 seconds).
         *
         * This override is test-scoped and gives the evaluation endpoint up to two minutes
         * to return one embedding; it does not alter production HTTP clients.
         */
        var requestFactory = new ReactorClientHttpRequestFactory();
        requestFactory.setReadTimeout(Duration.ofMinutes(2));

        var ollamaApi = OllamaApi.builder()
                .baseUrl(baseUrl)
                .restClientBuilder(RestClient.builder().requestFactory(requestFactory))
                .build();

        var embeddingModel = OllamaEmbeddingModel.builder()
                .ollamaApi(ollamaApi)
                .options(OllamaEmbeddingOptions.builder()
                        .model(OLLAMA_MODEL)
                        .build())
                .build();

        /*
         * Cache embeddings under target/ so repeated benchmark runs do not repeatedly call
         * Ollama for the same corpus/query text.
         *
         * The cache key also includes the model identity inside CachedEmbeddingClient, preventing
         * vectors from different embedding models being mixed.
         */
        var embeddings = new CachedEmbeddingClient(
                embeddingModel,
                OLLAMA_MODEL,
                Path.of("target/rageval-embedding-cache.json"));

        var corpus = new SyntheticCorpusGenerator().generate();

        try {
            /*
             * Generate and persist one embedding for every synthetic corpus chunk.
             *
             * The corpus is 300 chunks:
             *   100 per rule (95 clustered + 5 distractor) × 3 rules = 300.
             *
             * CachedEmbeddingClient means only uncached chunks result in Ollama calls.
             * Keep insertion inside this try so partial cache progress is flushed if an
             * embedding request fails partway through the corpus.
             */
            insertCorpus(corpus, embeddings);

            /*
             * Run both selectors against the same corpus/embeddings/candidates so the
             * MMR trade-off can be read directly against the TopK baseline:
             *
             *   literal rule name -> embedding -> pgvector candidate retrieval
             *       -> retrieval purity -> selection -> evaluation metrics
             */
            System.out.println("=== TopK baseline ===");
            new RagEvalHarness().run(
                    corpus,
                    embeddings,
                    new OfflineCandidateRetriever(jdbc),
                    new TopKSelector());

            System.out.println("=== MMR (lambda=" + MMR_LAMBDA + ") ===");
            new RagEvalHarness().run(
                    corpus,
                    embeddings,
                    new OfflineCandidateRetriever(jdbc),
                    new MMRSelector(embeddings, MMR_LAMBDA));
        } finally {
            /*
             * Persist newly generated embeddings even when the harness itself fails.
             *
             * Without this finally block, a failed evaluation run would discard the cache and
             * force the next run to regenerate every uncached embedding.
             */
            embeddings.flush();
        }
    }

    /**
     * Embeds every corpus chunk and inserts the resulting vector into pgvector.
     *
     * <p>Evaluation ground-truth fields are stored alongside the text, but they are not used
     * to generate or retrieve the embeddings.
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
     * Converts a Java embedding array to PostgreSQL's pgvector literal format.
     *
     * <p>Example: {@code [0.1f, 0.2f, 0.3f]} becomes {@code "[0.1,0.2,0.3]"}.
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
     * Checks whether the configured Ollama endpoint is available before starting the benchmark.
     *
     * <p>The lightweight {@code /api/tags} request verifies that the HTTP endpoint is reachable.
     * It does not verify that the requested embedding model has already been pulled; a missing
     * model will surface when the embedding is first requested.
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


}
