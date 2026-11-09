package io.rpe.triage.rag;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-30 testing bar #8: real pgvector, the project's Postgres image, real V1-V3 migrations.
 * Default schema (not triage) to avoid per-environment search_path setup.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcTriageVectorRepositoryIntegrationTest {

    // Same pgvector image the rageval harness builds
    private static final Path BUILD_CONTEXT =
            Path.of("../deploy/postgres").toAbsolutePath().normalize();

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> POSTGRES = new GenericContainer<>(
            new ImageFromDockerfile("rpe-postgres:16.3-pgvector", false)
                    .withFileFromPath(".", BUILD_CONTEXT))
            .withEnv("POSTGRES_DB", "rpe")
            .withEnv("POSTGRES_USER", "rpe")
            .withEnv("POSTGRES_PASSWORD", "rpe_test")
            .withExposedPorts(5432)
            .withCommand("postgres", "-c", "jit=off")
            .waitingFor(Wait.forListeningPort());

    private static JdbcTemplate jdbc;
    private static JdbcTriageVectorRepository repository;

    @BeforeAll
    static void migrateAndWire() {
        String url = "jdbc:postgresql://" + POSTGRES.getHost() + ":" + POSTGRES.getMappedPort(5432) + "/rpe";
        var ds = new DriverManagerDataSource(url, "rpe", "rpe_test");
        // Default schema avoids per-environment search_path setup
        Flyway.configure().dataSource(ds).locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(ds);
        repository = new JdbcTriageVectorRepository(jdbc, new SimpleMeterRegistry());
    }

    @Test
    void exactRuleNameReturnsThatDetectorsChunk() {

        // One seeded row per active detector
        for (String rule : List.of("velocity", "zscore", "geo")) {
            var rows = repository.findByRuleName(rule, 5);
            assertThat(rows).as(rule).hasSize(1);
            assertThat(rows.getFirst().ruleName()).isEqualTo(rule);
            assertThat(rows.getFirst().content()).isNotBlank();
            assertThat(rows.getFirst().similarity()).isEqualTo(1.0);
        }
    }

    @Test
    void unknownRuleReturnsEmpty() {
        assertThat(repository.findByRuleName("no_such_rule", 5)).isEmpty();
    }

    @Test
    void duplicateRuleNameIsDeterministicAndLimitApplies() {
        String firstContent = repository.findByRuleName("velocity", 1).getFirst().content();

        // Random vector: embedding is NOT NULL, content irrelevant
        jdbc.update("INSERT INTO triage_rag_corpus (rule_name, content, embedding) " +
                "VALUES ('velocity', 'duplicate row', " +
                "(SELECT array_agg(random()) FROM generate_series(1, 1536))::vector)");
        try {
            var rows = repository.findByRuleName("velocity", 1);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().content()).isEqualTo(firstContent);   // ORDER BY id: seeded row wins
        } finally {
            jdbc.update("DELETE FROM triage_rag_corpus WHERE content = 'duplicate row'");
        }
    }
}

