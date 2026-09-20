package io.rpe.outbox;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * Pins the outbox id PRIMARY KEY against schema drift.
 *
 * Raw inserts deliberately bypass ON CONFLICT so a weakened constraint cannot be masked
 * by application-level duplicate handling. Only V1 is migrated; later migrations are
 * unrelated to this persistence invariant.
 */
@Testcontainers(disabledWithoutDocker = true)
public class OutboxConstraintTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.3"))
                    .withDatabaseName("rpe")
                    .withUsername("rpe")
                    .withPassword("rpe_test");

    @Test
    void duplicateOutboxId_rejectedByPostgresConstraint() {
        Flyway.configure()
                .dataSource(
                        POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .target("1")
                .load()
                .migrate();

        JdbcTemplate jdbcTemplate = new JdbcTemplate(
                new DriverManagerDataSource(
                        POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword()));

        UUID outboxId = UUID.randomUUID();

        // Raw insert, no ON CONFLICT — tests the constraint, not app-level dedup handling.
        jdbcTemplate.update("""
                INSERT INTO outbox (id, account_id, payload)
                VALUES (?, ?, ?::jsonb)
                """, outboxId, "constraint-test", "{}");

        // Second raw insert must fail at the DB layer if the PK/unique constraint holds.
        assertThatThrownBy(() ->
                jdbcTemplate.update("""
                        INSERT INTO outbox (id, account_id, payload)
                        VALUES (?, ?, ?::jsonb)
                        """, outboxId, "constraint-test", "{}"))
                .isInstanceOf(DataIntegrityViolationException.class);

    }
}
