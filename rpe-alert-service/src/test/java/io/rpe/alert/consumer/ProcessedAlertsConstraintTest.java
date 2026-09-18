package io.rpe.alert.consumer;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * Pins the processed_alerts alert_id PRIMARY KEY against schema drift.
 *
 * Raw inserts deliberately bypass ON CONFLICT so a weakened constraint cannot be masked
 * by application-level duplicate handling. The test exercises PostgreSQL directly,
 * independently of AlertConsumer/Kafka processing, retries, or DLT handling.
 * Flyway applies V1__create_processed_alerts.sql only, keeping the test scoped to
 * the migration that defines the constraint.
 */
@Testcontainers(disabledWithoutDocker = true)
class ProcessedAlertsConstraintTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.3"))
                    .withDatabaseName("rpe")
                    .withUsername("rpe")
                    .withPassword("rpe_test");

    @Test
    void duplicateAlertId_rejectedByPostgresConstraint() {
        Flyway.configure().
                dataSource(
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

        UUID alertId = UUID.randomUUID();
        Timestamp producedAt = Timestamp.from(Instant.now());

        jdbcTemplate.update("""
                INSERT INTO processed_alerts (alert_id, account_id, rule_name, produced_at)
                VALUES (?, ?, ?, ?)
                """, alertId, "constraint-test", "velocity", producedAt);

        assertThatThrownBy(() ->
                jdbcTemplate.update("""
                        INSERT INTO processed_alerts (alert_id, account_id, rule_name, produced_at)
                        VALUES (?, ?, ?, ?)
                        """, alertId, "constraint-test", "velocity", producedAt))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
