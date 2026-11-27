package io.rpe.alert.consumer;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * Pins the admin-client transport-security fold (ADR-20). Per-service copy
 * of the detection-service test (no shared jar — schema contract only).
 *
 * <p>Without the securityProps fold the observability {@code Admin} client connects credential-less
 * under SASL_SSL: every depth poll fails inside the {@code @BoundaryHandler} poller and every
 * {@code rpe.dlt.depth} gauge freezes at its boot value 0 — false-healthy, silently disarming
 * the ADR-23 RpeDlt* alerts. The Testcontainers suites run PLAINTEXT, where the fold is a no-op,
 * so this test is the only CI guard on the SASL path.
 */
@Testcontainers(disabledWithoutDocker = true)
class ProcessedAlertsConstraintTest {

    @Container
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16.3"))
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
