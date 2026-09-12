package io.rpe.triage.config;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies retry classification for the RAG retrieval boundary (ADR-30).
 * Confirms PGJDBC 42.7.11 surfaces connection failures as PSQLException/08xxx,
 * and that deterministic SQL errors (syntax/permission) are never retried.
 */
class RagRetrievalRetryClassificationTest {

    @Test
    void retriesSqlTimeoutException() {
        // Generic JDBC timeout — always transient, always retryable.
        assertThat(RagResilienceConfig.isRetryableRetrievalException(
                new SQLTimeoutException("timeout"))).isTrue();
    }

    @Test
    void retriesSqlTransientException() {
        // JDBC-spec transient marker — driver-agnostic retry signal.
        assertThat(RagResilienceConfig.isRetryableRetrievalException(
                new SQLTransientException("transient"))).isTrue();
    }

    @Test
    void retriesPsqlConnectionFailure08006() {
        // Confirms the UNVERIFIED doc assumption: PGJDBC uses 08xxx for dropped connections.
        var ex = new PSQLException("connection failure", PSQLState.CONNECTION_FAILURE);
        assertThat(RagResilienceConfig.isRetryableRetrievalException(ex)).isTrue();
    }

    @Test
    void doesNotRetrySyntaxError42601() {
        // Deterministic bug class — retrying wastes a round trip and hides the error.
        var ex = new PSQLException("syntax error", PSQLState.SYNTAX_ERROR);
        assertThat(RagResilienceConfig.isRetryableRetrievalException(ex)).isFalse();
    }

    @Test
    void doesNotRetryUnrelatedException() {
        // Non-SQL exceptions must never fall through as retryable by default.
        assertThat(RagResilienceConfig.isRetryableRetrievalException(
                new RuntimeException("unrelated"))).isFalse();
    }

    @Test
    void retriesWrappedTransientPsqlConnectionFailure() {
        // JdbcTemplate's real shape: Spring wraps the driver exception, doesn't propagate it raw.
        var psqlEx = new PSQLException("connection failure", PSQLState.CONNECTION_FAILURE);
        var wrapped = new org.springframework.dao.DataAccessResourceFailureException("db unreachable", psqlEx);
        assertThat(RagResilienceConfig.isRetryableRetrievalException(wrapped)).isTrue();
    }

    @Test
    void doesNotRetryWrappedNonTransientSyntaxError() {
        // Non-transient DataAccessException wrapping a deterministic SQL bug — must not retry.
        var psqlEx = new PSQLException("syntax error", PSQLState.SYNTAX_ERROR);
        var wrapped = new org.springframework.dao.InvalidDataAccessResourceUsageException("bad query", psqlEx);
        assertThat(RagResilienceConfig.isRetryableRetrievalException(wrapped)).isFalse();
    }
}