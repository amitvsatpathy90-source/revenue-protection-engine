package io.rpe.triage.inbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * JDBC inbox. Every call hops to the dedicated platform-thread pool INTERNALLY —
 * callers run on virtual threads, and PgJDBC {@code synchronized} pins VT carriers
 * (same JVM constraint as the core module). Putting the hop inside the repository
 * means no caller can forget it.
 */
@Repository
public class JdbcTriageInbox implements TriageInbox {

    private static final Logger log = LoggerFactory.getLogger(JdbcTriageInbox.class);

    private final DataSource dataSource;
    private final ExecutorService jdbcExecutor;

    public JdbcTriageInbox(DataSource dataSource,
                           @Qualifier("jdbcExecutor") ExecutorService jdbcExecutor) {
        this.dataSource   = dataSource;
        this.jdbcExecutor = jdbcExecutor;
    }

    @Override
    public boolean tryInsert(UUID alertId, String accountId, String ruleName) {
        return onJdbcPool(() -> {
            String sql = "INSERT INTO triaged_alerts(alert_id, account_id, rule_name) "
                       + "VALUES (?::uuid, ?, ?) ON CONFLICT DO NOTHING";
            try (Connection conn = dataSource.getConnection();
                 var ps = conn.prepareStatement(sql)) {
                ps.setQueryTimeout(10);
                ps.setString(1, alertId.toString());
                ps.setString(2, accountId);
                ps.setString(3, ruleName);
                return ps.executeUpdate() == 1;
            } catch (SQLException e) {
                throw new IllegalStateException("inbox insert failed", e);
            }
        });
    }

    // The status guard makes "row leaves PENDING_TRIAGE exactly once" a database
    // invariant, not a scheduling assumption: a consumer mark racing a sweep that
    // already claimed and degraded the row (possible only if the consumer stalls past
    // the stale window) matches zero rows instead of overwriting a terminal status.
    private static final String MARK_SQL =
            "UPDATE triaged_alerts SET triage_status = ?, severity = ?, "
          + "verdict = ?::jsonb, prompt_version = ?, triaged_at = ? "
          + "WHERE alert_id = ?::uuid AND triage_status = 'PENDING_TRIAGE'";

    @Override
    public void markTriaged(UUID alertId, String triageStatus, String severity,
                            String verdictJson, String promptVersion) {
        onJdbcPool(() -> {
            try (Connection conn = dataSource.getConnection()) {
                int updated = executeMark(conn, alertId, triageStatus, severity,
                        verdictJson, promptVersion);
                if (updated == 0) {
                    log.warn("Mark skipped alertId={} — row already left PENDING_TRIAGE "
                            + "(stale-pending sweep won the race); verdict not recorded",
                            alertId);
                }
                return null;
            } catch (SQLException e) {
                throw new IllegalStateException("inbox mark-triaged failed", e);
            }
        });
    }

    /**
     * Exclusive claim-and-sweep: {@code SELECT ... FOR UPDATE SKIP LOCKED} plus the
     * mark UPDATEs in ONE transaction, so concurrent sweeps on other instances skip
     * this instance's claimed rows instead of double-publishing them. Same pattern
     * as the relay's outbox drain.
     *
     * <p>{@code action} (the Kafka publish) runs on the jdbcExecutor thread while the
     * row locks are held — bounded and acceptable at sweep cadence (60s, batch ≤ 50,
     * publish confirm ≤ 10s), never the hot path. Manual commit/rollback instead of
     * {@code @Transactional}: every statement must share this one connection, and the
     * repository already hops pools internally.
     *
     * <p>Failure semantics: {@code action} returning {@code null} skips the mark —
     * the row stays PENDING_TRIAGE and the claim releases at commit. A SQLException
     * rolls back the whole batch; rows whose verdicts were already published are
     * re-swept next cycle — a duplicate on the advisory topic, which is at-least-once
     * by contract (consumers dedupe on the deterministic alert_id).
     */
    @Override
    public int sweepStalePending(Duration staleAfter, int limit, SweepAction action) {
        return onJdbcPool(() -> {
            String claimSql = "SELECT alert_id, account_id, rule_name FROM triaged_alerts "
                            + "WHERE triage_status = 'PENDING_TRIAGE' AND created_at < ? "
                            + "ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED";
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    List<PendingRow> claimed = new ArrayList<>();
                    try (var ps = conn.prepareStatement(claimSql)) {
                        ps.setQueryTimeout(10);
                        ps.setTimestamp(1, Timestamp.from(Instant.now().minus(staleAfter)));
                        ps.setInt(2, limit);
                        try (var rs = ps.executeQuery()) {
                            while (rs.next()) {
                                claimed.add(new PendingRow(
                                        UUID.fromString(rs.getString("alert_id")),
                                        rs.getString("account_id"),
                                        rs.getString("rule_name")));
                            }
                        }
                    }
                    int swept = 0;
                    for (PendingRow row : claimed) {
                        Mark mark = action.process(row);   // publish happens in here
                        if (mark == null) continue;        // row stays PENDING_TRIAGE
                        // Claimed rows are locked and PENDING_TRIAGE, so the status
                        // guard always matches here — counting the update keeps that
                        // assumption honest instead of asserting it.
                        swept += executeMark(conn, row.alertId(), mark.triageStatus(),
                                mark.severity(), mark.verdictJson(), mark.promptVersion());
                    }
                    conn.commit();
                    return swept;
                } catch (SQLException | RuntimeException e) {
                    conn.rollback();
                    throw e instanceof RuntimeException re
                            ? re
                            : new IllegalStateException("stale-pending sweep failed", e);
                }
            } catch (SQLException e) {
                throw new IllegalStateException("stale-pending sweep failed", e);
            }
        });
    }

    /** @return rows updated: 1, or 0 if the row already left PENDING_TRIAGE. */
    private static int executeMark(Connection conn, UUID alertId, String triageStatus,
                                   String severity, String verdictJson, String promptVersion)
            throws SQLException {
        try (var ps = conn.prepareStatement(MARK_SQL)) {
            ps.setQueryTimeout(10);
            ps.setString(1, triageStatus);
            ps.setString(2, severity);
            ps.setString(3, verdictJson);
            ps.setString(4, promptVersion);
            ps.setTimestamp(5, Timestamp.from(Instant.now()));
            ps.setString(6, alertId.toString());
            return ps.executeUpdate();
        }
    }

    private <T> T onJdbcPool(Supplier<T> work) {
        try {
            return CompletableFuture.supplyAsync(work, jdbcExecutor).join();
        } catch (CompletionException e) {
            throw e.getCause() instanceof RuntimeException re
                    ? re
                    : new IllegalStateException(e.getCause());
        }
    }
}
