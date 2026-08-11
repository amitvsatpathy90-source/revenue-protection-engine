package io.rpe.alert.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rpe.alert.config.BoundaryHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * 30-day rolling purge of processed_alerts rows (Stage 6 — sole writer rule).
 *
 * alert-service is the sole writer of processed_alerts (ADR-17 §3.4), so it owns the
 * purge. The previous purge_old_records() function in the relay violated this by deleting
 * from a table it does not own. alert_role (this service's role) has DELETE on
 * processed_alerts; relay_role does not.
 *
 * <p><b>The horizon is a correctness contract, not housekeeping.</b> processed_alerts is the
 * exactly-once guard for at-least-once {@code payment.alerts} delivery (ADR-06/13): any path
 * that can present an old {@code alert_id} again — a DLT/parked re-drive (retention pinned at
 * 14d in the infra files), a relay-outage backlog drain, or an offsets-expiry replay
 * ({@code auto-offset-reset=earliest} x topic retention) — is only idempotent while the
 * original row still exists. Invariant: <b>purge horizon (30d) &gt; every redelivery/re-drive
 * horizon (14d DLT retention + operator latency)</b>. Shrinking this below DLT retention
 * re-opens silent double-acting on late re-drives (2026-07 arch-audit R4). The table holds
 * only alerts (~1% of events), so 30d of rows is cheap.
 *
 * Runs hourly with a 5-minute startup offset. Executes on a virtual thread — no JDBC
 * pinning risk as this is a simple DELETE with no PgJDBC synchronized path.
 */
@Component
@BoundaryHandler("scheduled-purge-must-survive any failure; logs and the next fixedDelay run retries")
class ProcessedAlertsPurge {

    private static final Logger log = LoggerFactory.getLogger(ProcessedAlertsPurge.class);

    private final DataSource dataSource;

    ProcessedAlertsPurge(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Scheduled(fixedDelay = 3_600_000, initialDelay = 300_000)
    public void purge() {
        // 30d, NOT 7d: must outlive the pinned 14d DLT/parked retention plus re-drive latency —
        // see the class javadoc contract before changing this interval.
        String sql = "DELETE FROM processed_alerts WHERE acted_at < now() - interval '30 days'";
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(sql)) {
            stmt.setQueryTimeout(60);
            int deleted = stmt.executeUpdate();
            if (deleted > 0) {
                log.info("processed_alerts purge completed: {} rows removed", deleted);
            }
        } catch (Exception e) {
            log.error("processed_alerts purge failed", e);
        }
    }
}
