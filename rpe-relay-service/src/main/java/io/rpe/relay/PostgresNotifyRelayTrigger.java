package io.rpe.relay;

import jakarta.annotation.PostConstruct;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import io.rpe.relay.config.BoundaryHandler;
import org.springframework.stereotype.Component;

import java.sql.DriverManager;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Postgres LISTEN/NOTIFY relay trigger with adaptive fallback polling (ADR-05).
 *
 * A dedicated platform thread (NOT virtual — PgJDBC synchronized pins VT carriers)
 * holds one LISTEN connection. The {@code outbox_insert_notify} trigger (V4 migration)
 * fires {@code pg_notify('outbox_ready', '1')} once per batch insert ({@code FOR EACH STATEMENT}).
 *
 * On notification: {@code semaphore.release()} wakes the relay loop immediately.
 * On 5s getNotifications() timeout: relay does its fallback adaptive poll (semaphore not released).
 * On connection loss: {@code semaphore.release()} in reconnect path so relay never stalls.
 *
 * {@code lastNotificationAt} detects "connected but trigger not firing" — distinct from
 * connection loss; visible in /actuator/health via {@link RelayListenerHealthIndicator}.
 */
@Component
@BoundaryHandler("listen-reconnect-loop-must-survive any JDBC/driver failure (ADR-05); releases the relay semaphore on reconnect")
public class PostgresNotifyRelayTrigger {

    private static final Logger log = LoggerFactory.getLogger(PostgresNotifyRelayTrigger.class);

    private final DataSourceProperties dsProps;
    private final Semaphore    semaphore           = new Semaphore(0);
    private final AtomicBoolean listenerHealthy    = new AtomicBoolean(false);
    private volatile Instant    lastNotificationAt = null;

    public PostgresNotifyRelayTrigger(DataSourceProperties dsProps) {
        this.dsProps = dsProps;
    }

    @PostConstruct
    public void startListenThread() {
        // Platform thread — PgJDBC synchronized internals pin VT carriers (immutable constraint)
        Thread.ofPlatform()
                .daemon(true)
                .name("rpe-pg-listen")
                .start(this::listenLoop);
    }

    /**
     * Blocks until a LISTEN notification (or reconnect nudge) arrives, or the timeout elapses.
     * The signal is an optimisation, never a correctness control: the relay always re-queries
     * the database on wake and never trusts signal content — a missed, delayed, or duplicate
     * signal changes latency, not correctness (ADR-05).
     *
     * @return {@code true} if signalled before timeout; {@code false} on timeout
     */
    public boolean awaitSignal(long timeoutMs) throws InterruptedException {
        return semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public boolean isListenerHealthy()    { return listenerHealthy.get(); }
    public Instant getLastNotificationAt() { return lastNotificationAt; }

    private void listenLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                connectAndListen();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("LISTEN connection failed ({}); reconnecting in 5s", e.getMessage());
                listenerHealthy.set(false);
                // Release so relay never stalls during the reconnect window (ADR-05)
                semaphore.release();
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void connectAndListen() throws Exception {
        try (var conn = DriverManager.getConnection(
                dsProps.determineUrl(),
                dsProps.determineUsername(),
                dsProps.determinePassword())) {

            var pgConn = conn.unwrap(PGConnection.class);
            try (var stmt = conn.createStatement()) {
                stmt.execute("LISTEN outbox_ready");
            }
            listenerHealthy.set(true);
            // JDBC URLs can embed credentials — never log the URL (security rule)
            log.info("LISTEN connection established on channel outbox_ready");

            while (!Thread.currentThread().isInterrupted()) {
                // Blocks up to 5s — provides adaptive polling fallback if trigger is silent
                var notifications = pgConn.getNotifications(5_000);
                if (notifications != null && notifications.length > 0) {
                    lastNotificationAt = Instant.now();
                    semaphore.release();
                }
            }
        }
    }
}
