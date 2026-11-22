package io.rpe.ingest.inbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

/**
 * PostgreSQL inbox. One statement does both dedup checks: {@code ON CONFLICT DO NOTHING} (no target) covers the
 * unique event id and the partial unique payment index, so a redelivery and a second event for an already
 * forwarded payment both return "not inserted".
 */
@Repository
public class JdbcInboxRepository implements InboxRepository {

    private static final String INSERT = """
            INSERT INTO ingest.inbox
                (event_id, event_type, payment_id, account_id, status, reason, payload, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?)
            ON CONFLICT DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public JdbcInboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean insert(InboxRecord r) {
        int rows = jdbc.update(INSERT,
                r.eventId(), r.eventType(), r.paymentId(), r.accountId(),
                r.status().name(), r.reason(), r.payloadJson(), Timestamp.from(r.receivedAt()));
        return rows == 1;
    }
}
