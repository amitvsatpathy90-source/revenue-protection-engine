-- Outbox contract DDL — TEST FIXTURE ONLY.
--
-- The relay owns NO tables (ADR-17 §3.4): in production the `outbox` table, its notify
-- trigger, and the purge function are created by the detection service's migrations, and
-- the relay runs with spring.flyway.enabled=false. This file provisions the slice of that
-- published contract the relay reads, so the relay's own integration test has a schema to
-- run against in its ephemeral Postgres. It mirrors the core V1/V2/V3/V4 migrations; if the
-- core outbox contract changes, this fixture must track it (microservices.md §2 — additive).

CREATE TABLE outbox (
    id          UUID        PRIMARY KEY,
    account_id  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'PENDING',
    attempts    INT         NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- ADR-25: W3C trace context the relay restores to continue the trace across the outbox gap.
    -- Mirrors detection's V5__outbox_trace_context.sql (additive, nullable).
    traceparent TEXT,
    tracestate  TEXT
);
CREATE INDEX idx_outbox_status_created ON outbox (status, created_at);

CREATE TABLE processed_alerts (
    alert_id    UUID PRIMARY KEY,
    account_id  TEXT NOT NULL,
    rule_name   TEXT NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    acted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stage 6: purge_old_records() removed from this fixture. The relay no longer calls a
-- cross-table purge function (relay_role has no DELETE on outbox or processed_alerts).
-- Purge is now owned per table: detection purges outbox; alert-service purges processed_alerts.

-- Cross-process relay wakeup: one notification per batch insert (FOR EACH STATEMENT).
CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('outbox_ready', '1');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_insert_notify
    AFTER INSERT ON outbox
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_outbox_insert();
