-- Durable inbox behind the webhook acknowledgement (ADR-33). Flyway creates the schema (spring.flyway.schemas).
CREATE TABLE ingest.inbox
(
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT        NOT NULL, -- x-razorpay-event-id: delivery dedup
    event_type   TEXT        NOT NULL, -- gateway event name, or 'unknown' for unusable bodies
    payment_id   TEXT,                 -- pay_...; business dedup for forwarded rows
    account_id   TEXT,
    status       TEXT        NOT NULL,
    reason       TEXT,                 -- stable code for REJECTED / STALE rows
    payload      JSONB,                -- outbound payment.events body; PENDING/PUBLISHED rows only
    received_at  TIMESTAMPTZ NOT NULL, -- becomes the Kafka record timestamp (ADR-33 item 8)
    published_at TIMESTAMPTZ,
    CONSTRAINT inbox_event_id_uk UNIQUE (event_id),
    CONSTRAINT inbox_status_chk CHECK (status IN ('PENDING', 'PUBLISHED', 'IGNORED', 'REJECTED', 'STALE')),
    CONSTRAINT inbox_forwarded_chk CHECK (status NOT IN ('PENDING', 'PUBLISHED')
        OR (payment_id IS NOT NULL AND payload IS NOT NULL))
);

-- One forwarded row per payment, whatever the delivery id (a second capture event cannot double-count)
CREATE UNIQUE INDEX inbox_forwarded_payment_uk ON ingest.inbox (payment_id)
    WHERE status IN ('PENDING', 'PUBLISHED');

-- Poller scan: oldest pending first, per-account order follows received_at
CREATE INDEX inbox_pending_idx ON ingest.inbox (received_at, id) WHERE status = 'PENDING';
