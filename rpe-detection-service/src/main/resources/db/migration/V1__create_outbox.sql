CREATE TABLE outbox (
    id         UUID        PRIMARY KEY,
    account_id TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'PENDING',
    attempts   INT         NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_status_created ON outbox (status, created_at);
