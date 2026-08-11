CREATE TABLE processed_alerts (
    alert_id    UUID        PRIMARY KEY,
    account_id  TEXT        NOT NULL,
    rule_name   TEXT        NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    acted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
