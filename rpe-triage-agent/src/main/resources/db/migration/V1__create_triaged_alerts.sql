-- Triage inbox + verdict store (ADR-15).
-- alert_id PK = the inbox dedup key. INSERT ... ON CONFLICT DO NOTHING runs BEFORE
-- the LLM call (ordering invariant, ai-triage-rules.md §2) — the deterministic
-- UUIDv5 alert_id (ADR-13) makes replay idempotency a one-line conflict clause.
--
-- triage_status lifecycle:
--   PENDING_TRIAGE        inbox row inserted; verdict not yet produced
--   LLM_TRIAGED           agent verdict produced + recorded
--   DEGRADED_RULE_BASED   fallback verdict (CB open / failure / stale sweep)
CREATE TABLE triaged_alerts (
    alert_id       UUID        PRIMARY KEY,
    account_id     TEXT        NOT NULL,
    rule_name      TEXT        NOT NULL,
    triage_status  TEXT        NOT NULL DEFAULT 'PENDING_TRIAGE',
    severity       TEXT,
    verdict        JSONB,
    prompt_version TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    triaged_at     TIMESTAMPTZ
);

-- Serves both the stale-PENDING sweep and the status-count query
CREATE INDEX idx_triaged_status_created ON triaged_alerts (triage_status, created_at);
