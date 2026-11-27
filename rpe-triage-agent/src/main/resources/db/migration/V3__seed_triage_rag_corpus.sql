-- Hand-authored corpus rows (ADR-30) — behavioral descriptions only, no threshold
-- values (content policy, enforced by review). One row per active Detector
-- (velocity/10, zscore/20, geo/30 — detector precedence order).

-- Data-only migration, no schema change — runs once via Flyway's normal
-- checksum/version tracking, same as any other migration (V1/V2). Adding a new
-- detector later means a new V4 INSERT-only migration, never editing this file.
--
-- The INSERT below pre-populates placeholder embeddings. They are deterministic
-- pseudo-random vectors, not semantic embeddings; similarity search against them
-- returns arbitrary results.
--
-- v1 retrieval uses exact rule_name lookup (TriageService.fetchRagContext) and
-- does not generate query embeddings or read the stored vectors. Real embeddings
-- are needed only if a similarity-retrieval path is enabled (ADR-30).
--
-- To use real embeddings before V3 is first applied, run:
--   ./deploy/scripts/generate-rag-corpus-embeddings.sh > corpus-insert.sql
-- and replace the INSERT block below with the generated INSERT. Once V3 has been
-- applied to a persistent database, do not edit it; use a new Flyway migration.
--
-- PLACEHOLDER EMBEDDINGS — OpenAI key deliberately deferred (cost decision,
-- not a technical blocker). These vectors satisfy the NOT NULL constraint and
-- structural tests only; they do not validate similarity retrieval.
SELECT setseed(0.42);

INSERT INTO triage_rag_corpus (rule_name, content, embedding)
VALUES ('velocity',
        'Fires when the number of payment events for an account within a sliding time window exceeds a configured maximum. Counts prior events in the window before the current event is added, so the check is exclusive of the triggering event itself. A high-frequency burst of transactions on one account is the signal.',
        (SELECT array_agg(random()) FROM generate_series(1, 1536))::vector),
       ('zscore',
        'Fires when a payment amount deviates from an account''s historical spending pattern by more than a configured number of standard deviations, using an incrementally-updated running mean and variance (Welford''s algorithm). Requires a minimum number of prior observations before evaluating, to avoid flagging early transactions on a new or sparse account as anomalous.',
        (SELECT array_agg(random()) FROM generate_series(1, 1536))::vector),
       ('geo',
        'Fires when the implied travel speed between two consecutive payment locations for the same account exceeds a configured maximum, calculated from the great-circle distance between the two points and the elapsed time between them. Accounts with no prior location on record, or accounts explicitly classified as exempt from per-account geo ordering, are skipped rather than flagged.',
        (SELECT array_agg(random()) FROM generate_series(1, 1536))::vector);
