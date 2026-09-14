-- Hand-authored corpus rows (ADR-30) — behavioral descriptions only, no threshold
-- values (content policy, enforced by review). One row per active Detector
-- (velocity/10, zscore/20, geo/30 — detector precedence order).

-- Data-only migration, no schema change — runs once via Flyway's normal
-- checksum/version tracking, same as any other migration (V1/V2). Adding a new
-- detector later means a new V4 INSERT-only migration, never editing this file.
--
-- Embeddings are NOT pre-populated here — embedding generation happens at
-- retrieval-query time only (TriageService.fetchRagContext), not at corpus
-- ingestion.

-- Note: pgvector similarity search (<=>) requires pre-computed embeddings on corpus rows.
-- The INSERT statement below is populated by running:
--   ./deploy/scripts/generate-rag-corpus-embeddings.sh > corpus-insert.sql
-- and pasting the generated SQL payload into this file prior to deployment.

-- PLACEHOLDER EMBEDDINGS — deterministic pseudo-random, NOT semantically meaningful.
-- OpenAI key deliberately deferred (cost decision, not a technical blocker). Similarity
-- search against these vectors returns arbitrary results — this satisfies the NOT NULL
-- constraint and the pipeline's structural wiring (Flyway/mvn verify/RAG boundary tests)
-- only. Re-run deploy/scripts/generate-rag-corpus-embeddings.sh and replace this block
-- wholesale once a real SPRING_AI_OPENAI_API_KEY is available (ADR-30).
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