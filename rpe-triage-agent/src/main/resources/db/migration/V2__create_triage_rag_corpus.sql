-- RAG corpus for triage narrative grounding (ADR-30). Lives in the triage schema,
-- same single-writer-per-table discipline as triaged_alerts (V1).
--
-- Content policy (hard constraint): rows describe DETECTOR BEHAVIOR ONLY, never a
-- parametric threshold value — a threshold here is retrievable by a prompt-injection
-- path and discloses detection-evasion parameters. Enforced by review — there is no
-- automated guard (DB constraint or otherwise) against this today.
--
-- Ingestion is hand-authored (ADR-30 — CI/GitOps auto-generation rejected). This
-- migration creates the empty table only; a fresh environment boots with zero corpus
-- rows and RAG silently retrieves nothing rather than auto-populating unreviewed
-- content. Seeding mechanism: separate data-only Flyway migration (V3__seed_triage_rag_corpus.sql),
-- not a manual runbook — decided, not open. This migration creates the empty table only.

-- CREATE EXTENSION requires superuser/CREATE privilege triage_role lacks (ADR-17 §5.2/5.3
-- schema-scoped least-privilege). Resolved: superuser pre-creates via CNPG postInitSQL in
-- k8s bootstrap, before this migration runs. This line is a no-op once the extension exists.
CREATE EXTENSION IF NOT EXISTS vector;

-- vector(1536): text-embedding-3-small's default output size. UNVERIFIED against a
-- live API call this session. Tied to TriageProperties.Rag.Embedding.model — a
-- dimensions-param override or provider switch requires a matching ALTER TABLE +
-- re-index, same class of silent config-vs-schema mismatch risk as other
-- config-coupling points in this system (see ADR-28 for the purge-horizon analog).
-- No automated guard ties this column width to the config value today.
CREATE TABLE triage_rag_corpus (
    id           BIGSERIAL   PRIMARY KEY,
    rule_name    TEXT        NOT NULL,      -- links a chunk to its detector
    content      TEXT        NOT NULL,      -- behavioral description only, no thresholds
    embedding    VECTOR(1536) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- HNSW over IVFFlat: IVFFlat's list count is tuned for a known row count in advance,
-- which doesn't fit a slowly hand-grown corpus; HNSW needs no such tuning step and
-- recall stays stable as the corpus grows. Requires pgvector >=0.5.0 — unverified
-- against the pinned extension version; confirm before trusting this index creates.
CREATE INDEX idx_triage_rag_corpus_embedding
    ON triage_rag_corpus
    USING hnsw (embedding vector_cosine_ops);

CREATE INDEX idx_triage_rag_corpus_rule_name ON triage_rag_corpus (rule_name);
