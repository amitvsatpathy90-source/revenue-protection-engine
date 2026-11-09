---
asset_id: adr-30-rag-augmented-triage-pgvector
asset_path: docs/adrs/ADR-30.md
asset_type: adr
version: 1.5.0
created: 2026-09-04
last_updated: 2026-11-09
status: ACCEPTED
reversal_cost: MEDIUM
owner: amit
tags: [rag, pgvector, keyed-lookup, triage, resilience4j, retrieval, spring-ai, advisory]
---

# ADR-30 — RAG-Augmented Triage: pgvector corpus, keyed rule lookup (v1), independent R4j boundaries

## Status

`ACCEPTED`

Supersedes: nothing — extends ADR-15 (the advisory-only triage agent). Amendment 1.4.0 superseded this ADR's own
D2 (placeholder-gate proposal); v1.5.0 implements D3. Related: ADR-15 (parent decision), ADR-29 (R4j independence
invariant), ADR-11 (the published `corpus_mode` contract field).

---

## Context

`rpe-triage-agent` (ADR-15) produces advisory verdicts from an LLM agent loop but has no access to RPE's own rule-set
documentation at inference time — narratives are generated from alert data and tool-fetched account history alone. A
retrieval-augmented layer, grounding narratives in the actual `Detector` rule definitions, extends the existing
advisory-only, quarantined-from-core-pipeline design (ADR-15 §2.1) without diluting it. v1 scope is a hand-authored,
behavioral-only rule-set corpus — no account/PII data, no parametric threshold values, no dependency on core-pipeline
config.

## Decision

We add pgvector-backed RAG to `rpe-triage-agent` using a hand-authored behavioral-abstraction corpus and a fixed-field
retrieval key (rule type/detector name only, never raw alert free-text).

**v1 retrieval path (D3, implemented v1.5.0): exact `rule_name` lookup, no embedding call.**
`TriageService.fetchRagContext` calls `RagRetrievalClient.retrieveByRule(alert.ruleName(), topK)` →
`TriageVectorRepository.findByRuleName` (`WHERE rule_name = ? ORDER BY id LIMIT ?`) via raw JDBC on the existing dedicated
platform-thread pool, behind its own R4j boundary `triage.rag.retrieval.*` (ADR-29). The call runs after the inbox dedup
insert and before the LLM call, and is skipped unconditionally when the chat CB is open. `retrieve` (cosine `<=>` TopK) and
`similaritySearch` remain in the codebase, as do `RagEmbeddingClient` and its `triage.rag.embedding.*` boundary, for the v2
historical-alerts similarity corpus; none of them is on the v1 path.

**Why keyed lookup, not vector search (D3):** the v1 corpus is one row per detector and the query is already the categorical
rule name, so vector search is a key lookup through a lossy channel. See "Amendment 1.4.0 Decisions" for the reasoning and
the retrieval-selection evaluation behind D1.

**Why not Spring AI's `VectorStore` abstraction directly (still applies to any similarity path):** `VectorStore`'s public
contract is `similaritySearch(SearchRequest)` / `similaritySearch(String query)` only — `SearchRequest` carries a `String
query` field, and embedding happens opaquely inside the call. There is no vector-input overload on the interface, so a
wrapped call cannot produce two independently-measurable, independently-circuit-broken sub-boundaries (embedding-side rate
limits are OpenAI-tier-shaped; retrieval-side is a local Postgres query — different failure classes, ADR-29). A similarity
path therefore keeps the explicit two-call shape: `RagEmbeddingClient` (own R4j boundary) then `RagRetrievalClient.retrieve`
(own R4j boundary), fronted by `TriageVectorRepository` so pgvector-dialect coupling stays in one class.

## Alternatives Considered

- **`RetrievalAugmentationAdvisor` (Spring AI `ChatClient.Advisor`)** — rejected. Requires a
  `ChatClient.builder().advisors(...)` chain; `rpe-triage-agent` uses raw `ChatModel.call()` with a hand-rolled bounded
  tool-round loop (ADR-15 §3.3, `TriageAgent.MAX_TOOL_ROUNDS`) specifically because Spring AI 2.0 removed automatic
  internal tool execution and the round budget must live in code, not a framework chain. Migrating to `ChatClient` to
  gain Advisor support would mean rewriting the tool loop, `TriageLlmClient`'s R4j decorator chain, and re-opening
  ADR-15 §3.9's runtime decision for a dependency-injection convenience — disproportionate blast radius for what direct
  calls achieve at equal functional result.
- **`QuestionAnswerAdvisor`** — rejected for the same reason as `RetrievalAugmentationAdvisor` above (requires
  `ChatClient`), not (as v1.0 stated) in favor of the other Advisor for future multi-corpus routing. Neither Advisor
  fits this module's architecture; the multi-corpus-routing need (v2 historical-alerts corpus) is met by
  `RagRetrievalClient` accepting a corpus/collection parameter, no Advisor required.
- **`VectorStore.similaritySearch(String)` as a single call** — rejected. Technically simpler, but collapses the
  embedding and retrieval sub-boundaries into one CB/timeout, which cannot satisfy ADR-29's independence invariant at
  the granularity this ADR's Negative section specifies (see Decision, above). Would require walking back the
  "independent sub-boundary" language rather than implementing it.
- **Ollama / local embedding model** — rejected. ADR-15 §3.10 already rules out local-model fallback ("8GB host
  constraint"); no reason to add a second provider dependency for embeddings alone. Scope note (v1.4.0): Ollama is used
  only as a **test-scope** embedding generator for the retrieval eval harness (`spring-ai-starter-model-ollama`, test
  scope); no runtime provider change.
- **Retrieval before the inbox insert** — rejected; the insert must stay unconditionally first (the AI triage ruleset
  §2), and a pre-insert retrieval call risks delaying the dedup gate itself.
- **Shared R4j instance with `triage.llm`** — rejected per ADR-29. Different call shape, separate OpenAI rate-limit
  tier, and a shared CB would let RAG failures throttle chat capacity.
- **Retrieval query built from raw alert fields (merchant/memo)** — rejected. Attacker-controlled free-text shaping
  which corpus chunks return is a targeted-retrieval / information-disclosure vector, distinct from content injection; a
  fixed structured-field query removes attacker control over the query, not just the answer.
- **Retrieval query issued directly on the triage consumer's virtual thread** — rejected. Would violate the repo-wide
  "JDBC on dedicated platform-thread pool" Immutable Constraint; the pgvector query gets no exemption.
- **Retrieval attempted on the degraded (CB-open) path, discarded on failure** — rejected; keeps
  `DegradedTriageFallback`'s zero-Spring-AI-import constraint trivially true by never invoking retrieval, not just
  discarding its result.
- **Parametric corpus content (real threshold values) with dynamic runtime resolution** — rejected. Relocates the
  detection-logic disclosure risk rather than closing it (a resolved value reaching the narrative is exposed regardless
  of when it's computed), and a resolver reading live `Detector` config directly conflicts with ADR-15 §3.1's "no shared
  code with core modules" scope line.
- **CI/GitOps-generated corpus from `Detector` config, OIDC-signed artifacts, immutable registry** — rejected.
  Disproportionate to this project's stated scale ("lab benchmarks only," single-digit-USD/month triage spend); also
  implies the same core-pipeline coupling the parametric-resolver alternative was rejected for.
- **MMR / dedup / relative-score-floor selector stage on retrieved chunks** — rejected for v1. On prod-shaped (unscoped)
  pools MMR (0.5) regresses relevance, dedup trades relevance for diversity, and a relative score floor is a no-op
  because top-5 scores sit within 0.02 of the best hit (see Evaluation Findings). Lab-only evidence; revisit with a real
  corpus and real embeddings.

## Consequences

**Positive:**

- Narratives grounded in RPE's actual detection logic — addresses ADR-15 §6.4's named risk directly.
- Failure isolation is structural: the retrieval boundary has its own CB, separate from `triage.llm`, so a RAG outage
  degrades to `rag_context_used=false` without touching chat-completion availability (testing bar #1, #8). The v1 path
  makes no embedding call, so there is no OpenAI dependency or spend on retrieval; the embedding boundary stays
  independent for the v2 similarity path (testing bar #6).
- Fixed-field retrieval query closes the targeted-retrieval vector at the design level.
- Behavioral-only corpus content closes the detection-logic-disclosure risk at the design level — no runtime value ever
  reaches the narrative, so there is nothing to leak regardless of retrieval path.
- Hand-authored ingestion keeps the trust boundary narrow (single human writer) and leaves ADR-15 §3.1's
  zero-core-coupling scope fully intact — no exception required.
- Zero new egress/secret surface — v1 keyed lookup is a local Postgres query; a future embedding path is covered by the
  existing `allow-triage-egress` port-443 rule and `SPRING_AI_OPENAI_API_KEY`.
- Crash/sweep semantics require no new logic — retrieval failure is more of the existing "result gone, never re-attempt"
  surface the 5-minute sweep already covers.
- `TriageVectorRepository` interface confines pgvector-dialect coupling (`<=>`, `::vector` casts) to one implementation
  class — a future vector-store swap touches that class, not `TriageService` or the R4j boundaries around it.
- Embedding-client retry wiring confirmed via bytecode (2026-09-12) — no duplicate-retry blast radius on the embedding
  boundary, matching the already-fixed chat path.
- `gen_ai` embedding spans confirmed auto-instrumented (2026-09-12, docs) — `OpenAiEmbeddingModel` takes
  `ObservationRegistry`; Spring AI ships a dedicated `spring-ai-autoconfigure-model-embedding-observation` module
  consumed by the OpenAI autoconfig. ADR-25's tracing-coverage claim holds for the explicit `embed()` call path with no
  manual `Observation` wrap needed.
- `EvidenceValidator`/`ragSources[]` extension closed as not-applicable — confirmed against live `TriageAgent.java`: RAG
  context reaches only the prompt's fenced `<rag_context>` block, never `evidenceValidator.validate()` or
  `TriageVerdict`. RAG stays narrative-only by design; zero evidence-path changes needed.

**Negative:**

- **v1 retrieval gives no semantic matching.** Keyed lookup returns the detector's own row, and nothing else; it cannot
  surface cross-rule context. Accepted for a one-row-per-detector corpus; the similarity path (v2) is where that changes.
  RAG goes live in v1.5.0 for the first time against the seeded corpus: no faithfulness measurement exists, and the prompt
  grows by one chunk per alert (token-cost increase, unmeasured).
- **No UNIQUE constraint on `rule_name`.** `ORDER BY id` makes a duplicate row deterministic, not impossible. A constraint
  is a separate decision (see Amendment 1.4.0).
- **Bypassing `VectorStore` forfeits Spring AI's managed abstraction.** No framework-provided schema auto-configuration,
  no standardized multi-store test harness, no automatic future Spring AI `VectorStore` feature (e.g. hybrid
  keyword-vector search) without hand-rolling it in SQL. Accepted: this project needs two independently-measurable R4j
  boundaries more than it needs store portability, and `TriageVectorRepository` bounds the cost of a later
  reconsideration to one class. Applies to the (dormant) similarity path only; v1 uses plain SQL equality.
- **pgvector-dialect coupling is explicit, not hidden behind a framework.** Distance-operator choice (`<=>` cosine vs
  `<->` L2 vs `<#>` inner product) and `::vector` casts live in hand-written SQL inside `TriageVectorRepository`. Fine
  for this project's single-Postgres-target scope; would need rewriting, not reconfiguring, on a hypothetical move to a
  dedicated vector DB (Qdrant/Milvus/Pinecone) — no such move is planned or in scope.
- **Embedding-dimension lock-in.** The `vector_store` column type (`vector(1536)` for `text-embedding-3-small`) and the
  HNSW index are sized to one embedding model's output. Switching providers later is a migration (`ALTER TABLE`,
  re-index), not a config change. Noted as a residual, not solved now — no provider switch is planned.
- **Corpus staleness has no automated linkage to `Detector` deploys.** Hand-authored + behavioral-only lowers
  the stakes (a stale behavioral description stays roughly true after a threshold tune; a stale parametric one
  would go actively wrong) but doesn't eliminate the gap. Partial mitigation: the `rag.corpus.last_ingested_at`
  gauge exists (cached at startup, so a reseed shows only after restart); no alerting rule is configured.
  Since v1.5.0 a stale row reaches live prompts. See Residual Risks.
- **Narrative precision is reduced.** Behavioral descriptions can't cite exact thresholds. Accepted — same trade-off
  shape as the existing degraded-mode fallback: lose narrative quality, never lose correctness or availability.
- **Retrieved-chunk injection is a third prompt-injection surface**, distinct from the tool-result and `alert_data`
  paths ADR-15 §8 accounts for. Mitigated by a dedicated injection-corpus test (testing bar #2), not eliminated.
- **Shutdown-timeout margin is reduced, not preserved.** The original 20s `timeout-per-shutdown-phase` was sized against
  the chat path's 12s TimeLimiter alone. Worst-case in-flight duration is now `chat-TimeLimiter + retrieval-TimeLimiter`
  on the v1 path, plus `embedding-TimeLimiter` once a similarity path is enabled. **Provisional resolution:** the combined
  RAG ceiling stays a padded 5s. That figure assumes a single small-batch OpenAI embedding call (typically sub-2s P99) plus
  a sub-100ms raw pgvector query against a low-hundreds-row HNSW index. The 5s pads for tail latency across both calls
  without a measured sample. Sum: `12s + 5s = 17s`, so `timeout-per-shutdown-phase: 25s`; the previous 20s does not clear
  the sum with margin. **The 25s value was sized for the two-call path (now conservative for v1) and rests on OpenAI's
  documented endpoint characteristics, not a measured sample.** Re-derive it against real P50/P99 once the RAG latency
  sample (Negative-section item below) lands. Tighten the phase timeout down from 25s if the real number permits.
- **The raw pgvector query competes for the same dedicated-pool capacity** the inbox insert and degraded-fallback path
  depend on; `hikari.maximum-pool-size: 4` needs re-evaluation under combined load, not assumed sufficient by
  inheritance. **Does not require the full synthetic ingestor** — a narrow local harness (JMH/loop hitting
  `rpe-triage-agent` directly with concurrent inbox-insert + degraded-fallback + pgvector-query calls) suffices to
  observe `hikari.connections.pending`/wait-time. Run as a manual check during the coding phase, before wiring the full
  ingestor.
- **`triage.rag.embedding.*` and `triage.rag.retrieval.*` R4j values are only partially sourced.** `rpm`/
  `bulkhead-concurrent` on the embedding sub-boundary are grounded against OpenAI's published Tier-1 limits (3,000 RPM /
  1,000,000 TPM); retrieval-side timeout, CB thresholds, and retry shape remain `TBD` pending a real latency sample
  against the actual raw-JDBC pgvector query (not a generic estimate).
- **Embedding-client retry wiring — RESOLVED (2026-09-12, bytecode-confirmed):** `javap -p -c` against
  `OpenAiEmbeddingAutoConfiguration` (`spring-ai-autoconfigure-model-openai:2.0.0`) shows a call to
  `OpenAiCommonProperties.getMaxRetries()` inside the `openAiEmbeddingModel` bean method — the identical shared field
  ADR-15/RPE-10 already fixed to `0` for chat. Embedding inherits `max-retries: 0`; no separate override required.
  Closes testing bar #7's blocking condition.
- **Provider billing cap is shared across chat and embedding traffic** even though rate limits are independent
  per-endpoint. The ADR-15 §5.2 hard monthly cap needs headroom sized for both call types, not chat's baseline alone.
- **Second billing surface on top of ADR-15's existing one**, same per-instance-RateLimiter-is-the-only-cap shape as
  ADR-15 §5.2 / ADR-03.

## Evaluation Findings (retrieval selection, 2026-11-04 — lab-only)

Harness: `rpe-triage-agent/src/test/java/io/rpe/triage/rageval/` (test scope; never in the runtime
path). Ollama `mxbai-embed-large` (1024-d) on a 300-chunk templated synthetic corpus, pgvector HNSW
cosine, query text = rule name (as `TriageService.buildFixedFieldQuery`), K=20/40, one seed.
Embeddings differ from prod's `text-embedding-3-small`; absolute numbers do not transfer. The
integration test skips without Docker/Ollama — a skip is not a measurement.

| Selector                                  | allPass /15 | Note                                                                                            |
|-------------------------------------------|-------------|-------------------------------------------------------------------------------------------------|
| TopK (prod shape: unscoped, top-5, ≥0.75) | 1           | Baseline                                                                                        |
| TopK+dedup(0.90)                          | 7           | Unscoped velocity P@5 0.60 vs 0.80                                                              |
| MMR(0.5)                                  | 11          | Only 3 passes from unscoped rows; unscoped velocity K=40 P@5/grade0 0.40/0.60 vs TopK 0.80/0.20 |
| Relative floor δ=0.02–0.10                | 1           | Identical to TopK on every unscoped row; +dedup 2–7                                             |
| TopK + `rule_name` predicate (scoped)     | —           | K=40 P@5 1.00, grade0 0.00 — ceiling (clean labels)                                             |

- No label-free selector (dedup, MMR, relative floor) beat plain TopK on prod-shaped pools without regressing. This
  bounds any future similarity-retrieval path (D1); v1 uses keyed lookup (D3).
- Top-5 scores sit within 0.02 of the best hit, so score-based admissibility cannot separate
  cross-rule chunks.
- Not measured: faithfulness/hallucination, citations, label noise, held-out calibration, variance.

## Amendment 1.4.0 Decisions (2026-11-04)

**D1 — Selection among similarity candidates stays plain TopK (decided; applies to the future semantic path).** No
label-free selector (MMR, dedup, relative floor) beat cosine TopK on prod-shaped (unscoped) pools. Any future similarity
retrieval keeps plain TopK unless a selector change is backed by eval evidence on prod-shaped pools, not scoped
best-case pools. Main still runs cosine TopK (`top-k: 5`, `similarity-threshold: 0.75`) until D3 is implemented.

**D2 — Placeholder-corpus gap (SUPERSEDED by D3; never implemented).** `rag.corpus-mode` defaults to `PLACEHOLDER` and
is read only to stamp the verdict (`TriageAgent`, `DegradedTriageFallback`); `TriageService` does not read it, so
retrieved chunks reach the fenced `<rag_context>` block regardless of mode. A placeholder-mode gate (skip embedding and
retrieval, `triage.rag.skipped{reason=placeholder_corpus}`) was proposed, then dropped: the hazard is the non-semantic
seed vectors, and keyed lookup (D3) does not use vectors, so the gate would be deleted in the next release. Until D3 is
implemented, similarity retrieval over the placeholder vectors remains in main; its result is probably empty (random
vectors against the 0.75 threshold), which is an inference, not a measurement. The fate of `corpus_mode` on
`TriagedAlertMessage` (a published contract, additive-only per ADR-11) is decided with the D3 implementation, not here.

**D3 — Retrieval design for the v1 rule-doc corpus (DECIDED: exact `rule_name` lookup, no embedding call; implementation
pending).** The v1 corpus is one hand-authored row per detector (V3 seed), the retrieval query is already the
categorical `alert.ruleName()` (`TriageService.buildFixedFieldQuery`), and `triage_rag_corpus.rule_name` is indexed.
Vector search over this corpus is a key lookup through a lossy channel, and a `rule_name` predicate on top of it (option
a) leaves at most one row per rule, so ranking does nothing. Static prompt inclusion (option c) was rejected because it
drops the reviewed Flyway corpus and the retrieval boundary that the v2 historical-alerts corpus builds on. Decision: v1
retrieves by exact `rule_name` through the existing retrieval R4j boundary on the dedicated JDBC pool. The embedding
boundary is not on the v1 path; the embedding client and its configuration stay in the codebase for the v2 similarity
corpus. This is a structural argument, not an eval result: the harness measured selectors over candidate pools, not
keyed lookup. Not decided here, each a separate change: deleting dormant embedding code, a UNIQUE constraint on
`rule_name`, and the `corpus_mode` field.

## Residual Risks (explicit)

| Item                                                          | Note                                                                                                                                                                                                                                                                                                                         |
|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Corpus staleness metric                                       | Built (2026-09-12): `rag.corpus.last_ingested_at` gauge in `JdbcTriageVectorRepository`, cached-at-startup (`AtomicLong` set once from `MAX(created_at)`) — matches one-time hand-authored ingestion; will NOT reflect a live reseed without restart. Alerting rule not yet configured.                                      |
| Re-ingestion torn-read window                                 | Single transaction (delete+insert) — decided; blue-green rejected as disproportionate to hand-authored/single-writer ingestion scale                                                                                                                                                                                         |
| Shutdown-phase timeout — provisional value                    | Set to 25s (12s chat + 5s padded combined-RAG ceiling), sourced from OpenAI docs not measured latency — re-derive once a real P50/P99 sample exists                                                                                                                                                                          |
| `hikari.maximum-pool-size: 4`                                 | Reframed as a manual narrow-harness check, not blocked on the full synthetic ingestor — run during coding phase                                                                                                                                                                                                              |
| Single API key spans embedding + chat                         | Optional hardening — scope a restricted key to embeddings if adopted                                                                                                                                                                                                                                                         |
| `rag_context_used=false` conflates CB-open vs. empty-result   | Needs a distinct `rag.retrieval.empty_context.count` metric to stay diagnosable                                                                                                                                                                                                                                              |
| Chunking strategy unowned                                     | Size/overlap/splitter choice unassigned                                                                                                                                                                                                                                                                                      |
| `similarityThreshold`/`topK`                                  | Characterized on a synthetic corpus with non-prod embeddings (see Evaluation Findings); still no held-out calibration against prod embeddings                                                                                                                                                                                |
| HNSW build params, `vector_store` backup/restore              | Low-severity, pre-existing, still open                                                                                                                                                                                                                                                                                       |
| `DegradedTriageFallback` ArchUnit enforcement                 | No structural guard exists; RAG raises the stakes on an already-flagged gap — diff drafted, held                                                                                                                                                                                                                             |
| `TriageVectorRepository` interface — future backend swap cost | Bounded to one class by design (Decision, above); embedding-dimension lock-in (vector(1536), HNSW index) still requires a migration on provider switch, not eliminated by the interface                                                                                                                                      |
| AI triage ruleset section-number citations                    | File has four flat headers (Boundary, Pipeline shape, Rules, Known limitations), no numbered subsections — code comments and this ADR cite `§2`–`§7` that don't resolve. Fix pending: either restructure the rules file with real headers matching current citation granularity, or de-scope the citations. Not yet decided. |
| Placeholder seed vectors (D2, superseded)                     | Until D3 is implemented, similarity retrieval still runs over non-semantic vectors; result probably empty (unmeasured). Resolved by keyed lookup, not by a gate                                                                                                                                                              |
| Scoped-retrieval label-noise robustness                       | Unmeasured; relevant only to a future scoped similarity path (v1 keys on the detector's own `rule_name`)                                                                                                                                                                                                                     |
| Faithfulness / citation measurement                           | None; retrieval metrics do not establish fewer hallucinations                                                                                                                                                                                                                                                                |
| Testing Bar coverage vs code                                  | At `1986b70`, a search of `rpe-triage-agent/src/test` (excluding `rageval/`) found tests for #5 only; none found for #1, #3, #4, #6, #7; #2 not checked by name. Verify before treating the Testing Bar as enforced                                                                                                          |
| `similaritySearch` never run against live pgvector            | `JdbcTriageVectorRepository` javadoc flags it UNVERIFIED and no repository integration test exists. The eval harness uses its own SQL on `rag_eval_corpus` and does not cover it                                                                                                                                             |

---

## Testing Bar Additions (extends the AI triage ruleset §7)

1. **Retrieval-CB fallback test** — stubbed slow/failing `RagRetrievalClient` ⇒ `triage.rag.retrieval` CB opens
   independently of `triage.rag.embedding` and `triage.llm`; verdict produces with `rag_context_used=false`; no
   consumer-lag stall; chat LLM call proceeds normally.
2. **Retrieved-chunk injection corpus test** — rule-doc chunks containing instruction-like text ⇒ schema-valid verdicts,
   no evidence items lacking tool-call IDs, no verbatim compliance with injected instructions in `narrative`.
3. **Ingestion idempotency test** — same `(rule_set_id, version)` re-ingested twice ⇒ exactly one row set per version,
   no accumulation, no orphaned old-version vectors.
4. **Ordering/crash-recovery test** — crash between inbox insert and produce, with retrieval already completed
   pre-crash ⇒ sweep emits `rag_context_used=false` regardless of pre-crash retrieval outcome.
5. **Degraded-path zero-RAG-attempt test** — chat CB forced open ⇒ assert both `RagEmbeddingClient` and
   `RagRetrievalClient` are never invoked (zero interactions, not just discarded output).
6. **Independent CB isolation test** — force `triage.rag.embedding` CB open via embedding failures only ⇒
   `triage.rag.retrieval` and `triage.llm` CB state unaffected; force `triage.rag.retrieval` CB open via pgvector query
   failures only ⇒ `triage.rag.embedding` and `triage.llm` CB state unaffected. Three-way isolation, not two.
7. **Embedding-retry double-fire test** — unblocked (2026-09-12, see Negative section): embedding confirmed to inherit
   chat's `max-retries: 0`. Assert exactly one retry sequence fires on a 429 — R4j's, not a duplicate SDK-level retry.
8. **Keyed-lookup tests (with D3, once implemented)** — exact `rule_name` match returns that detector's chunk; unknown
   rule ⇒ empty context and `rag_context_used=false`; chat CB open ⇒ zero retrieval calls; retrieval failure ⇒
   unaugmented verdict; retrieved content reaches the prompt; repository integration test against a real pgvector
   Postgres. Items 6 and 7 bind the embedding boundary, which is off the v1 path; they apply again when a similarity
   path is enabled.

## Reversal Cost

`MEDIUM` — the keyed-lookup path itself is code-only: returning an empty list from `TriageService.fetchRagContext`
restores unaugmented triage, which is already the degraded mode. Two things raise the cost. V2 and V3 are applied Flyway
migrations (immutable checksums; the `vector` extension and `triage_rag_corpus` exist wherever they ran). And
`corpus_mode` is stamped on the published `payment.alerts.triaged` contract (ADR-11, additive-only), so it cannot be
removed silently. Switching RAG off is cheap; unwinding its schema and contract surface is not.

## References

- `rpe-triage-agent` — `RagRetrievalClient.retrieveByRule` / `TriageVectorRepository.findByRuleName`,
  the v1 keyed path (v1.5.0); `RagRetrievalClientTest` covers the shared R4j chain on both entry points.
- `rpe-triage-agent/src/main/resources/db/migration/V2__create_triage_rag_corpus.sql`,
  `V3__seed_triage_rag_corpus.sql` — corpus schema and the three-row seed (never edited after apply;
  Flyway checksum).
- `rpe-triage-agent/src/test/java/io/rpe/triage/rageval/` — retrieval-selection harness behind D1
  (merged in `19307b1`, PR #44).
- `docs/adrs/ADR-15.md` — parent decision this ADR extends.
- `docs/adrs/ADR-29.md` — no-global-R4j-defaults rule.
- `docs/adrs/ADR-11.md` — additive-only contract rule behind the `corpus_mode` field.
- `docs/adrs/ADR-22.md` — shutdown-phase ladder and timeout-trio contract (the 25s sizing above).
- `deploy/k8s/netpol/41-allow-paths.yaml` — egress coverage for the dormant embedding path; the v1
  keyed lookup is local Postgres and needs no netpol change.
- Spring AI `VectorStore` interface (verified 2026-09-05; version-dependent): `similaritySearch(SearchRequest)` /
  `similaritySearch(String)` only, no vector-input overload — grounds the "Why not Spring AI's `VectorStore`
  abstraction directly" paragraph in Decision.

## Changelog

| Version | Date       | Author | Description                                                                                                                                                                                                                                                                                                                                            |
|---------|------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1.0.0   | 2026-09-04 | @amit  | Initial — PROPOSED. Adds pgvector-backed RAG to triage using behavioral-only rule documentation, while keeping retrieval outside the core pipeline and advisory-only boundary.                                                                                                                                                                         |
| 1.1.0   | 2026-09-04 | @amit  | Amendment — PROPOSED. Confirms ADR numbering and reframes embedding-retry and Hikari-pool items as manual coding-phase validation rather than blockers.                                                                                                                                                                                                |
| 1.2.0   | 2026-09-05 | @amit  | Amendment — PROPOSED. Replaces `RetrievalAugmentationAdvisor` with explicit `EmbeddingModel` + pgvector JDBC calls through `RagEmbeddingClient` and `RagRetrievalClient`, preserving independent Resilience4j boundaries for embedding and retrieval.                                                                                                  |
| 1.3.0   | 2026-09-12 | @amit  | Amendment — PROPOSED. Closes 3 Residuals via verification pass: embedding-retry wiring confirmed (bytecode), `gen_ai` embedding spans confirmed (docs), `EvidenceValidator`/`ragSources[]` closed as not-applicable (RAG narrative-only by design). `rag.corpus.last_ingested_at` staleness metric implemented (cached-at-startup).                    |
| 1.4.0   | 2026-11-04 | @amit  | Amendment — PROPOSED. Adds retrieval-selection evaluation findings (D1: TopK for any similarity path), supersedes the placeholder-gate proposal (D2), decides exact `rule_name` lookup for v1 (D3, implementation pending); updates Residuals and replaces Testing Bar #8. Status flips with the Decision-section rewrite in the implementing release. |
| 1.5.0   | 2026-11-09 | @amit  | Amendment — ACCEPTED. Implements D3: `TriageService` retrieves by exact `rule_name` (`retrieveByRule` → `findByRuleName`), no embedding call on v1. Adds `triage.rag.retrieval.empty` counter and 3s statement timeout; layout aligned to standard ADR format. `corpus_mode`, embedding code, UNIQUE constraint stay separate decisions.               |
