
# ADR-30 — RAG-Augmented Triage: pgvector, OpenAI Embeddings, Independent R4j Boundary

**Status:** PROPOSED

**Decided on:** 2026-09-04
**Owner:** amit
**Related:** ADR-15 (AI triage agent — parent decision), ADR-29 (R4j independence invariant)

---

### Context

`rpe-triage-agent` (ADR-15) produces advisory verdicts from an LLM agent loop but has no access to RPE's own rule-set documentation at inference time — narratives are generated from alert data and tool-fetched account history alone. A retrieval-augmented layer, grounding narratives in the actual `Detector` rule definitions, extends the existing advisory-only, quarantined-from-core-pipeline design (ADR-15 §2.1) without diluting it. v1 scope is a hand-authored, behavioral-only rule-set corpus — no account/PII data, no parametric threshold values, no dependency on core-pipeline config.

### Decision

We add pgvector-backed retrieval to `rpe-triage-agent` using OpenAI `text-embedding-3-small`, a hand-authored behavioral-abstraction corpus, and a fixed-field retrieval query (rule type/detector name/severity only, never raw alert free-text).

Retrieval is implemented as two explicit, independently-wrapped calls, not a single Spring AI `VectorStore.similaritySearch()` round trip:

1. **`RagEmbeddingClient`** — wraps `EmbeddingModel.embed(String)` directly. Own R4j boundary: `triage.rag.embedding.*` (ADR-29).
2. **`RagRetrievalClient`** — takes the embedding vector from (1) and issues a hand-written pgvector similarity query (`ORDER BY embedding <=> ?::vector LIMIT ?`) via raw JDBC on the existing dedicated platform-thread pool. Own R4j boundary: `triage.rag.retrieval.*` (ADR-29). Fronted by a `TriageVectorRepository` interface so the pgvector-dialect coupling this introduces stays isolated to one implementation.

Both calls execute after the inbox dedup insert and before the LLM call, and are skipped unconditionally when the chat CB is open. The shutdown-phase timeout must cover the combined chat-plus-RAG TimeLimiter budget (embedding + retrieval, not one merged figure).

**Why not Spring AI's `VectorStore` abstraction directly:** `VectorStore`'s public contract is `similaritySearch(SearchRequest)` / `similaritySearch(String query)` only — `SearchRequest` carries a `String query` field, and embedding happens opaquely inside the call. There is no vector-input overload on the interface. Wrapping that single opaque call cannot produce two independently-measurable, independently-circuit-broken sub-boundaries; it collapses embedding and retrieval into one undifferentiated call regardless of how the R4j config is labeled. Bypassing `VectorStore` for the explicit two-call shape is the only way to honor ADR-29 at the granularity this ADR's own Negative section requires (embedding-side rate limits are OpenAI-tier-shaped; retrieval-side is a local Postgres query — different failure classes, different tuning, must not share a CB).

### Alternatives Considered

- **`RetrievalAugmentationAdvisor` (Spring AI `ChatClient.Advisor`)** — rejected. Requires a `ChatClient.builder().advisors(...)` chain; `rpe-triage-agent` uses raw `ChatModel.call()` with a hand-rolled bounded tool-round loop (ADR-15 §3.3, `TriageAgent.MAX_TOOL_ROUNDS`) specifically because Spring AI 2.0 removed automatic internal tool execution and the round budget must live in code, not a framework chain. Migrating to `ChatClient` to gain Advisor support would mean rewriting the tool loop, `TriageLlmClient`'s R4j decorator chain, and re-opening ADR-15 §3.9's runtime decision for a dependency-injection convenience — disproportionate blast radius for what direct calls achieve at equal functional result.
- **`QuestionAnswerAdvisor`** — rejected for the same reason as `RetrievalAugmentationAdvisor` above (requires `ChatClient`), not (as v1.0 stated) in favor of the other Advisor for future multi-corpus routing. Neither Advisor fits this module's architecture; the multi-corpus-routing need (v2 historical-alerts corpus) is met by `RagRetrievalClient` accepting a corpus/collection parameter, no Advisor required.
- **`VectorStore.similaritySearch(String)` as a single call** — rejected. Technically simpler, but collapses the embedding and retrieval sub-boundaries into one CB/timeout, which cannot satisfy ADR-29's independence invariant at the granularity this ADR's Negative section specifies (see Decision, above). Would require walking back the "independent sub-boundary" language rather than implementing it.
- **Ollama / local embedding model** — rejected. ADR-15 §3.10 already rules out local-model fallback ("8GB host constraint"); no reason to add a second provider dependency for embeddings alone.
- **Retrieval before the inbox insert** — rejected; the insert must stay unconditionally first (the AI triage ruleset §2), and a pre-insert retrieval call risks delaying the dedup gate itself.
- **Shared R4j instance with `triage.llm`** — rejected per ADR-29. Different call shape, separate OpenAI rate-limit tier, and a shared CB would let RAG failures throttle chat capacity.
- **Retrieval query built from raw alert fields (merchant/memo)** — rejected. Attacker-controlled free-text shaping which corpus chunks return is a targeted-retrieval / information-disclosure vector, distinct from content injection; a fixed structured-field query removes attacker control over the query, not just the answer.
- **Retrieval query issued directly on the triage consumer's virtual thread** — rejected. Would violate the repo-wide "JDBC on dedicated platform-thread pool" Immutable Constraint; the pgvector query gets no exemption.
- **Retrieval attempted on the degraded (CB-open) path, discarded on failure** — rejected; keeps `DegradedTriageFallback`'s zero-Spring-AI-import constraint trivially true by never invoking retrieval, not just discarding its result.
- **Parametric corpus content (real threshold values) with dynamic runtime resolution** — rejected. Relocates the detection-logic disclosure risk rather than closing it (a resolved value reaching the narrative is exposed regardless of when it's computed), and a resolver reading live `Detector` config directly conflicts with ADR-15 §3.1's "no shared code with core modules" scope line.
- **CI/GitOps-generated corpus from `Detector` config, OIDC-signed artifacts, immutable registry** — rejected. Disproportionate to this project's stated scale ("lab benchmarks only," single-digit-USD/month triage spend); also implies the same core-pipeline coupling the parametric-resolver alternative was rejected for.

### Consequences

**Positive:**
- Narratives grounded in RPE's actual detection logic — addresses ADR-15 §6.4's named risk directly.
- Failure isolation is structural: independent CB on both the embedding and retrieval sub-boundaries (true independence now, via the explicit two-call shape — not simulated by config labels on one merged call) means a RAG outage degrades to `rag_context_used=false` without touching chat-completion availability (testing bar #1, #6).
- Fixed-field retrieval query closes the targeted-retrieval vector at the design level.
- Behavioral-only corpus content closes the detection-logic-disclosure risk at the design level — no runtime value ever reaches the narrative, so there is nothing to leak regardless of retrieval path.
- Hand-authored ingestion keeps the trust boundary narrow (single human writer) and leaves ADR-15 §3.1's zero-core-coupling scope fully intact — no exception required.
- Zero new egress/secret surface — existing `allow-triage-egress` port-443 rule and `SPRING_AI_OPENAI_API_KEY` cover both call types.
- Crash/sweep semantics require no new logic — retrieval failure is more of the existing "result gone, never re-attempt" surface the 5-minute sweep already covers.
- `TriageVectorRepository` interface confines pgvector-dialect coupling (`<=>`, `::vector` casts) to one implementation class — a future vector-store swap touches that class, not `TriageService` or the R4j boundaries around it.

**Negative:**
- **Bypassing `VectorStore` forfeits Spring AI's managed abstraction.** No framework-provided schema auto-configuration, no standardized multi-store test harness, no automatic future Spring AI `VectorStore` feature (e.g. hybrid keyword-vector search) without hand-rolling it in SQL. Accepted: this project needs two independently-measurable R4j boundaries more than it needs store portability, and `TriageVectorRepository` bounds the cost of a later reconsideration to one class.
- **pgvector-dialect coupling is explicit, not hidden behind a framework.** Distance-operator choice (`<=>` cosine vs `<->` L2 vs `<#>` inner product) and `::vector` casts live in hand-written SQL inside `TriageVectorRepository`. Fine for this project's single-Postgres-target scope; would need rewriting, not reconfiguring, on a hypothetical move to a dedicated vector DB (Qdrant/Milvus/Pinecone) — no such move is planned or in scope.
- **Embedding-dimension lock-in.** The `vector_store` column type (`vector(1536)` for `text-embedding-3-small`) and the HNSW index are sized to one embedding model's output. Switching providers later is a migration (`ALTER TABLE`, re-index), not a config change. Noted as a residual, not solved now — no provider switch is planned.
- **Corpus staleness has no automated linkage to `Detector` deploys.** Hand-authored + behavioral-only lowers the stakes (a stale behavioral description stays roughly true after a threshold tune; a stale parametric one would go actively wrong) but doesn't eliminate the gap. Minimum bar: a staleness metric (`rag.corpus.last_ingested_at` age, alerted) — not yet built.
- **Narrative precision is reduced.** Behavioral descriptions can't cite exact thresholds. Accepted — same trade-off shape as the existing degraded-mode fallback: lose narrative quality, never lose correctness or availability.
- **Retrieved-chunk injection is a third prompt-injection surface**, distinct from the tool-result and `alert_data` paths ADR-15 §8 accounts for. Mitigated by a dedicated injection-corpus test (testing bar #2), not eliminated.
- **Shutdown-timeout margin is reduced, not preserved.** The original 20s `timeout-per-shutdown-phase` was sized against the chat path's 12s TimeLimiter alone. Worst-case in-flight duration is now `chat-TimeLimiter + embedding-TimeLimiter + retrieval-TimeLimiter`. **Provisional resolution:** the combined RAG ceiling stays a padded 5s (single small-batch OpenAI embedding call, typically sub-2s P99, plus a sub-100ms raw pgvector query against a low-hundreds-row HNSW index — 5s pads for tail latency across both calls without a measured sample). Sum: `12s + 5s = 17s`, so `timeout-per-shutdown-phase: 25s` (previous 20s does not clear the sum with margin). **This value is sourced from OpenAI's documented endpoint characteristics, not a measured sample** — re-derive against real P50/P99 once the RAG latency sample (Negative-section item below) lands, and tighten the phase timeout down from 25s if the real number permits.
- **The raw pgvector query competes for the same dedicated-pool capacity** the inbox insert and degraded-fallback path depend on; `hikari.maximum-pool-size: 4` needs re-evaluation under combined load, not assumed sufficient by inheritance. **Does not require the full synthetic ingestor** — a narrow local harness (JMH/loop hitting `rpe-triage-agent` directly with concurrent inbox-insert + degraded-fallback + pgvector-query calls) suffices to observe `hikari.connections.pending`/wait-time. Run as a manual check during the coding phase, before wiring the full ingestor.
- **`triage.rag.embedding.*` and `triage.rag.retrieval.*` R4j values are only partially sourced.** `rpm`/`bulkhead-concurrent` on the embedding sub-boundary are grounded against OpenAI's published Tier-1 limits (3,000 RPM / 1,000,000 TPM); retrieval-side timeout, CB thresholds, and retry shape remain `TBD` pending a real latency sample against the actual raw-JDBC pgvector query (not a generic estimate).
- **Embedding-client retry wiring is unverified** — ADR-15's chat-path fix (`max-retries: 0`, RPE-10) was bytecode-confirmed for `OpenAiChatAutoConfiguration` specifically; whether `OpenAiEmbeddingAutoConfiguration`/the embedding model's `RetryTemplate` shares that wiring is unconfirmed — this now matters directly since `RagEmbeddingClient` calls `EmbeddingModel.embed()` explicitly rather than through a Spring AI-managed path. **Static check, no ingestor needed** — `mvn dependency:tree` to pin the `spring-ai-openai` version, then `javap`/decompile `OpenAiEmbeddingAutoConfiguration`/`OpenAiEmbeddingModel` to confirm whether it sources retry config from `OpenAiCommonProperties.getMaxRetries()` (shared, already fixed) or builds its own default independently. Run manually during the coding phase when the module first takes shape; if independent, apply the same fix (`spring.ai.openai.embedding.max-retries: 0`).
- **Provider billing cap is shared across chat and embedding traffic** even though rate limits are independent per-endpoint. The ADR-15 §5.2 hard monthly cap needs headroom sized for both call types, not chat's baseline alone.
- **Second billing surface on top of ADR-15's existing one**, same per-instance-RateLimiter-is-the-only-cap shape as ADR-15 §5.2 / ADR-03.

### References
- `docs/adrs/ADR-15.md` — parent decision this ADR extends
- `docs/adrs/ADR-29.md` — no-global-R4j-defaults rule
- `CLAUDE.md` Immutable Constraints — JDBC-dedicated-pool rule; shutdown-order/timeout-trio discipline (same reasoning pattern applied here)
- the AI triage ruleset §2, §4 (needs addendum for third injection path), §5, §7 (testing bar additions) — **section-number citations here are provisional pending a rules-file restructure; the current file has no numbered subsections (see Residuals)**
- `deploy/k8s/netpol/41-allow-paths.yaml` — existing egress coverage, no netpol change required
- Spring AI `VectorStore` interface (verified 2026-09-05): `similaritySearch(SearchRequest)` / `similaritySearch(String)` only, no vector-input overload — grounds the "why not `VectorStore` directly" call in Decision, above.

---

### Residuals (non-blocking, tracked)

| Item | Note |
|---|---|
| Corpus staleness metric | `rag.corpus.last_ingested_at` age, alerted — not yet built |
| Re-ingestion torn-read window | Single transaction (delete+insert) — decided; blue-green rejected as disproportionate to hand-authored/single-writer ingestion scale |
| Shutdown-phase timeout — provisional value | Set to 25s (12s chat + 5s padded combined-RAG ceiling), sourced from OpenAI docs not measured latency — re-derive once a real P50/P99 sample exists |
| Embedding retry wiring | Reframed as a manual static check (bytecode/`javap`), not blocked on the synthetic ingestor — run during coding phase; now directly relevant since `RagEmbeddingClient` calls `EmbeddingModel.embed()` explicitly |
| `hikari.maximum-pool-size: 4` | Reframed as a manual narrow-harness check, not blocked on the full synthetic ingestor — run during coding phase |
| Single API key spans embedding + chat | Optional hardening — scope a restricted key to embeddings if adopted |
| `rag_context_used=false` conflates CB-open vs. empty-result | Needs a distinct `rag.retrieval.empty_context.count` metric to stay diagnosable |
| `EvidenceValidator` doesn't cover retrieved chunks | Extend `evidence[]`/add `ragSources[]`; extend validator to the new source — design locked, diff drafted, held pending `TriageAgent.java` structured-output schema confirmation |
| Chunking strategy unowned | Size/overlap/splitter choice unassigned |
| `gen_ai` embedding spans under existing trace | With the explicit `EmbeddingModel.embed()` call (not a Spring AI-managed `VectorStore` path), automatic Spring AI observation instrumentation (ADR-25) needs re-confirming — unconfirmed, was previously assumed likely-automatic under the `VectorStore` path that is no longer used |
| `similarityThreshold`/`topK` | Unsourced, needs calibration pass against held-out query set |
| HNSW build params, `vector_store` backup/restore | Low-severity, pre-existing, still open |
| `DegradedTriageFallback` ArchUnit enforcement | No structural guard exists; RAG raises the stakes on an already-flagged gap — diff drafted, held |
| `TriageVectorRepository` interface — future backend swap cost | Bounded to one class by design (Decision, above); embedding-dimension lock-in (vector(1536), HNSW index) still requires a migration on provider switch, not eliminated by the interface |
| AI triage ruleset section-number citations | File has four flat headers (Boundary, Pipeline shape, Rules, Known limitations), no numbered subsections — code comments and this ADR cite `§2`–`§7` that don't resolve. Fix pending: either restructure the rules file with real headers matching current citation granularity, or de-scope the citations. Not yet decided. |

---

### Testing Bar Additions (extends the AI triage ruleset §7)

1. **Retrieval-CB fallback test** — stubbed slow/failing `RagRetrievalClient` ⇒ `triage.rag.retrieval` CB opens independently of `triage.rag.embedding` and `triage.llm`; verdict produces with `rag_context_used=false`; no consumer-lag stall; chat LLM call proceeds normally.
2. **Retrieved-chunk injection corpus test** — rule-doc chunks containing instruction-like text ⇒ schema-valid verdicts, no evidence items lacking tool-call IDs, no verbatim compliance with injected instructions in `narrative`.
3. **Ingestion idempotency test** — same `(rule_set_id, version)` re-ingested twice ⇒ exactly one row set per version, no accumulation, no orphaned old-version vectors.
4. **Ordering/crash-recovery test** — crash between inbox insert and produce, with retrieval already completed pre-crash ⇒ sweep emits `rag_context_used=false` regardless of pre-crash retrieval outcome.
5. **Degraded-path zero-RAG-attempt test** — chat CB forced open ⇒ assert both `RagEmbeddingClient` and `RagRetrievalClient` are never invoked (zero interactions, not just discarded output).
6. **Independent CB isolation test** — force `triage.rag.embedding` CB open via embedding failures only ⇒ `triage.rag.retrieval` and `triage.llm` CB state unaffected; force `triage.rag.retrieval` CB open via pgvector query failures only ⇒ `triage.rag.embedding` and `triage.llm` CB state unaffected. Three-way isolation, not two.
7. **Embedding-retry double-fire test** — *blocked pending `OpenAiEmbeddingAutoConfiguration` bytecode verification (see Negative section).* If embedding has independent retry wiring: assert exactly one retry sequence fires on a 429, not R4j's stacked on the SDK's own.

## Changelog

| Version | Date | Author | Description |
|---|---|---|---|
| 1.0.0 | 2026-09-04 | @amit | Initial — PROPOSED. Adds pgvector-backed RAG to triage using behavioral-only rule documentation, while keeping retrieval outside the core pipeline and advisory-only boundary. |
| 1.1.0 | 2026-09-04 | @amit | Amendment — PROPOSED. Confirms ADR numbering and reframes embedding-retry and Hikari-pool items as manual coding-phase validation rather than blockers. |
| 1.2.0 | 2026-09-05 | @amit | Amendment — PROPOSED. Replaces `RetrievalAugmentationAdvisor` with explicit `EmbeddingModel` + pgvector JDBC calls through `RagEmbeddingClient` and `RagRetrievalClient`, preserving independent Resilience4j boundaries for embedding and retrieval. |