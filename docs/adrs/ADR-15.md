<!-- edit-log (newest first): v1.1 | 2026-07-04 | Doc-drift audit correction — status PROPOSED → ACCEPTED. The module was fully implemented, is cited as settled/binding by ADR-17/18/23/24 and every rules file, and passes 31/31 tests; the status field was simply never flipped after implementation landed. Also dropped the dangling "arch-decisions-log.md §2.1" cross-reference — that file does not exist anywhere in this repo. | v1.0 | 2026-06-12 | Initial — status PROPOSED. RPE service-scoped series (not workspace ADR-NNNN). -->

# ADR-15 — AI Triage Agent: Spring AI, advisory-only, downstream of `payment.alerts`

- **Status:** ACCEPTED
- **Decided on:** 2026-06-12
- **Owner:** amit
- **Reversal cost:** LOW — delete the `rpe-triage-agent` module and the `payment.alerts.triaged` topic; core pipeline is untouched by design.
- **Related:** ADR-02 (R4j fallback philosophy), ADR-13 (UUIDv5 alert identity), ADR-14 (Detector split — keeps detection deterministic, which this ADR depends on)

---

## 1. TL;DR

We add agentic LLM-based alert triage as a **separate Maven module (`rpe-triage-agent`)** consuming `payment.alerts` and producing `payment.alerts.triaged` with `{severity, narrative, evidence[], confidence, triage_status}`. The LLM is treated as the system's least reliable, most expensive outbound dependency: full Resilience4j stack, hard tool-round budget, schema-validated structured output, rule-based degraded fallback. **The LLM never gates detection, alert identity, or delivery.** Framework: Spring AI. LangChain4j rejected.

---

## 2. Context

RPE's detection path is deliberately deterministic: atomic Lua gate, threshold `Detector`s (ADR-14), UUIDv5 alert identity (ADR-13), exactly-once delivery via transactional relay. Those properties are the project's core claim and must not be diluted.

Real fraud platforms pair deterministic detection with probabilistic triage: an analyst-facing layer that explains *why* an alert matters, pulls supporting evidence, and assigns severity. An agentic LLM with tool access is a credible implementation of that layer — and a portfolio-relevant one — **provided** it is architecturally quarantined from the correctness path.

### 2.1 Forces in tension

| Force | Direction | Notes |
|---|---|---|
| Deterministic correctness claims | Keep LLM out entirely | Non-determinism upstream of `alert_id` breaks replay safety silently |
| Portfolio signal (agentic AI) | Add LLM capability | Tool-calling agency + resilience story is the differentiator |
| Operational cost / reliability | Bound the LLM hard | Unbounded tail latency, per-token billing, provider outages |
| Coupling | Separate module | Core Architecture Spec, benchmarks, invariants must remain untouched |

---

## 3. Decision

We build `rpe-triage-agent` as an independent Spring Boot module with these binding sub-decisions:

1. **Topology:** consumes `payment.alerts` on its own consumer group with `isolation.level=read_committed`; produces enriched verdicts to `payment.alerts.triaged`. No shared code with the core pipeline beyond the alert DTO contract (`@JsonIgnoreProperties`, `schema_version` — ADR-11 applies).
2. **Framework:** Spring AI (`ChatClient` + `@Tool` tool-calling + structured output). Single framework; no LangChain4j.
3. **Agency budget:** the agent selects tools per alert type; **max 3 tool-call rounds**, enforced in code, not prompt.
4. **Tools (Phase 1 ships 2; all read-only):** `redisAccountHistory(account_id)`, `recentAlertsForAccount(account_id)` (reads `processed_alerts`); Phase 2: `geoContext(lat, lon)`, `merchantAggregate(merchant_id)`.
5. **Idempotency:** inbox pattern — `INSERT INTO triaged_alerts(alert_id, …) ON CONFLICT DO NOTHING` **before** the LLM call; conflict ⇒ skip. Keyed on the deterministic UUIDv5 `alert_id` (ADR-13 dividend).
6. **Resilience (binding config, no global defaults):** TimeLimiter 12s hard cap; CircuitBreaker on **slow-call rate** (slow ≥ 8s, threshold 50%) in addition to error rate; Bulkhead 5 concurrent calls; RateLimiter as billing breaker (per-minute call cap from env); Retry max 2, exponential backoff, **only** on transport/429/5xx — never on content/parse failures.
7. **Degraded mode:** CB open, retry exhaustion, or schema-validation failure ⇒ emit verdict with `triage_status=DEGRADED_RULE_BASED` and static severity mapped from rule type. Alerts are never dropped or delayed indefinitely by LLM unavailability.
8. **Injection containment:** raw event free-text (merchant name, memo) is passed only as fenced *data*, never concatenated into the system prompt; output bound to a Java record via Spring AI structured output, rejected on parse failure; every `evidence[]` entry must reference a tool-call ID from this run — unreferenced claims are dropped by a validator.
9. **Runtime:** single MVC + virtual-threads surface. Blocking `ChatClient` call on a VT, wrapped in the R4j stack (TimeLimiter via `CompletableFuture` on the VT executor). No WebFlux in this module.
10. **Provider:** abstracted behind Spring AI; key via env (`SPRING_AI_*`), never committed. Local-model fallback is out of scope (8GB host constraint).

### 3.1 Scope of binding

- **Applies to:** `rpe-triage-agent` module, `payment.alerts.triaged` topic, `triaged_alerts` table.
- **Does not apply to:** core pipeline — no core file changes beyond Architecture Spec index/constraint rows.
- **Exceptions via:** successor ADR only.

---

## 4. Alternatives Considered

| Option | Chosen / Rejected | Reason |
|---|---|---|
| Spring AI, separate downstream module | **Chosen** | Boot-native auto-config, Micrometer `gen_ai` metrics out of the box, structured output → record binding; zero coupling to core; LLM failure cannot touch delivery |
| LangChain4j | Rejected | ~90% feature overlap with Spring AI; second AI framework in one stack is incoherent; weaker Boot/Micrometer integration. Revisit only on a concrete Spring AI feature gap |
| LLM scoring inside `Detector` (in the lane) | Rejected | Seconds-scale latency on a path budgeted in ms; non-determinism upstream of UUIDv5 breaks replay safety; provider outage halts ingestion. Violates two Immutable Constraints outright |
| LLM verdict gating relay delivery | Rejected | Couples exactly-once delivery to the flakiest dependency; CB-open would mean alert loss or unbounded delay — both unacceptable |
| Deterministic weighted-score adjudicator (no LLM) | Deferred, not rejected | Cheaper and replay-safe, but doesn't demonstrate agentic capability — the stated goal. Remains the documented Stage-1 upgrade path; would take ADR-16 if built |

---

## 5. Consequences

### 5.1 Positive
- **"Deterministic detection, probabilistic triage"** — architecturally enforced, demoable (kill the LLM endpoint; show CB open in Grafana; show `DEGRADED_RULE_BASED` verdicts still flowing).
- **ADR-13/14 dividends realized:** UUIDv5 makes triage idempotency a one-line `ON CONFLICT`; Detector split keeps thresholds out of the prompt.
- **Strictly additive:** module deletion = full rollback.

### 5.2 Negative
- **New billing surface.** Per-token cost; per-instance RateLimiter is the only enforcement (global = `N × per_instance`, same shape as ADR-03's limitation).
- **Non-deterministic output.** Same alert ⇒ different narrative on reprocess. Bounded by inbox dedup; accepted as advisory-tier residual.
- **New secret in the environment** (provider API key) — breaks the prior "no API billing" workspace posture; requires a hard monthly cap at the provider.
- **Prompt-injection surface exists permanently;** mitigations bound blast radius to advisory metadata but cannot eliminate the class.
- **Second deployable** raises local-stack memory pressure on the 8GB host; triage module must not run concurrently with load benchmarks.

### 5.3 Operational impact
- **Observability (locked signals, with labels):** `triage_llm_latency_seconds` histogram {provider, outcome}; `triage_tool_rounds` histogram; `triage_verdicts_total` {triage_status}; `resilience4j_circuitbreaker_state` + state-transition events {name="llm"}; `triage_consumer_lag` {group}; Spring AI `gen_ai.client.token.usage` {type=input|output} as the cost signal. Alert on fallback-rate rate-of-change, not level.
- **Cost:** triage volume = ~1% of events; at demo throughput, single-digit USD/month under the RateLimiter cap.
- **Security:** key via env only; token counts in metrics, **never** prompt/response bodies in logs or span attributes (PII rule applies — event payloads contain account data).

---

## 6. Failure Modes

### 6.1 Provider degradation (latency, not errors)
- **Trigger:** p99 LLM latency climbs; calls succeed slowly.
- **Blast radius:** triage lag grows; core delivery unaffected.
- **Detection:** CB slow-call rate ≥ 50%; `triage_consumer_lag` rising.
- **Mitigation:** CB opens automatically ⇒ degraded verdicts; lag drains at fallback speed. This is why the CB is slow-call-rate based — error-rate-only CBs are blind to this mode.

### 6.2 Prompt injection via event fields
- **Trigger:** crafted merchant/memo text attempts instruction override.
- **Blast radius:** worst case = one wrong advisory verdict; cannot alter `alert_id`, delivery, or detection.
- **Detection:** schema-validation failure counter; evidence-reference validator drop counter.
- **Mitigation:** fenced data framing + structured-output rejection + evidence-ID validation (Decision §8); failed parse routes to degraded verdict, not retry.

### 6.3 Cost runaway
- **Trigger:** alert storm (upstream bug or replay) multiplies LLM calls.
- **Blast radius:** billing, provider rate-limit bans.
- **Detection:** `gen_ai.client.token.usage` rate; RateLimiter rejection counter.
- **Mitigation:** RateLimiter rejects ⇒ degraded verdicts; inbox dedup makes replays free; provider-side hard cap as the last line.

### 6.4 Central assumption wrong — LLM triage adds no analyst value over static severity mapping
- **Trigger:** narratives are generic; severity correlates 1:1 with rule type.
- **Blast radius:** wasted spend and complexity; no correctness impact.
- **Detection:** manual review of sampled verdicts vs. degraded baseline (no automated signal — synthetic data, no labels).
- **Mitigation:** demote to Phase-1 demo scope; pivot effort to the deterministic adjudicator path (Alternatives, row 5).

---

## 7. Reversal Cost

**Declared:** LOW. Remove the module, topic, and table; revert Architecture Spec rows. No data migration (advisory data is disposable), no core-pipeline change, no external contract.

---

## 8. Validating Assumptions

| # | Assumption | Status | What would invalidate it |
|---|---|---|---|
| 1 | Spring AI structured output reliably binds to records at low temperature with current providers | UNVERIFIED — verify against Spring AI release notes notes at implementation time (§4 of operating contract) | High parse-failure rate ⇒ add repair-reprompt step or reconsider framework |
| 2 | `ChatClient` blocking call on a VT does not pin the carrier | INFERRED — Spring AI 2.0's `OpenAiChatModel` runs on the official OpenAI Java SDK (OkHttp/Okio), **not** Spring `RestClient` (see `LlmResilienceConfig.isRetryableLlmException` — the retry rewrite established this); profile the OkHttp dispatcher via JFR/`tracePinnedThreads` in dev | Pinning observed ⇒ move LLM calls to a small dedicated platform pool (same remedy as JDBC) |
| 3 | 3 tool rounds suffice for useful triage | INFERRED | `triage_tool_rounds` histogram saturating at cap with low-confidence verdicts |
| 4 | ~1% alert rate holds, keeping cost trivial | VERIFIED — lab generator config | Alert-storm scenarios; covered by §6.3 |

---

## 9. Implementation Notes

- **Module:** `rpe-triage-agent/` — own `pom.xml`, port 8081, own Dockerfile; compose profile `triage`.
- **Anchors:** `TriageAgent` (ChatClient orchestration), `TriageTools` (`@Tool` methods), `TriageVerdict` record (output schema), `EvidenceValidator`, `DegradedTriageFallback`, `resilience4j` config under `triage.llm.*`.
- **Order of work:** inbox + consumer + degraded path FIRST (the system must be correct with the LLM permanently absent), then the agent loop, then tools.
- **Docs deltas:** Architecture Spec v1.1 (done), `ADR-15.md` (new).

---

## 10. Open Questions / Followups

- Deterministic weighted-score pre-stage (gray-zone routing) — ADR-16 candidate; deferred behind cert-critical path.
- LLM-as-judge eval harness for verdict quality — deferred; requires labeled data RPE doesn't have.

---

## Changelog

| Date | Version | Author | Change |
|---|---|---|---|
| 2026-07-04 | 1.1.0 | amit | Doc-drift audit correction: flipped Status PROPOSED → ACCEPTED. Implementation completed and has been treated as settled fact by ADR-17 (§Stage 0), ADR-18, ADR-23, ADR-24, Architecture Spec, README.md, and `ADR-15.md` since 2026-06-17 — the status field was the only place still saying otherwise. Removed the dangling `arch-decisions-log.md` cross-reference (file does not exist in this repo). |
| 2026-06-12 | 1.0.0 | amit | Initial draft — status PROPOSED |
