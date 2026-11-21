---
asset_id: adr-32-per-alert-time-budget-vs-shutdown-windows
asset_path: docs/adrs/ADR-32.md
asset_type: adr
version: 1.0.0
created: 2026-11-21
last_updated: 2026-11-21
status: PROPOSED
reversal_cost: LOW
owner: amit
tags: [ triage, graceful-shutdown, kubernetes, timelimiter, tool-rounds, lab-measured ]
---

# ADR-32 — Worst-case per-alert time vs. the triage shutdown windows

## Status

`PROPOSED` — flips to `ACCEPTED` when the windows below are applied and verified at the k8s T0 gate.
Related: ADR-22 (graceful shutdown; risk R4), ADR-15 §3.3 (tool-round cap), ADR-31.

## Context

ADR-22 sized triage's shutdown windows above a single 12s LLM TimeLimiter. The agent loop can make up to
`MAX_TOOL_ROUNDS + 1` = 4 LLM calls per alert, each bounded by `triage.llm.time-limit-ms` (12s default, retries and
backoff included), after one RAG lookup whose padded ceiling is 5s.

| Quantity (default profile)                    | Value                       |
|-----------------------------------------------|-----------------------------|
| Worst-case LLM time                           | 4 × 12s = 48s               |
| Worst-case in-flight triage (RAG + LLM)       | 5s + 48s = 53s              |
| `spring.lifecycle.timeout-per-shutdown-phase` | 25s (sized for 5s + 12s)    |
| k8s `terminationGracePeriodSeconds`           | 45s, of which preStop is 5s |

The worst case exceeds both windows. ADR-22 R4 names this class of drift: a component deadline above the phase timeout
or the grace period lets SIGKILL pre-empt an in-flight triage. No pre-emption has been observed; this is derived from
configuration. The window values were read from `application.yml` and `33-triage.yaml`.

`ollama-local` (dev, no k8s): 45s per call × 2 calls (`max-tool-rounds: 1`) = 90s. Cold-start model-load time was not
measured; one observed two-call smoke alert took about 13s.

## Decision (proposed)

Re-derive the windows from the worst case and keep the ADR-15 three-round cap:

| Window                          | Current | Proposed                                  |
|---------------------------------|---------|-------------------------------------------|
| `timeout-per-shutdown-phase`    | 25s     | 60s (above the 53s worst case)            |
| `terminationGracePeriodSeconds` | 45s     | 75s (preStop 5s + phase 60s + 10s margin) |

Keep `timeout-per-shutdown-phase` below the k8s grace period (ADR-22 R4).

## Alternatives Considered

| Option                                                                          | Outcome                                                                                                        |
|---------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| Aggregate per-alert deadline in `TriageAgent` (budget checked before each call) | More code and tests to bound a worst case never observed. Revisit if real latency shows long multi-call tails. |
| Lower the default `max-tool-rounds` to 1                                        | Worst case still 2 × 12 + 5 = 29s (above 25s), and it changes the ADR-15 behavior on OpenAI. Not adopted.      |
| Accept the risk                                                                 | Rejected as the default: the fix is three numbers.                                                             |

## Consequences

- A pod with an in-flight alert at the worst case takes up to 75s to terminate; normal shutdowns are unaffected.
- Not verified: behavior when SIGKILL pre-empts a triage. `PendingTriageSweep` exists for stale pending rows; its
  production staleness value was not re-checked here.
- Windows are tuned constants that must stay in sync across `application.yml`, `33-triage.yaml` and ADR-22.

## Reversal Cost

LOW. Revert two numbers and their comments.

## Changelog

| Version | Date       | Author | Description                                                                     |
|---------|------------|--------|---------------------------------------------------------------------------------|
| 1.0.0   | 2026-11-21 | @amit  | Initial — PROPOSED. Records the worst-case budget gap and the proposed windows. |
