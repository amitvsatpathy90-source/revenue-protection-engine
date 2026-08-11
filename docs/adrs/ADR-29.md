<!-- edit-log (newest first): v1.0 | 2026-07-15 | Initial. ACCEPTED — backfill documenting a convention enforced since ADR-01 (2026-05-20) that never had its own ADR. -->

---
asset_id: adr-29-no-global-resilience4j-defaults
asset_path: docs/adrs/ADR-29.md
asset_type: adr
version: 1.0.0
created: 2026-07-15
last_updated: 2026-07-15
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [resilience4j, circuit-breaker, bulkhead, rate-limiter, retry, timelimiter, configuration, blast-radius]
---

# ADR-29 — No global Resilience4j defaults: every boundary instance is configured independently

## Status

`ACCEPTED`

Supersedes: nothing — formalizes a convention already in force since `ADR-01` (2026-05-20). Related:
ADR-15 §3.6 (the LLM R4j boundary — TimeLimiter/CB/Bulkhead/RateLimiter/Retry, the richest instance
set), ADR-17 §3.3 / non-goal #2 (no shared business-code module — the sibling principle this
generalizes to configuration), ADR-24 §4 (the canonical example: the `ratelimit` CB is deliberately
kept separate from the gate's `redis` CB so one boundary's trouble can't open the other's breaker),
`ADR-15.md` §5 (states "no global defaults" as binding for the LLM boundary).

---

## Context

RPE uses Resilience4j across all four services for multiple, architecturally distinct boundaries: the
Redis gate circuit breaker + bulkhead (detection), the distributed rate-limiter circuit breaker +
bulkhead (detection, `ADR-24`, deliberately separate from the gate's breaker), the Postgres bulkhead
(detection), and the full LLM boundary stack — TimeLimiter, CircuitBreaker, Bulkhead, RateLimiter,
Retry (triage, `ADR-15`). These boundaries have materially different failure signatures: a Redis blip
recovers in milliseconds and should trip on a tight sliding window (detection's `redis` instance:
`slidingWindowSize: 10`, `waitDurationInOpenState: 10s`), while an LLM call's slow-call profile needs
an 8s slow-call threshold and a 12s hard `TimeLimiter` cap. Resilience4j's Spring Boot integration
supports a shared `resilience4j.*.configs` template that named `instances:` can inherit from via
`base-config` — precisely the mechanism that would let one boundary's tuning silently leak into
another's, or let a new boundary inherit a threshold tuned for an unrelated failure mode. "No global
defaults" has been stated as a rule since `ADR-01` and is referenced by name in Architecture Spec's Stack
section and `ai-triage-rules.md` §5, but never had its own ADR — this backfills that gap and records
the verification behind it.

## Decision

Every Resilience4j instance (CircuitBreaker, Bulkhead, RateLimiter, Retry, TimeLimiter) across all
four services is configured fully and independently — confirmed by repo-wide grep: zero
`resilience4j.*.configs`/`base-config` blocks exist in any `application.yml`, and triage's
Java-based `LlmResilienceConfig` builds every registry via `.custom()`, never Resilience4j's
`.ofDefaults()`. A new boundary is always a new, fully-specified instance; it never inherits or
overrides a shared baseline.

## Alternatives Considered

- **A shared `configs.default` block with per-instance overrides for what differs** — rejected.
  Creates implicit coupling: a change to the shared default silently changes every instance that
  didn't explicitly override it — exactly the blast-radius leak the `redis`/`ratelimit` CB split in
  `ADR-24` exists to prevent between just those two instances, let alone across all boundaries.
- **Per-service (not per-boundary) shared config** — e.g. one detection-wide circuit-breaker
  template — rejected. Detection alone spans boundaries with materially different failure
  signatures (sub-100ms Redis vs. the Postgres bulkhead vs. the token-bucket rate limiter); a
  per-service default would need per-instance overrides for most fields immediately, buying no real
  simplification while reintroducing the same silent-leak risk.
- **A shared `rpe-common` Resilience4j config module** — rejected as a special case of the broader
  no-shared-business-code-module non-goal (`ADR-17` §3.3 / non-goal #2). Would recouple services at
  compile time for configuration that should stay scoped per-boundary regardless.

## Consequences

**Positive:**
- No instance can silently inherit another's threshold — every circuit breaker/bulkhead/limiter's
  configuration is visible in one place and fully self-describing.
- Adding a new boundary forces an explicit decision (sliding window, failure threshold, wait
  duration) instead of a silent default that may be wrong for that boundary's actual failure profile.
- Verified structurally, not just by convention: zero `configs:`/`base-config:` blocks exist anywhere
  in the repo today, so there is nothing to accidentally inherit from even by mistake.

**Negative:**
- More verbose configuration — every instance fully specified, no compression via a shared baseline;
  detection's `resilience4j` block alone runs to roughly 35 lines for 5 instances.
- No structural (ArchUnit or CI) enforcement that a future PR can't introduce a
  `configs.default`/`base-config` block — the invariant holds today by convention plus this ADR, not
  by a build-time guard the way `broadCatchOnlyInBoundaryHandlers` (`ADR-21`) enforces its own rule.

## Residual Risks (explicit)

- **R1 — No automated guard.** Nothing in `mvn verify` fails the build if a future PR adds a
  `resilience4j.*.configs.default` block or an `.ofDefaults()` call. Mitigation today is this ADR plus
  code review; a lint/ArchUnit-style check is a candidate follow-up, not yet built.
- **R2 — Convention predates formal ADR governance.** This rule has been assumed and enforced since
  `ADR-01` (2026-05-20) but had no ADR of its own until now — any prior deviation, if one ever existed
  before this repo-wide grep, would have had no formal decision to point back to.

## Reversal Cost

`LOW` — introducing a shared default block is a config-only change with no schema, topic, or contract
impact, and reversing it back to independent instances is equally mechanical. The low mechanical cost
is not the reason to keep this decision, though — the failure mode it prevents (a silent blast-radius
leak between architecturally unrelated boundaries) is what would actually be expensive.

## References

- `rpe-detection-service/src/main/resources/application.yml` — the `resilience4j.circuitbreaker.instances`
  / `resilience4j.bulkhead.instances` blocks (`redis`, `ratelimit`, `postgres`), each fully independent.
- `rpe-triage-agent/src/main/java/com/example/rpe/triage/config/LlmResilienceConfig.java` — every
  registry built via `.custom()`.
- Architecture Spec Stack section — "Resilience4j — circuit breaker + bulkhead per outbound boundary; no
  global defaults."
- `ADR-15.md` §5 — "Resilience4j boundary (`triage.llm.*` — no global defaults,
  per Architecture Spec)".
- `ADR-24` §4 — the canonical example: the `ratelimit` CB kept separate from the gate's `redis` CB so
  a rate-limit hiccup can't open the gate breaker.
- `ADR-15` §3.6 — the LLM boundary's full R4j spec (TimeLimiter/CB/Bulkhead/RateLimiter/Retry).
- `ADR-17` §3.3 / non-goal #2 — no shared business-code module, the sibling principle this generalizes.

## Changelog

| Date | Version | Author | Change |
|---|---|---|---|
| 2026-07-15 | 1.0.0 | amit | Initial — ACCEPTED. Backfills a convention enforced since ADR-01; verified via repo-wide grep (zero `configs:`/`base-config:` blocks) and inspection of `LlmResilienceConfig.java` (`.custom()` throughout). |
