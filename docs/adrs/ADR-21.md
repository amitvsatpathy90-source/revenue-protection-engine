<!-- edit-log (newest first): v1.0 | 2026-06-29 | Initial. ACCEPTED. -->

---
asset_id: adr-21-exception-boundary-discipline
asset_path: docs/adrs/ADR-21.md
asset_type: adr
version: 1.0.0
created: 2026-06-29
last_updated: 2026-06-29
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [error-handling, archunit, code-quality, boundary-handler, exceptions]
---

# ADR-21 — Exception-boundary discipline: narrow by default, broad catch only in `@BoundaryHandler`, ArchUnit-enforced

## Status

`ACCEPTED`

Supersedes: nothing.
Related: `spring-boot-4.md` (Jackson 3 `JacksonException` is a `RuntimeException`),
`reactive-pipeline.md` (`subscribe` onError ArchUnit rule), ADR-02 (CB fallback / DLT routing),
ADR-15 (triage degraded fallback), Reactive Resilience Architecture.

## Context

An audit found ~20 `catch (Exception e)` sites across the four services. A literal "no broad catch"
ban is wrong for a resilient event system: a `@KafkaListener`'s outermost frame, a background relay
loop, a scheduled purge, an observability poller, and a read-only `@Tool` **must** catch broadly or a
single failure kills the execution unit. The real defect is that the codebase could not distinguish a
deliberate last-line-of-defense from a lazy catch, and nothing prevented a future broad catch from
landing in domain logic (detection, agent reasoning) where a specific catch belongs.

## Decision

A **two-tier, mechanically-enforced policy** (full detail in `error-boundaries.md`):

1. **Narrow by default — the ladder:** catch the specific exception → else `RuntimeException` (the
   allowed narrower rung for unchecked-only `try` blocks: reactive `.block()`, Jackson 3, JDBC runtime
   wrappers) → else `catch (Exception)` which is permitted **only** inside a `@BoundaryHandler` code unit.
2. **`@BoundaryHandler(String why)`** marks a method or class whose broad catch is a deliberate boundary;
   the `value()` documents why. Per-service copy (no shared jar — ADR-17 non-goal #2), like `Pii`.
3. **ArchUnit enforcement** (`broadCatchOnlyInBoundaryHandlers` in each `*ArchTest`): inspects
   `JavaCodeUnit.getTryCatchBlocks()` and fails the build on any `catch (java.lang.Exception)` outside a
   `@BoundaryHandler`. Joins the existing silent-failure guards (`subscribe` onError, no-`synchronized`,
   no-`randomUUID`, no-cross-service-import).

**Applied:** 8 sites narrowed (JDBC → `SQLException`; reactive/converter → `RuntimeException`; Jackson 2 →
`JsonProcessingException`; future-get → `ExecutionException | TimeoutException`); ~12 genuine boundaries
annotated (Kafka listener, relay/listen loops, scheduled purge/sweep, observability pollers, tool
boundaries, the R4j LLM wrapper, the publish wrappers, the triage message handler).

## Alternatives Considered

| Option | Decision | Reason |
|---|---|---|
| Blanket ban on `catch (Exception)` | Rejected | Breaks the correct resilient-loop pattern; would force fragile rethrow gymnastics at boundaries. |
| Doc-only convention (no enforcement) | Rejected | Conventions rot; the repo's culture is ArchUnit-enforced. A future broad catch must fail the build. |
| Annotate every broad catch (skip narrowing) | Rejected | The annotation becomes wallpaper; narrowing where a known throw surface exists keeps the broad-catch surface genuinely minimal and the annotation meaningful. |
| Flag `Throwable` too | Rejected | Try-with-resources desugars to a synthetic `catch (Throwable)` indistinguishable via ArchUnit; flagging it produces unavoidable false positives. Target `Exception` only. |

## Consequences

**Positive:** broad catches in domain logic are now a build failure; every remaining broad catch is a
deliberate, greppable, reason-documented boundary; slots into existing per-service ArchUnit guards;
unchanged behavior (all suites green: detection 76, relay 5, alert 10, triage 28).
**Negative:** ~12 annotations + 4 rule copies to maintain; the rule checks breadth, not swallow-quality.

## Residual Risks (explicit)

- **R1 — Swallow-quality not machine-checked.** A narrow catch that silently ignores is still possible;
  the rule checks catch *breadth* only. Mitigation: `@BoundaryHandler` reason strings + review; the
  `subscribe` onError rule covers the reactive variant.
- **R2 — `@BoundaryHandler` over-application.** A vague reason could rubber-stamp a lazy catch.
  Mitigation: mandatory `value()`, code review, `grep -rn @BoundaryHandler` audit.
- **R-twr — Hand-written `catch (Throwable)` is not flagged** (try-with-resources synthetic-handler
  collision). Mitigation: none exist today; review. See `error-boundaries.md`.
- **R3 — Lambda attribution.** A broad catch inside a lambda is attributed to the enclosing code unit's
  annotation, which is the intended (correct) behavior; verified green.

## Reversal Cost

`LOW` — delete the rule from the four `*ArchTest`s and the `@BoundaryHandler` annotations; the narrowed
catches are strictly-better standalone improvements and would stay.

## References

- Reactive Resilience Architecture — the ladder, boundary catalog, enforcement detail
- `spring-boot-4.md` — Jackson 3 `JacksonException` is unchecked
- ArchUnit `TryCatchBlock.getCaughtThrowables()` (1.3.0) — the enforcement primitive
