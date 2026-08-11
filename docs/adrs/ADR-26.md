<!-- edit-log (newest first): v1.0 | 2026-07-06 | Initial. ACCEPTED. -->

---
asset_id: adr-26-detection-ack-cb-redrive-correctness
asset_path: docs/adrs/ADR-26.md
asset_type: adr
version: 1.0.0
created: 2026-07-06
last_updated: 2026-07-06
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [kafka, async-acks, offset-commit, circuit-breaker, resilience4j, dlt, re-drive, correctness, detection, adr-02-follow-on, adr-23-follow-on]
---

# ADR-26 — Detection ordering-and-recovery correctness: async acks, CB auto-transition, outcome-aware DLT re-drive

## Status

`ACCEPTED`

Makes true three safety claims that ADR-02/ADR-22/ADR-23 already *asserted* but that were
structurally unsound in the shipped code. An EADIE audit (2026-07-06) found and fixed the gaps; this
ADR records the reasoning. Related: ADR-02 (the C+D Redis CB fallback whose "buffer loss ≠ data loss"
this makes real), ADR-07 (the per-account lane `ThreadPoolExecutor` whose completion-order acks force
this), ADR-09 (rebalance drain — the per-partition analogue), ADR-12 (the ≤50ms async-enqueue crash
window this bounds), ADR-13 (the deterministic UUIDv5 `alert_id` the re-drive reconstruction reuses),
ADR-14 (the frozen gate step-order — dedup-last — that makes a re-driven record dedup-blocked),
ADR-18/ADR-23 (per-writer DLT ownership + the operator-gated re-drive this makes outcome-aware),
ADR-22 (the forced-drain "uncommitted offset ⇒ redelivery" claim this underwrites),
`ADR-24.md` (§Circuit Breaker Fallback + §Dedup + Delivery),
`ADR-23.md` (the re-drive decision tree).

## Context

RPE's entire crash-safety story rests on one sentence, repeated across ADR-02, ADR-12, ADR-22 and
`kafka-consumer.md`: **"an uncommitted Kafka offset means the event is redelivered on restart, so
losing an in-flight/buffered event is not data loss."** The audit found that sentence was **not true**
as built, for three independent reasons.

**1. Out-of-order acks silently advanced the committed watermark.** The payment container runs
`AckMode.MANUAL_IMMEDIATE`, and acks are issued from **per-account lane virtual threads** (ADR-07),
which complete in **wall-clock/completion order, not Kafka offset order** — a slow lane (a rate-limit
park, outbox backpressure, CB buffering) lets a *later* offset ack *before* an earlier one still
in-flight. The Spring Kafka reference is explicit that this is unsafe: with manual acks "the
acknowledgments must be acknowledged **in order**, because Kafka does not maintain state for each
record, only a committed offset for each group/partition." Kafka's committed offset is a single
high-water mark; acking offset *N+5* while *N* is still on a parked lane advances the mark **past** the
in-flight *N*. A crash in that window loses *N* **silently and permanently** — no DLT record, no
redelivery, no metric. Every "uncommitted offset ⇒ redelivery" claim in the repo was quietly false
whenever lane completion reordered acks.

**2. The Redis CB and its fallback waited for each other forever.** The C+D fallback (ADR-02) reacts
to the `redis` breaker opening by **buffering every event without calling the gate** — deliberately,
so a Redis outage stops hammering Redis. But Resilience4j's OPEN→HALF_OPEN transition is **lazy by
default**: "the transition to HALF_OPEN only happens if a call is made, even after
`waitDurationInOpenState` is passed." A traffic-stopping fallback plus a call-triggered transition is a
**mutual wait**: the breaker waits for a call to move to HALF_OPEN; the fallback waits for the
OPEN→HALF_OPEN event to start draining. Nothing breaks the cycle. Detection wedges **permanently** —
the buffer fills, the handler transitions to PAUSED, the consumer is paused, and zero probes ever run
again; **Redis recovering does not un-wedge it.** The existing `CbFallbackHandlerTest` masked the bug
by calling `transitionToHalfOpenState()` manually — a call nothing in production ever issues.

**2b. The old fallback state machine had two proven races** even once transitions could fire. It
choreographed NORMAL/BUFFERING/PAUSED/DRAINING across three threads (the Kafka listener, the R4j event
publisher, the drain thread) with lone CAS operations: (i) a task offered in the window between the
drain's final empty poll and its DRAINING→NORMAL flip was **stranded until the next CB cycle —
potentially forever**; (ii) an R4j `HALF_OPEN_TO_CLOSED` event could flip the handler to NORMAL
**mid-drain with a non-empty buffer**, letting fresh events for an account overtake older buffered
events for the *same* account — inverting per-account state-mutation order (stale geo re-applied after
newer state), the one ordering guarantee the lane exists to provide.

**3. A post-gate alert failure was lost on re-drive.** The gate's step 4 (dedup `SET NX`) runs
**last** and **before Java sees the result** (ADR-14, immutable dedup-last). So when a detector fires
but the alert intent cannot be durably enqueued (outbox saturated past its backpressure window —
sustained Postgres outage), the event reaches `payment.events.DLT` **with its dedup key already
marked**. A later operator re-drive (ADR-23) is therefore **dedup-blocked**: the detectors never
re-run, and past the dedup TTL the velocity window has moved so the original rule would not re-fire
anyway. Preserving the *event* on the DLT preserved nothing recoverable — the **alert** was silently
gone on re-drive. Compounding it, `deploy/kafka/dlt-redrive.sh` **dropped all record headers** on
re-produce despite a comment claiming byte-for-byte preservation, so even a header-carried outcome
would not have survived the round-trip.

## Decision

Three changes to `rpe-detection-service`, each pinned by a test that documents the failure mode it
prevents. No topic, schema, or alert-contract change.

**1. `asyncAcks=true` on the payment listener container (KafkaConfig) — load-bearing, not tuning.**
With `MANUAL_IMMEDIATE` + `asyncAcks`, the container **defers** an out-of-order ack until the offset
gap before it fills, then commits the contiguous prefix — so the committed watermark can never pass an
in-flight earlier event. This is exactly what restores "uncommitted offset ⇒ redelivery": a crash now
redelivers from the lowest un-acked offset, and buffered/parked/in-flight events genuinely survive.
Cost: **more duplicate deliveries** after a failure (the whole gap replays), absorbed by the gate's
dedup pre-check (deterministic per-event dedup key) and the deterministic `alert_id` at the alert
boundary. Pinned by `KafkaConfigAckPropertiesTest`, in the same spirit as `DetectorPrecedenceTest`
pins detector order — a correctness contract, not a knob.

**2. `automaticTransitionFromOpenToHalfOpenEnabled: true` on the `redis` breaker (application.yml),
plus a race-hardened `CbFallbackHandler`.** The automatic transition makes OPEN→HALF_OPEN fire **on
schedule with zero traffic** — precisely the traffic pattern the fallback produces — which fires the
`OPEN_TO_HALF_OPEN` event that starts the drain, breaking the mutual wait. The handler was rewritten so
transitions are correct under concurrency:
- `dispatch(accountId, task)` is the **single** lane-vs-buffer routing decision. The NORMAL fast path
  is a lock-free volatile read; **every other decision re-checks state under one `transitionLock`**,
  so an offer can never interleave with the NORMAL flip (kills race i).
- The **DRAINING→NORMAL flip has exactly one owner — the drain thread** — and fires only when the
  buffer is observed **empty AND the breaker CLOSED**, both under the lock. A `HALF_OPEN_TO_CLOSED`
  event mid-drain no longer short-circuits the drain (kills race ii).
- While the breaker is HALF_OPEN the drain **trickles at most the breaker's permitted probe count**
  per pacing interval rather than flooding — flooding would bounce `(buffer − permits)` tasks straight
  back through `CallNotPermittedException` → `dispatch` → buffer, a churn storm on every recovery. The
  consumer is paused only on BUFFERING→PAUSED and resumed only on DRAINING→NORMAL (resume on an
  un-paused container is a harmless no-op, so the pairing stays balanced on every path).
Pinned by `RedisCircuitBreakerAutoTransitionTest` (the config flag + the zero-traffic HALF_OPEN
mechanism it relies on).

**3. Outcome-aware DLT records + a re-drive reconstruction path.** A post-gate "fired but not durable"
failure now carries the detection outcome to the DLT as headers, and a re-driven copy of such a record
is reconstructed straight to the outbox:
- `AlertNotDurableException(ruleName, reason)` is thrown when a detector fired but the outbox enqueue
  returned "saturated". The `DeadLetterPublishingRecoverer.headersFunction` stamps
  `x-rpe-outcome=ALERT_UNDURABLE`, `x-rpe-rule=<ruleName>`, `x-rpe-reason=<reason>` on the DLT record,
  making it **self-sufficient**. (`reason` rides the header, never the exception *message* — the
  message carries the rule name only, per security.md fingerprint-PII discipline.)
- On consume, `PaymentEventConsumer.handleRedrivenDuplicate` detects a re-driven record (the
  `x-redrive-attempts` header the script stamps, ADR-23) that is dedup-blocked. If it carries
  `ALERT_UNDURABLE` + a rule, the alert is reconstructed **deterministically** —
  `UUIDv5(eventId + ":" + rule)`, the identical `alert_id` (ADR-13) — and submitted **straight to the
  outbox**: no gate re-run, no velocity/Welford/geo pollution; `processed_alerts ON CONFLICT` absorbs
  the duplicate if the original intent had in fact reached Postgres. Still-saturated ⇒ back to the DLT
  (headers re-stamped). A re-driven dedup-blocked record with **no** reconstructable outcome is
  **fail-visible** — returned to the DLT so the attempt cap parks it for an operator (ADR-23), never a
  silent ack that pretends it was handled. Metrics: `rpe.redrive.reconstructed{outcome}`,
  `rpe.redrive.unrecoverable`.
- `deploy/kafka/dlt-redrive.sh` now **re-emits the original record headers** on re-produce (all
  `kafka_dlt-*` forensic headers and the `x-rpe-*` outcome headers), replacing only the `x-redrive-*`
  stamps — its previous "preserved byte-for-byte" comment was false. Pinned by
  `RedriveReconstructionTest`.

## Alternatives Considered

| Option | Decision | Reason |
|---|---|---|
| Keep per-record sync acks (no `asyncAcks`) | Rejected | Lane completion reorders acks vs. offsets; the committed watermark passes in-flight events → silent permanent loss on crash. The bug this fixes. |
| Force offset-ordered ack by serialising lane completion in offset order | Rejected | Reintroduces head-of-line blocking across accounts and destroys the per-account parallelism the lane model exists for — one slow account would stall commit for all. `asyncAcks` gets ordered *commit* without ordered *completion*. |
| Tickle the lazy CB with a synthetic health-probe call on a timer | Rejected | Reinvents R4j's own `automaticTransitionFromOpenToHalfOpen`, adds a fake call path, and still races the fallback's state. Use the built-in transition. |
| Keep the CAS-choreographed fallback (add more CAS) | Rejected | The two races are inherent to lock-free multi-owner transitions here. A single `transitionLock` (with a lock-free NORMAL fast path + under-lock re-check) is simpler and provably strands nothing. |
| Re-run the gate/detectors on a re-driven record | Rejected | Impossible and unsafe: the record is dedup-blocked (gate step 4 already marked it), and re-running would mutate velocity/Welford/geo — violating "a throttled/breached/replayed event must not mutate state". Past the TTL the velocity window has moved, so the rule would not re-fire regardless. |
| DLT the undurable event with no outcome headers (pre-fix behaviour) | Rejected | The alert is silently lost on re-drive (dedup-blocked, detectors skipped). Preserving the event without the outcome preserves nothing actionable. |
| Random `alert_id` on reconstruction | Rejected | Breaks `processed_alerts` dedup and replay safety permanently (ADR-13). The whole point is that UUIDv5 makes the reconstructed `alert_id` bit-identical to the original. |
| Silently ack a re-driven dedup-blocked record with no outcome | Rejected | Indistinguishable from "handled"; hides a real gap. Fail-visible → park → operator decision (ADR-23) is the correct terminal. |

## Consequences

**Positive:** the repo's documented crash-safety model is now **structurally true**, not aspirational.
"Uncommitted offset ⇒ redelivery" (ADR-02 buffer, ADR-22 forced drain) holds under lane completion
reordering; "**buffer loss on crash ≠ data loss**" (ADR-02) is real because the un-acked gap holds the
partition watermark; and the SIGKILL safety-net (ADR-22 — uncommitted offsets + Redis dedup +
deterministic `alert_id` + `processed_alerts ON CONFLICT`) actually carries correctness, because the
"uncommitted offsets" leg now behaves. The Redis CB can no longer wedge detection permanently, and its
recovery preserves per-account ordering. A post-gate alert failure now survives an operator re-drive
with the **same** deterministic `alert_id`, closing the last silent-loss gap in the DLT path. All three
are pinned (`KafkaConfigAckPropertiesTest`, `RedisCircuitBreakerAutoTransitionTest`,
`RedriveReconstructionTest`) with every suite green (detection **91**, relay 5, alert 10, triage 31).

**Negative:** `asyncAcks` **widens duplicate delivery** after any failure — the whole un-acked offset
gap replays, so more events hit the dedup pre-check on recovery (absorbed, but not free), and the
committed watermark now lags behind the *slowest in-flight* lane, so `rpe.consumer.lag` reflects
deferred-commit position rather than true backlog and must be read alongside `rpe.cb.buffer.size` and
lane health. The `CbFallbackHandler` is more code than the old CAS version (a lock + a drain-owned
flip + probe pacing). The re-drive reconstruction adds a header-carrying exception and a reconstruction
branch on the consume path, and its correctness depends on the **operator script** re-emitting headers
— a script regression would silently break reconstruction, and the script sits **outside** `mvn verify`
(ADR-23 R1), mitigated only by `RedriveReconstructionTest` (the in-process half) plus the header
re-emit now being present.

## Residual Risks (explicit)

- **R1 — Wider duplicate delivery meets best-effort dedup.** Redis dedup is bounded by TTL and Redis
  uptime (ADR-13); a duplicate redelivered *past* the dedup TTL re-applies Welford/geo (bounded,
  self-healing drift — ADR-09). `asyncAcks` makes post-failure replays larger, so this pre-existing
  limitation is hit somewhat more often. Accepted: bounded drift ≪ silent loss.
- **R2 — Consumer-lag optics.** A single stuck lane holds the committed offset back, so
  `rpe.consumer.lag` can look large while real backlog is small. Alerting on lag alone will
  false-positive during a slow-lane episode; correlate with buffer size and lane liveness.
- **R3 — Recovery drains at probe rate by design.** After a long Redis outage the buffer drains at the
  breaker's permitted-probe rate (pacing interval 50ms) until the breaker CLOSES, then floods. A very
  large buffer therefore drains slowly at first — a throughput cost during recovery, never loss.
- **R4 — Reconstruction depends on headers surviving the DLT round-trip.** If any tool in the DLT path
  strips headers (or a record was parked *before* this fix and carries none), reconstruction cannot
  fire and the record parks — fail-visible, but it turns an automatic recovery into an operator
  decision. The script's header re-emit closes the in-repo path; external tooling is out of scope.
- **R5 — Reconstruction is scoped to `ALERT_UNDURABLE`.** Any *other* post-gate failure that reaches
  the DLT and is later re-driven while dedup-blocked has no reconstructable outcome and parks. For a
  record that never fired an alert this is conservative (nothing was lost), but re-driving such records
  always requires an operator to discard them from parking. Widening reconstruction to other verdicts
  is a future change, not this ADR.

## Reversal Cost

`LOW` mechanically, **but do not** — each revert reintroduces the exact silent defect its test pins.
Removing `asyncAcks=true` reinstates out-of-order-ack silent loss (`KafkaConfigAckPropertiesTest`
fails). Removing `automaticTransitionFromOpenToHalfOpenEnabled: true` reinstates the permanent CB wedge
(`RedisCircuitBreakerAutoTransitionTest` fails). Reverting `CbFallbackHandler` to the CAS version
reinstates the strand + ordering-inversion races. Dropping the outcome headers / reconstruction path
reinstates silent alert loss on re-drive (`RedriveReconstructionTest` fails). Treat items 1–2 as
Immutable Constraints in review.

## References

- `ADR-24.md` §Circuit Breaker Fallback + §Dedup + Delivery — enforcement detail
- `ADR-23.md` — the outcome-header-aware `payment.events` re-drive path
- `rpe-detection-service` — `config/KafkaConfig` (`setAsyncAcks(true)`, DLT `headersFunction`),
  `consumer/CbFallbackHandler` (`dispatch`, drain-owned flip, probe trickle),
  `consumer/AlertNotDurableException` (outcome headers), `consumer/PaymentEventConsumer`
  (`handleRedrivenDuplicate`), `src/main/resources/application.yml` (`redis` breaker flag)
- `KafkaConfigAckPropertiesTest`, `RedisCircuitBreakerAutoTransitionTest`, `RedriveReconstructionTest`
  — the three pins
- `deploy/kafka/dlt-redrive.sh` — now re-emits original headers on re-produce
- ADR-02 (CB fallback), ADR-07 (lane executor), ADR-12 (async-enqueue window), ADR-13 (UUIDv5
  `alert_id`), ADR-14 (gate dedup-last), ADR-18/ADR-23 (DLT ownership + re-drive), ADR-22 (shutdown
  drain safety-net)
