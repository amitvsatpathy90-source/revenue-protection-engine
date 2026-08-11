<!-- edit-log (newest first): v1.0 | 2026-06-29 | Initial. ACCEPTED. -->

---
asset_id: adr-22-graceful-shutdown
asset_path: docs/adrs/ADR-22.md
asset_type: adr
version: 1.0.0
created: 2026-06-29
last_updated: 2026-06-29
status: ACCEPTED
reversal_cost: MEDIUM
owner: amit
tags: [lifecycle, graceful-shutdown, smartlifecycle, kafka, virtual-threads, observability, kubernetes]
---

# ADR-22 — Coordinated graceful shutdown: SmartLifecycle phase ordering, bounded per-service drains, readiness-flip + observability

## Status

`ACCEPTED`

Supersedes: nothing.
Related: ADR-01 (runtime surface split — each service drains the thread model it owns), ADR-02
(CB buffer loss ≠ data loss; uncommitted offsets redeliver), ADR-05/06 (relay loop + transactional
producer), ADR-09 (partition-scoped lane drain on rebalance — the per-rebalance analogue of this
per-process drain), ADR-12 (50ms outbox loss window), ADR-13 (deterministic alert_id makes replay
safe), ADR-17 (independent deployability), ADR-19 (readiness probe surface),
`ADR-22.md`, `ADR-01.md`.

## Context

An audit of shutdown behavior found **no service had coordinated graceful shutdown**. On `SIGTERM`
each service tore down its threads in an accidental, partially-inverted order:

- `server.shutdown` and `spring.lifecycle.timeout-per-shutdown-phase` were **unset in all four** →
  Spring used immediate (non-graceful) shutdown.
- **Detection** had no global drain of the per-account lanes — the Caffeine `removalListener`
  shuts a lane only on eviction. In-flight lane tasks (Redis gate → detector → outbox submit) were
  abandoned on exit, and the `OutboxBatchWriter`'s `@PreDestroy` flush was **not ordered after** the
  lane drain, so a late lane submit could land in a queue whose flusher had already stopped.
- **Relay** ran its loop as a **raw, unmanaged `Thread.ofVirtual().start()`** launched on
  `ApplicationReadyEvent`. Nothing interrupted it; the Kafka-transactional producer factory (a
  `DisposableBean`) could be torn down *underneath* an in-flight `executeInTransaction`.
- JDBC scheduler pools (daemon threads) were **never shut down** — JDBC could be killed mid-statement.
- **k8s** Deployments set no `terminationGracePeriodSeconds` and no `preStop` — 30s default, abrupt
  SIGKILL, no endpoint-deregistration window.

A literal "wait for everything" is wrong (unbounded), and a single `@PreDestroy` per bean cannot
express the cross-bean **ordering** that loss-free draining requires. The correct shape is a
*coordinated, phase-ordered, bounded* drain: stop ingesting first, then drain in-flight work, then
flush the sink, then close the pools — each step bounded, each step observable.

## Decision

Adopt Spring `SmartLifecycle` **phase ordering** anchored to the Spring Kafka container constant,
plus `server.shutdown: graceful`, bounded per-component deadlines, a readiness flip, and k8s
termination alignment. Full enforcement detail in `lifecycle-shutdown.md`.

**1. Phase ladder.** Spring stops `SmartLifecycle` beans in **descending phase** order. Spring Kafka
containers stop at `AbstractMessageListenerContainer.DEFAULT_PHASE = 2147483547`
(`Integer.MAX_VALUE − 100`, verified against spring-kafka 4.1.0). Our drain beans anchor *below* it:

```
STOP ORDER (high phase → low phase, stopped first → last)
  Integer.MAX_VALUE            web server graceful drain               (framework)
  2147483547  (KAFKA)          Kafka consumer containers stop  ← ingestion ceases
  KAFKA − 10  (DRAIN)          lane drain (detection) / relay-loop join (relay)
  KAFKA − 20  (FLUSH)          outbox final flush (detection)
  …bean destruction            JDBC/lane pools shutdown; producer factory close
```

This makes the invariant **stop ingest → drain in-flight → flush sink → close pools** *structural*,
and fixes the flush-before-drain race by construction (FLUSH stops strictly after DRAIN).

**2. Per-service bounded drains.**
- *Detection:* `LaneDrainLifecycle` (DRAIN, 5s) drains every live lane preserving per-account order
  (single-threaded lanes finish their queued tasks before terminating). `OutboxBatchWriter` becomes
  a `SmartLifecycle` (FLUSH, 5s) that drain-flushes the whole queue with a **no-progress guard** (a
  Postgres outage re-queues → flat size → break early rather than busy-spin). `SchedulerConfig`
  shuts the JDBC/lane pools at `@PreDestroy` (after the flush).
- *Relay:* `OutboxRelay` becomes a `SmartLifecycle` with a **two-phase stop** (10s total): join
  briefly so an in-flight batch finishes and the loop exits at its `running` guard, then interrupt an
  idle poll-park and join the remainder — the producer factory is never closed mid-transaction.
- *Alert / Triage:* already synchronous listeners (the Kafka container's `stop()` awaits the
  in-flight `consume()`/`process()`); they need only a sized grace window. Triage's window is set
  above the 12s LLM TimeLimiter so an in-flight triage completes.

**3. Readiness flip + endpoint deregistration.** A `ShutdownLifecycleListener` (per-service copy)
flips readiness to `REFUSING_TRAFFIC` on `ContextClosedEvent` (the earliest shutdown hook, before any
`stop()`), so k8s removes the pod from Service endpoints before the drain. A k8s `preStop` sleep (5s)
gives the endpoints controller time to act ahead of the SIGTERM-driven drain. Liveness stays UP.

**4. Config + k8s.** `server.shutdown: graceful` + `spring.lifecycle.timeout-per-shutdown-phase`
(per-phase upper guard; component deadlines are smaller) on all four. k8s
`terminationGracePeriodSeconds` (detection 60s, others 45s) sized above `preStop` + the realistic
drain total.

**5. Observability (production-grade).** `rpe.shutdown.initiated`,
`rpe.shutdown.drain.duration{component}` (timer), `rpe.shutdown.tasks.drained{component}`, and
`rpe.shutdown.forced{component}` (incremented when a bounded await times out = work abandoned), plus
structured drain logs. A forced drain is loud, not silent.

## Alternatives Considered

| Option | Decision | Reason |
|---|---|---|
| Leave immediate shutdown (rely on uncommitted offsets + dedup) | Rejected | Correct for *replay safety* but loses the in-flight outbox flush window unnecessarily and tears the relay tx producer down mid-commit; ops gets no drain signal. |
| One `@PreDestroy` per bean | Rejected | `@PreDestroy` ordering follows bean-dependency, which cannot express "flush only after lanes drain" or "stop the loop before the producer factory dies". The flush-before-drain race is a `@PreDestroy` artifact. |
| `@PreDestroy`-only, no phases; unbounded waits | Rejected | Unbounded drain blocks SIGKILL; a single hung lane/LLM call stalls the whole shutdown. |
| Hardcode literal phase numbers | Rejected | Reference `AbstractMessageListenerContainer.DEFAULT_PHASE` so the ordering survives a Spring Kafka version bump that moves the constant. |
| Keep the relay loop as a raw thread, add a shutdown hook | Rejected | A `Runtime` hook runs outside the Spring lifecycle, after the producer factory may already be destroyed — exactly the bug. A `SmartLifecycle` stops *during* the lifecycle, before bean destruction. |

## Consequences

**Positive:** zero avoidable in-flight loss on rolling deploys (lanes drain in order; outbox flushes;
relay finishes its batch); the tx producer is never closed mid-commit; the drain is bounded (never
blocks SIGKILL) and fully observable; readiness-flip + preStop give clean LB deregistration; the
ordering invariant is structural, not convention. All suites green unchanged (detection 76, relay 5,
alert 10, triage 28 — every integration test boots the new shutdown beans and tears down cleanly).

**Negative:** four `SmartLifecycle`/listener copies + per-service config to maintain (consistent with
the no-shared-jar rule); `OutboxBatchWriter` and `OutboxRelay` gained a lifecycle contract (more
surface than a bare bean); grace windows and phase deltas are tuned constants that must stay in sync
with k8s `terminationGracePeriodSeconds`.

## Residual Risks (explicit)

- **R1 — Sub-millisecond async-enqueue window (ADR-12, unchanged).** A lane task acks the Kafka offset,
  then dispatches `outboxWriter.submit()` asynchronously on `jdbcScheduler`. The lane-drain-before-flush
  ordering catches everything already enqueued; an enqueue still in the async hop at the instant of
  FLUSH is the existing ADR-12 50ms window — now much smaller during a graceful drain, table intact,
  offset committed. Not eliminated; documented.
- **R2 — CB-buffered events are not drained.** When the Redis CB is open, events sit in the
  `CbFallbackHandler` buffer with offsets uncommitted (ADR-02). On shutdown they are dropped and
  redelivered on restart — by design (buffer loss ≠ data loss); not part of the lane drain.
- **R3 — Forced-drain abandonment.** A lane/relay drain that hits its bounded deadline abandons
  remaining work; offsets stay uncommitted and redeliver (dedup-safe, ADR-13). Surfaced via
  `rpe.shutdown.forced` — an alert signal, not a silent loss.
- **R4 — Grace-window drift.** If a future change raises a component deadline (e.g. LLM TimeLimiter)
  above `timeout-per-shutdown-phase` or k8s `terminationGracePeriodSeconds`, SIGKILL can pre-empt the
  drain. The three numbers are a contract; the rule file records the ordering (component deadline <
  phase timeout < k8s grace).
- **R5 — SIGKILL / OOM / `kill -9`.** Graceful shutdown only runs on an orderly SIGTERM. A hard kill
  falls back to the pre-existing safety net (uncommitted offsets + dedup + deterministic alert_id +
  `processed_alerts ON CONFLICT`). Graceful shutdown narrows the loss window; it does not replace the
  durability design.

## Reversal Cost

`MEDIUM` — remove the four `SmartLifecycle`/listener beans, revert `OutboxBatchWriter`/`OutboxRelay`
to `@PostConstruct`/`@EventListener`, and drop the `server.shutdown`/k8s settings. The narrowed
constants and the pool `@PreDestroy` shutdowns are strictly-better standalone improvements and would
stay. No data-format or topic/contract change, so no cross-service coordination is required to revert.

## References

- `ADR-22.md` — the phase ladder, per-service drain contract, metrics, k8s alignment
- `AbstractMessageListenerContainer.DEFAULT_PHASE = 2147483547` (spring-kafka 4.1.0) — the anchor constant
- Spring Boot `server.shutdown: graceful` + `spring.lifecycle.timeout-per-shutdown-phase`
- ADR-02 (CB buffer), ADR-09 (rebalance drain), ADR-12 (outbox loss window), ADR-13 (replay safety)
