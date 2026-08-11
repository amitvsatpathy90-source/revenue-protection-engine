# ADR-12: `synchronous_commit=off` vs UNLOGGED Table for Outbox

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

Outbox inserts need to be fast (~3× faster than synchronous WAL flush). Two candidates: `UNLOGGED` table (no WAL at all) or `SET LOCAL synchronous_commit=off` (deferred WAL flush, bounded loss window).

## Decision

`SET LOCAL synchronous_commit=off` per outbox insert transaction. Not `UNLOGGED`.

## Alternatives

**UNLOGGED table:** Loses ALL rows written since the last checkpoint on crash. Default Postgres checkpoint interval: 5 minutes. Under load, relay pickup lag could add further seconds — total loss window is unbounded and load-dependent. Replication does not work on UNLOGGED tables (no WAL). Strictly worse on every dimension. Rejected.

**Full synchronous WAL (default):** Correct for production. ~3× slower than `synchronous_commit=off` for the lab throughput objective. Production upgrade path: remove `SET LOCAL synchronous_commit=off` — single line, no schema change.

## Consequences

- Loss window: committed outbox rows in the last 200ms WAL flush window may not survive a hard OS/hardware crash. Table structure and all previously flushed rows survive intact.
- Replication works normally (WAL is still generated, just not synchronously acknowledged).
- Production upgrade: remove `SET LOCAL synchronous_commit=off`. Zero schema migration, zero redeployment coordination.

## Failure Modes

- **Hard crash during 200ms window:** Up to 200ms of alert intents lost. Table survives. On restart, relay continues from the last durable row. Detection: gap in `alert_id` sequence for the crash window. Accepted and documented.

- **Full alert-intent loss window is wider than the 200ms WAL window — and wider than the "50ms" batch figure.** The hot path commits the Kafka offset *synchronously* right after handing the alert to a **fire-and-forget** `Mono.fromCallable(outboxWriter::submit).subscribeOn(jdbcScheduler)` — the offset can commit before the intent is even enqueued into the volatile queue. So a crash between offset-commit and durable WAL flush loses the intent with **no Kafka redelivery** (offset already committed), and the true window is the sum of three stages: (1) `jdbcScheduler` schedule/queue latency for the async submit, (2) up to the 50ms batch flush timer, (3) this 200ms `synchronous_commit=off` WAL window. The `rpe.hotpath.alerts.submitted − rpe.hotpath.alerts.buffered` gauge delta is the live measure of stage (1). State the loss window as this chain, not a flat "50ms". Removing `SET LOCAL synchronous_commit=off` (the production upgrade) closes stage (3) only.

  **RESOLVED (arch-audit HIGH-1):** the submit is no longer fire-and-forget. `OutboxBatchWriter.submit` now returns whether the intent was durably enqueued (applying bounded backpressure when the queue is full), and `PaymentEventConsumer` **waits for that result before acking**: on success it acks as before (a steady-state offer is O(1), so the ADR-12 fast path is unchanged); on saturation — a sustained Postgres outage — it routes the event to `payment.events.DLT` for redrive instead of acking a dropped alert. This eliminates the "committed offset, no redelivery, lost alert" **outage path** (sustained Postgres unavailability). It does **not** collapse the loss window to stage (3) alone: "durably enqueued" means the intent is accepted into the writer's in-memory queue, not that it has reached Postgres — stage (2), the bounded up-to-50ms batch-flush timer, is unaffected by this fix and stays open. **Correction 2026-07-15 (coverage audit):** this paragraph previously stated "the residual normal-operation exposure is stage (3) only" — that undercounted it. The fix eliminates stage (1) (the unbounded async-schedule delay that used to precede the enqueue) because the ack now *follows* a successful enqueue instead of racing it; stages (2) and (3) were never touched by it. The correct residual on a hard crash after ack is stages (2)+(3): up to the 50ms batch-flush timer plus the 200ms `synchronous_commit=off` WAL window. Architecture Spec's Known Limitations entry for this ADR already states the corrected figure ("≤50ms flush timer + 200ms WAL") — this paragraph was the one that had drifted from it, not the reverse. Deterministic `alert_id` + `processed_alerts ON CONFLICT` keep DLT redrive exactly-once regardless.

## Changelog

| Date | Change |
|---|---|
| 2026-07-15 | Coverage-audit correction: the arch-audit HIGH-1 RESOLVED note's "residual exposure is stage (3) only" claim was wrong — the durable-enqueue fix only closes stage (1); stage (2) (≤50ms batch-flush timer) was never eliminated and remains open alongside stage (3) (200ms WAL). Corrected to match Architecture Spec's already-accurate restatement, which had silently diverged from this ADR's own text. |
