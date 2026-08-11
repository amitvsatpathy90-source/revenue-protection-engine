# ADR-05: Relay Trigger — LISTEN/NOTIFY + Adaptive Polling + RelayTrigger Interface

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

The relay needs a low-latency wake-up on outbox insert. Fixed 200ms polling generates 300 unnecessary DB queries/min at idle and adds latency at burst. An in-process `Semaphore` signal works co-located but cannot survive relay extraction to a separate service.

## Decision

`RelayTrigger` interface with `PostgresNotifyRelayTrigger` as the wired implementation. LISTEN/NOTIFY as primary wake-up signal; adaptive polling (50ms–5s exponential backoff) as self-healing fallback. Dedicated platform thread (not VT — PgJDBC `synchronized` pins) holds the LISTEN connection. Self-healing reconnect loop on `SQLException`. `HealthIndicator` exposes listener state to `/actuator/health`.

**Correctness invariant:** The signal is an optimisation, not a correctness mechanism. The relay always `SELECT ... FOR UPDATE SKIP LOCKED` on wake — it never trusts signal content. A missed, delayed, or duplicate signal changes latency, not correctness.

**Adaptive polling:** Processed rows > 0 → tighten to `MIN_POLL_MS=50`. No rows → backoff to `MAX_POLL_MS=5000`. Result: 40× fewer idle DB queries vs fixed 200ms polling.

## Alternatives

**In-process `Semaphore` as production implementation:** Works co-located, breaks on relay extraction. The relay is architecturally always separable — `SKIP LOCKED` and per-instance `transactional.id` assume it. Rejected.

**Fixed-interval polling:** 300 unnecessary queries/min at idle; adds measurable latency at burst. Rejected.

## Consequences

- One persistent PG connection for LISTEN, isolated from the JDBC pool.
- Flyway V4 migration required for trigger DDL (`FOR EACH STATEMENT` — one notification per batch, not per row; zero column references — survives all schema evolution).
- `lastNotificationAt` timestamp surfaces "connected but trigger not firing" as a distinct failure mode from connection loss.

## Failure Modes

- **LISTEN connection drops:** Reconnect loop retries every 5s. During reconnect window, adaptive polling fires every 5s (fallback). No relay stall — `semaphore.release()` in `catch` block.
- **Trigger not firing (DDL corrupted):** `lastNotificationAt` goes stale. Detection: `rpe.relay.listener.healthy` gauge drops + `rpe.outbox.pending.age_seconds` rises. Mitigation: re-apply V4 migration.
