# ADR-13: Conscious Non-Durable Event Dedup

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** MEDIUM

---

## Context

RPE's event dedup is Redis-resident and best-effort (TTL-bounded, 1s AOF window). The alternative is a durable transactional inbox (`INSERT ... ON CONFLICT DO NOTHING` in the same DB transaction as the business effect — the ChaosForge pattern). The decision must be conscious, not accidental.

## Decision

RPE accepts non-durable event dedup to keep the hot path Redis-only. Exactness is deferred to the alert boundary via deterministic UUIDv5 `alert_id` + `processed_alerts ON CONFLICT DO NOTHING`.

Rationale: the hot path must never touch Postgres per event — throughput is the demonstration objective. Non-durable dedup within a 300s TTL window is strong in practice; the gap is the 1s AOF crash window and TTL expiry on long-silent events. Compensation: duplicate event processing by detection rules is possible post-crash, but no duplicate alert action will ever be taken — the same `event_id + rule_name` always produces the same UUIDv5 `alert_id`; `processed_alerts ON CONFLICT DO NOTHING` is the exactness control.

## Alternatives

**Durable transactional inbox (ChaosForge pattern):** Correctness primitive — inbox row + business effect in one transaction. Requires per-event Postgres write on the hot path. Contradicts the throughput objective. Rejected for RPE; correct for ChaosForge.

**Redis durable dedup (persistence mode):** RDB + AOF synchronous (`appendfsync always`). ~10× Redis write latency; makes Redis the throughput bottleneck. Rejected.

## Consequences

- Duplicate event processing by detection rules possible within TTL window after a Redis crash. Bounded, self-healing.
- No duplicate alert action possible — deterministic UUIDv5 `alert_id` + `processed_alerts` is the correctness control.
- This is the tightest guarantee achievable on this stack without XA transactions.

**Contrast with ChaosForge:** ChaosForge optimises for transactional correctness — inbox is the correctness primitive. RPE optimises for throughput — exactness lives at the alert boundary. Both choices are correct for their respective objectives.

## Failure Modes

- **Redis crash + AOF 1s window:** Events in the last second may replay past dedup gate on restart. Detection: `rpe.dedup.blocked` counter drop post-restart. Correctness: deterministic `alert_id` absorbs any duplicate. No action required.
- **Dedup TTL expiry on long-silent account:** Event replays after 300s TTL. Handled identically to above.
