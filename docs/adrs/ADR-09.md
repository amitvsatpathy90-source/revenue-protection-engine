# ADR-09: Consumer Rebalance — Partition-Scoped Lane Drain, 5s Bound

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

Kafka rebalances revoke partition ownership. In-flight lane tasks for revoked partitions may complete after another consumer starts processing that partition — concurrent dual-processing without drain coordination. Draining all lanes on any rebalance stalls active partitions that are not being revoked.

## Decision

`onPartitionsRevoked` drains lanes only for accounts on revoked partitions, bounded at 5 seconds. Post-timeout overlap is accepted; correctness is maintained by the dedup gate + deterministic `alert_id` + `processed_alerts ON CONFLICT DO NOTHING`.

**Partition → account index:** `partitionAccountIndex.computeIfAbsent(partition, k -> ConcurrentHashMap.newKeySet()).add(accountId)` on each record processed. On revocation, drain accounts for that partition only, then remove from index.

## Alternatives

**Drain all lanes on any rebalance:** Stalls active partitions not being revoked — unnecessary consumer lag on healthy partitions. Rejected.

**No drain:** Dual-processing overlap for all in-flight tasks at rebalance. Handled by dedup but increases dedup load unnecessarily. Rejected.

## Consequences

- Brief processing overlap possible after 5s timeout. Correctness maintained by the dedup + deterministic ID stack.
- 5s bound prevents infinite wait when Redis is slow or down.

## Failure Modes

- **Drain exceeds 5s (Redis slow):** Timeout triggers; brief dual-processing begins. Detection: duplicate dedup key hits (Redis `SET NX` returns 0) — observable via `rpe.dedup.blocked` counter. Correctness unaffected.
