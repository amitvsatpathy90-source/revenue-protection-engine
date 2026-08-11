# ADR-10: Redis Eviction Policy — `volatile-lru`

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

`allkeys-lru` evicts any key under memory pressure, including Welford `stats:{account}` keys which have no TTL. Evicting stats silently resets an account's z-score history — 30-event warmup restarts; z-score is blind during warmup. This is the most expensive form of state loss in RPE.

## Decision

`maxmemory-policy volatile-lru`. Welford stats keys carry no TTL by design — they are fully protected under `volatile-lru`. Dedup, velocity, and geo keys carry TTLs and remain eviction candidates.

## Alternatives

**`allkeys-lru`:** Evicts stats keys under pressure, silently restarting warmup for affected accounts. Loss is not self-healing within the session. Rejected.

**`noeviction`:** Redis returns errors on write when memory is full. Consumer pipeline errors out. Rejected.

## Consequences

- Under extreme memory pressure: bounded missed detections (dedup/velocity/geo eviction). Stats accuracy preserved.
- Dedup/velocity/geo loss is self-healing (reaccumulates over subsequent events). Stats loss is not.
- Memory sizing documented in README: 96mb supports ~50,000 concurrently active accounts.

## Failure Modes

- **Redis hits `maxmemory`:** `volatile-lru` begins evicting TTL-bearing keys. Detection: `rpe.dedup.blocked` drops (dedup key evicted = replay slips through). Mitigation: increase `maxmemory` or reduce account population.
