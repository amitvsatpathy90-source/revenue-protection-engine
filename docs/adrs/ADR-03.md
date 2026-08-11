# ADR-03: Rate Limiting — Inside Lane, Throttle-Not-Drop

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

A hot account saturates its lane. The naive implementation acquires the rate limiter on the Kafka consumer thread — this blocks all partitions assigned to that consumer while one account throttles, penalising unrelated accounts.

## Decision

`RateLimiter.tryAcquire(5, SECONDS)` executes inside the lane task (after submission). Consumer thread submits and returns immediately. DLT routing only on sustained 5-second breach. Default: 500/s; elevated: 2000/s (`rpe.rate-limiting.*`). Caffeine-backed limiter map (`maximumSize(50_000)`, `expireAfterAccess(10 MINUTES)`).

## Alternatives

**Acquire on consumer thread:** Blocks all partitions on that consumer instance while one account throttles. One hot account penalises unrelated accounts across all partitions. Rejected.

**Drop immediately on limit breach:** Permanently loses detection signal. Payment events are unrepeatable. Rejected.

## Consequences

- Per-instance rate limit. Multi-instance: effective global = `N × per_instance`. Production global enforcement via Redis `INCR+EXPIRE` on isolated pool — documented as upgrade path, not implemented.
- Caffeine-backed map bounds memory for large account populations.

## Failure Modes

- **Limiter map unbounded growth:** Mitigated by Caffeine `maximumSize(50_000)` + `expireAfterAccess`.
- **Elevated classification Redis lookup fails:** 60s Caffeine cache covers short outages; on cache miss falls back to default limit — logged at WARN.
