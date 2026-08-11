# ADR-02: Redis CB Fallback — C+D Hybrid; DLT Rejected for Transient Failures

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** MEDIUM

---

## Context

When the Redis circuit breaker opens, the pipeline cannot process events. Three options: route to DLT, fail-open, or hold events. Payment events are unique and unrepeatable — a given event represents a transaction that happened exactly once. The question is whether DLT routing is correct for infrastructure failures.

## Decision

C+D hybrid: in-memory `ArrayBlockingQueue` absorbs events during short Redis outages (Option D). When the buffer fills on extended outages, pause the Kafka consumer container (Option C). Resume on buffer drain after CB transitions to `HALF_OPEN`. DLT routing explicitly rejected for Redis unavailability.

**State machine:**
```
NORMAL    → [CB opens]      → BUFFERING
BUFFERING → [buffer full]   → PAUSED
PAUSED    → [CB half-open]  → DRAINING → NORMAL
BUFFERING → [CB half-open]  → DRAINING → NORMAL
```

**Buffer sizing:** `event_rate_per_sec × cb_wait_duration_sec × 1.5`. Default: 500 slots (`rpe.resilience.redis.buffer-size=500`).

## Alternatives

**DLT routing on CB open:** Payment events are unique — routing to DLT on a Redis blip permanently removes a fraud signal from the pipeline. The event that represented a fraud is never actioned. DLT is correct only for structurally unprocessable events, not transient infrastructure failures. Rejected.

**Option D alone (buffer, no pause):** Under extended outage, buffer grows unboundedly → heap exhaustion. Rejected.

**Option C alone (pause immediately):** A 2-second Redis blip causes visible consumer lag and Kafka offset stall. Disproportionate for transient failures. Rejected.

## Consequences

- Buffer is heap-resident; must be sized correctly. Crash during buffering is not data loss — uncommitted offsets cause Kafka to redeliver from last commit on restart.
- Extended outages (lag > SLO threshold) require human decision — no automatic escalation.
- Drain path re-submits buffered events through the normal lane path — per-account ordering preserved.

## Failure Modes

- **Buffer undersized:** Heap exhaustion before pause triggers. Detection: `rpe.consumer.lag` rising + JVM heap metrics. Mitigation: increase `rpe.resilience.redis.buffer-size`.
- **CB never closes:** Consumer stays paused indefinitely. Detection: `rpe.consumer.lag` alert after SLO threshold. Mitigation: human ops intervention; Redis restart.
