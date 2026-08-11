# ADR-07: Bounded VT Lane Queue + Caffeine-Backed Lane Map

**Status:** ACCEPTED (amended 2026-07-06) | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

An unbounded `ConcurrentHashMap<String, ExecutorService>` with unbounded task queues fails in two ways: (1) heap exhaustion under a hot account or slow Redis, and (2) memory growth proportional to distinct account population lifetime.

## Decision

Caffeine cache (`maximumSize(50_000)`, `expireAfterAccess(10 MINUTES)`) replaces `ConcurrentHashMap`. Each lane: `ThreadPoolExecutor(1,1)` with `ArrayBlockingQueue(200)` + a **bounded-blocking submit policy** (amendment, below). Caffeine removal listener shuts down the evicted executor.

The blocking submit is the back-pressure mechanism: when a lane queue fills, the submitting Kafka consumer thread **parks on the queue until capacity frees**. The consumer cannot poll Kafka while parked — throughput naturally throttles. `rpe.consumer.lag` rises visibly.

## Alternatives

**Unbounded `ConcurrentHashMap` + unbounded queue:** Heap exhaustion on hot accounts or Redis slowdown. No back-pressure signal. Rejected.

**`AbortPolicy` (reject task):** Drops events permanently (rejection propagates to the error handler → DLT after retries). Payment events are unrepeatable. Rejected.

**`CallerRunsPolicy` (original decision, superseded 2026-07-06):** Ran the overflow task inline on the Kafka listener thread — CONCURRENTLY with the lane worker for the same account. This silently broke the per-account single-writer invariant the lane exists to provide: (a) two threads in the Redis gate simultaneously for one account (geo read-before-write race — both read the same prev location); (b) the inline event processed out of offset order; (c) the listener parked up to 5s in the in-lane rate-limiter acquire — the exact anti-pattern ADR-03 moved acquisition into the lane to avoid. The poll-stall backpressure was correct; the inline execution was not. Rejected in favour of parking the submitter.

## Consequences

- Consumer thread parks on a saturated lane's queue — intended behaviour; same backpressure signature as the original inline execution (cannot poll; visible via consumer lag) without the invariant breach.
- The lane worker is the ONLY thread that ever executes an account's tasks; single-writer and offset order hold under saturation. Asserted by `LaneExecutorServiceTest`.
- One inline execution remains, deliberately: the lane's own worker re-submitting to its full lane (CB-open retry racing the BUFFERING flip). Parking there would deadlock the queue's only consumer; same-thread inline preserves both invariants.
- Lane eviction and recreation on next event is zero-cost — all account state lives in Redis, not in the lane executor.
- Memory bounded at all account population sizes.

## Failure Modes

- **Lane queue fills:** submitter parks; consumer lag rises; WARN logged every ~5s of parking. Ops-visible. Mitigation: investigate slow Redis or hot account.
- **Lane shut down mid-park (rebalance drain):** the park exits with `RejectedExecutionException`; the offset stays unacknowledged and `asyncAcks` holds the partition watermark — Kafka redelivers. Never a silent drop (the original `CallerRunsPolicy` silently discarded tasks on shut-down executors).
- **Caffeine evicts active lane mid-processing:** Next event for that account creates a fresh lane. No correctness impact (state is in Redis).

## Amendment (2026-07-06)

`CallerRunsPolicy` → `BoundedBlockingSubmitPolicy` (`LaneExecutorService`). The original decision documented the poll-stall as the backpressure feature but missed that inline execution runs concurrently with the lane worker, violating the "per-account single-threaded lane" invariant that geo read-before-write correctness depends on (EADIE audit, deferred P2). The replacement parks the submitter on the queue (bounded 500ms interruptible slices, retried until capacity/shutdown/interrupt), keeping the identical backpressure signature. Depends on `asyncAcks=true` (KafkaConfig) for the shutdown-mid-park redelivery guarantee.
