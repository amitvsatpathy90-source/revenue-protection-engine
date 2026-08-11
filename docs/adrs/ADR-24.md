<!-- edit-log (newest first): v1.1 | 2026-07-02 | R2 resolved — bucket clock moved from caller System.currentTimeMillis() to server-authoritative redis.call('TIME'); removes inter-replica skew from the shared bucket (arch audit). | v1.0 | 2026-06-29 | Initial. ACCEPTED. -->

---
asset_id: adr-24-distributed-rate-limiting
asset_path: docs/adrs/ADR-24.md
asset_type: adr
version: 1.0.0
created: 2026-06-29
last_updated: 2026-06-29
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [rate-limiting, redis, lua, token-bucket, resilience, observability, adr-03-follow-on]
---

# ADR-24 — Distributed rate limiting: global atomic Lua token bucket, degrade-to-local on Redis outage

## Status

`ACCEPTED`

Supersedes: the "Redis `INCR`+`EXPIRE` … documented as upgrade path, not implemented" line of
ADR-03 (the upgrade path is now built — and is a token bucket, not `INCR`+`EXPIRE`; see Decision §1).
Related: ADR-03 (per-account rate limiting, inside-lane, throttle-not-drop — the contract this
preserves), ADR-04 (elevated/geo-exempt classification driving the rate), ADR-10 (`volatile-lru`;
the `ratelimit:` key is a TTL'd eviction candidate), ADR-14 (the gate's frozen step-order/return
contract this script stays out of), ADR-15 (the triage LLM `RateLimiter` — intentionally left
per-instance; see Consequences), `ADR-24.md` (§Rate Limiting),
`ADR-01.md` (publishOn; no feature flags).

## Context

ADR-03 rate-limits each account inside its lane with a per-instance Guava `RateLimiter`. Its named
known limitation: **the effective global limit is `N × per_instance`** across N detection replicas.
A `default-limit-per-sec: 500` becomes 1500/s at three replicas — the limit does not mean what it
says. ADR-03 gestured at "Redis `INCR`+`EXPIRE` on an isolated pool" as the upgrade path but never
built it, and `INCR`+`EXPIRE` is the wrong primitive (see §1).

The constraint that makes this delicate: ADR-03's **throttle-not-drop** invariant. A payment event
is unrepeatable, so the limiter *waits* (up to 5s) rather than dropping, and routes to DLT only on a
sustained breach. Any distributed design must preserve that AND must not introduce a new way to lose
fraud signal when its Redis dependency hiccups.

## Decision

A **global atomic token bucket in Redis Lua** replaces the per-instance Guava bucket as the primary
limiter, with the existing Guava limiter retained as a **degraded local fallback**. The public
contract (`RateLimiterService.tryAcquire(accountId) → boolean`, called inside the lane) is unchanged
— `PaymentEventConsumer` is untouched. Enforcement detail in `ADR-24.md`.

**1. Token bucket, NOT fixed-window `INCR`+`EXPIRE`** (deviating from ADR-03's casual suggestion):
- A fixed window has the **boundary-burst exploit** — 2× the limit across a window edge — the exact
  flaw this codebase already rejects for the velocity detector (`lua-gate.md`: "Sliding window, NOT
  fixed. Fixed window has a boundary exploit"). Using it for rate limiting would be internally
  inconsistent.
- A token bucket is **semantically identical to the Guava limiter it replaces** (smoothed rate +
  ~1s burst capacity = `RateLimiter.create(rate)`'s `maxBurstSeconds = 1.0`), making it a true
  drop-in.
- One atomic round-trip mirrors the gate's design; a sliding-window *log* (ZSET) would store
  500–2000 members/account/sec — unbounded memory at these rates.

`rate_limit.lua` (KEYS `ratelimit:{account}`; ARGV `rate_per_sec, key_ttl_ms`) reads its clock from
`redis.call('TIME')` (server-authoritative — see R2), refills by elapsed server time, consumes one
token, and returns `{allowed (0|1), wait_ms}`.

**2. Separate script, runs BEFORE the gate.** Rate-limiting is a distinct Lua script, not folded
into `gate.lua`: a throttled or breached event must NOT mutate velocity/Welford/geo state, so the
check precedes the gate (as it did with Guava). The gate's step-order + return contract (ADR-14)
stays frozen.

**3. Throttle-not-drop preserved.** On a distributed deny, `RateLimiterService` parks the lane VT for
the bucket-reported `wait_ms` (bounded by `acquire-timeout-ms`, default 5s) and retries —
replicating Guava's blocking `tryAcquire(5s)`. The lane is single-threaded per account and VT-backed,
so the park preserves ordering and does not pin a carrier (same model as `RedisGate` /
`AccountClassificationService`). Budget exhausted ⇒ `false` ⇒ DLT, exactly as ADR-03.

**4. Degrade to the local limiter on Redis trouble — never fail closed.** The distributed call has
its OWN Resilience4j `ratelimit` CircuitBreaker + Bulkhead (no global defaults; distinct from the
gate's `redis` instances so a rate-limit hiccup cannot open the gate CB and needlessly buffer
events). When the `ratelimit` CB is open or Redis errors/times out, `tryAcquire` falls back to the
per-instance Guava limiter (the pre-ADR-24 behaviour) for the remaining budget:

| Redis state | Limiter behaviour | Global effect |
|---|---|---|
| healthy | distributed token bucket | global `rate/s` (the fix) |
| down / CB open | per-instance Guava (degraded) | `N × rate/s`, bounded, self-healing |

Failing **closed** (deny on Redis error) would drop unrepeatable events — exactly what ADR-03
forbids. Fail-**open-unlimited** removes all protection. Degrade-to-local keeps today's bound as the
floor; the Guava code is not deleted — it *becomes* the resilience floor (mirrors triage's
`DEGRADED_RULE_BASED`). `rpe.ratelimit.degraded` is the alert signal that the distributed limiter is
unavailable; CB-open short-circuits with no Redis round-trip (fast-fail, no per-event timeout).

**5. Memory + eviction.** `ratelimit:{account}` carries a TTL (`key-ttl-ms`, default 60s) ⇒ it is a
`volatile-lru` eviction candidate (ADR-10); eviction or expiry just resets a full bucket, which is
safe. Idle accounts evaporate; no unbounded growth.

**6. No new feature flag.** The distributed limiter is always-on; degradation is automatic via the
CB, never a config toggle (`reactive-pipeline.md` — RPE has no feature flags beyond the dev block).
`key-ttl-ms` / `acquire-timeout-ms` are plain tuning config.

## Alternatives Considered

| Option | Decision | Reason |
|---|---|---|
| Fixed-window `INCR`+`EXPIRE` (ADR-03's suggestion) | Rejected | Boundary-burst exploit (2× at the window edge) — the flaw the velocity detector already rejects. Token bucket is the consistent, drop-in-equivalent primitive. |
| Sliding-window log (ZSET of timestamps) | Rejected | Stores 500–2000 members/account/sec — unbounded memory at production rates; the bucket is O(1) state. |
| Fold rate-limit into `gate.lua` | Rejected | Rate-limit must run *before* state mutation (a throttled/breached event must not touch velocity/Welford/geo); folding it in inverts that and disturbs the frozen ADR-14 contract. |
| Fail-closed on Redis error | Rejected | Drops unrepeatable payment events on a Redis blip — exactly what ADR-03's throttle-not-drop forbids. |
| Fail-open-unlimited on Redis error | Rejected | Removes all rate protection during an outage. Degrade-to-local keeps today's per-instance bound as a floor. |
| Share the gate's `redis` CB | Rejected | Conflates a rate-limit hiccup with detection-core health; a rate-limit failure could open the gate CB and buffer events for a non-detection reason. Separate `ratelimit` CB. |
| Distribute the triage LLM `RateLimiter` too | Rejected (out of scope) | That limiter is a per-instance *billing breaker* (ADR-15) — a deliberate, separate decision. Making it global adds a Redis dependency to a triage path that has none. Noted, not changed. |

## Consequences

**Positive:** the configured `rate/s` is now the **actual global limit**, independent of replica
count — ADR-03's `N × per_instance` is closed for the detection account limiter. The change is a true
drop-in (call site unchanged), preserves throttle-not-drop, and adds no new failure mode that can
drop an event: a Redis outage degrades to the pre-existing per-instance floor rather than failing.
Fully observable (`rpe.ratelimit.decision{outcome,mode}`, `rpe.ratelimit.wait` timer,
`rpe.ratelimit.degraded`, `ratelimit` CB health). Proven against a real Redis: two limiter instances
sharing one account admit ~1× (not ~2×) the per-instance count
(`DistributedRateLimiterIntegrationTest`). All suites green (detection **80**, relay 5, alert 10,
triage 31).

**Negative:** the hot path gains one Redis round-trip before the gate (bounded 500ms, bulkheaded,
CB-protected; negligible vs the gate it precedes). Two limiters now coexist (distributed + local
fallback) — a small dual-surface cost, justified by never-fail-closed. The triage LLM RPM limit
(ADR-15) remains per-instance by design — a documented asymmetry, not an oversight.

## Residual Risks (explicit)

- **R1 — Degraded mode loses the global guarantee, not events.** While the `ratelimit` CB is open the
  limit reverts to `N × rate/s`. This is bounded and self-healing (CB half-open probes Redis), and
  surfaced by `rpe.ratelimit.degraded` — but a *sustained* Redis outage means the global cap is not
  enforced for its duration. Acceptable: the alternative (fail-closed) drops fraud signal.
- **R2 — Caller-clock refill. RESOLVED (v1.1).** The bucket originally refilled off the calling
  instance's `System.currentTimeMillis()`, so inter-replica clock skew perturbed the *shared*
  bucket. The original text called this "≪ 1s negligible" — that understated it: the refill term is
  `elapsed/1000 × rate`, so the error scales with **`rate`, not one token**. At
  `elevated-limit-per-sec: 2000`, a 200 ms skew injects up to ~400 spurious tokens on a
  cross-replica transition, and `max(0, …)` only guards the *negative* direction — a persistently
  fast replica systematically over-admits, eroding the very "global = rate/s" guarantee this ADR
  exists to provide. **Fix:** the script now reads `redis.call('TIME')`, so every replica refills
  off one clock (the Redis primary's); the Java caller no longer passes `now_ms`. This makes the
  script effects-replicated (fine on Redis 7+, the only mode this system runs) and removes clock
  skew as a correctness variable entirely. The old integration test
  (`DistributedRateLimiterIntegrationTest`) still proves the global cap via the unchanged
  `tryConsume` contract; note it ran with synchronised clocks and so never exercised the skew this
  fix removes — a multi-replica-skew soak remains the stronger proof.
- **R3 — Per-account hot-spotting unchanged.** This makes the *limit* global; it does not redistribute
  a hot account across partitions (ADR-04's composite-key scale-out is still the production path for
  throughput, not correctness).
- **R4 — Token-bucket fairness under contention.** Concurrent replicas race on the same key; Redis
  serialises script execution so there is no lost update, but ordering among replicas is arrival-time,
  not fair-queued. Acceptable for a rate limiter (throttle, not scheduler).

## Reversal Cost

`LOW` — delete `rate_limit.lua`, `DistributedRateLimiter`, the `rateLimitScript` bean, and the
`ratelimit` R4j instances; revert `RateLimiterService` to the Guava-only path (still present as the
fallback). No data-format, schema, or topic change; the `ratelimit:` keys expire on their own.

## References

- `ADR-24.md` §Rate Limiting — the enforcement detail (token bucket, degrade,
  metrics)
- `rpe-detection-service/src/main/resources/lua/rate_limit.lua` — the token-bucket script
- `DistributedRateLimiter` / `RateLimiterService` — primary + degraded orchestration
- `DistributedRateLimiterIntegrationTest` — the global-cap proof (two instances, one bucket)
- ADR-03 (per-instance limiter this supersedes the upgrade-path line of), ADR-10 (eviction),
  ADR-14 (gate contract), ADR-15 (triage LLM limiter asymmetry)
