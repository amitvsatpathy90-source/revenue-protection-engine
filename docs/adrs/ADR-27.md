<!-- edit-log (newest first): v1.0 | 2026-07-12 | Initial. ACCEPTED — implemented with the arch-audit Bundle 2 change (gate.lua PEXPIRE + RedisMemoryMetrics + redis.rules.yml). -->

---
asset_id: adr-27-stats-key-lifecycle
asset_path: docs/adrs/ADR-27.md
asset_type: adr
version: 1.0.0
created: 2026-07-12
last_updated: 2026-07-12
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [redis, lua, welford, eviction, ttl, lifecycle, observability, adr-10-follow-on]
---

# ADR-27 — Stats-key lifecycle: sliding idle TTL on `stats:{account}`, memory watermark

## Status

`ACCEPTED`

Supersedes: the "stats keys carry no TTL so `volatile-lru` cannot evict them" **sub-decision** of
ADR-10 (the `volatile-lru` policy itself, and the TTL'd dedup/vel/geo eviction-candidate design,
stand unchanged). Related: ADR-08 (Welford sample cap — the other bounded-precision trade on this
key), ADR-09 (replay drift — the failure class this decision's loss mode joins), ADR-14 (gate
return contract — untouched; the TTL is a new `ARGV[11]`, appended so indexes 1–10 stay frozen),
`ADR-27.md`, `ADR-27.md`.

## Context

ADR-10 chose `volatile-lru` and deliberately left `stats:{account}` (the Welford baselines)
without a TTL so eviction could never destroy detection history. The 2026-07 arch audit traced
where that protection terminates:

- Stats keys accumulate **per account ever seen**, not per concurrently-active account —
  infra-config's "96mb ≈ 50,000 concurrently active accounts" sized the wrong population. Under
  account churn the unevictable set grows monotonically (~200B/account, doc-derived estimate ⇒
  order 500k accounts to fill 96mb).
- Redis documents that the `volatile-*` policies **behave like `noeviction` when no keys carry an
  expiration** (redis.io key-eviction reference). The terminal state is therefore: stats fill
  `maxmemory` → write commands error → every `gate.lua` call fails → the `redis` CB opens → the
  ADR-02 fallback buffers to PAUSED — and stays there, because the ADR-26 auto-half-open probes
  keep hitting the same OOM. Detection is stalled until an operator flushes or resizes, and a
  flush destroys ALL baselines and in-flight dedup state.
- The dominant harm arrives **earlier and silently**: as the unevictable set grows, `volatile-lru`
  cannibalises the TTL'd dedup/velocity/geo working set first. Evicted velocity/geo state =
  missed alerts that look like a quiet day. No error, no metric, no log.
- The lab masks all of this: the synthetic generator uses a bounded account set. This is a
  production-path defect in a portfolio artifact that claims production-path honesty.

## Decision

`stats:{account}` gets a **sliding idle TTL** — `rpe.detection.stats.ttl-ms`, default 30 days —
applied by a `PEXPIRE` in gate step 2 (new `ARGV[11]`, appended additively), so every event
refreshes it and only accounts idle past the window are reclaimed. Detection (the sole Redis
owner) additionally exposes a **memory watermark**: `rpe.redis.used_memory.bytes` /
`rpe.redis.maxmemory.bytes` gauges (`RedisMemoryMetrics`, INFO-polled) backing an
`RpeRedisMemoryHigh` alert at 80% plus an `RpeRedisInfoPollFailing` meta-alert. This supersedes
ADR-10's stats-protected sub-decision: expiry of idle baselines and LRU eviction of stats under
genuine memory pressure are now possible **and intended** — graceful degradation instead of a
wedge.

## Alternatives Considered

- **SCAN-based idle sweep** (add a `last_seen` field; a scheduled sweeper deletes stats idle >
  N days) — rejected. A new moving part (pacing, cursor state, its own failure modes and metrics)
  that re-implements what Redis expiry does natively in one script line. The audit's own first
  proposal; demoted on critique for exactly that reason.
- **`allkeys-lru`** — rejected. Makes everything evictable but erases the deliberate eviction
  *ordering* (dedup/vel/geo are designed short-lived candidates; stats should outlive them under
  mild pressure). The TTL keeps ADR-10's ordering while removing only the immortality.
- **Watermark alert only, no lifecycle** — rejected. An alert on unbounded growth just schedules
  the wedge for a human to watch; the set still grows to any ceiling.
- **Resize `maxmemory`** — rejected as the primary fix. Delays, does not bound; an unbounded set
  fills any ceiling. Resizing remains the *operational response* the watermark may recommend.

## Consequences

**Positive:**

- The noeviction wedge is structurally removed: idle accounts expire; under pressure,
  approximated-LRU reclaims the least-recently-written stats first. Detection degrades
  per-account and self-heals instead of stalling globally.
- The memory-sizing claim becomes honest: occupancy is bounded by (accounts active within the
  TTL window) × per-account cost, not by all-time account cardinality.
- The silent-decay phase finally has a signal: `RpeRedisMemoryHigh` fires while the damage is
  still "eviction pressure", before it is "missed alerts".

**Negative:**

- A dormant account returning after >30 days idle has lost its Welford baseline: z-score is
  silent for that account's next 30 events (`welfordCount < 30` gate). Bounded, self-healing,
  same failure class as ADR-09 replay drift — accepted.
- Under sustained pressure, LRU approximation can evict a stats key for an account that is
  merely *infrequent*, not dormant — same 30-sample re-entry cost. This is the deliberate trade:
  per-account degradation over global stall.
- One extra O(1) `PEXPIRE` per event inside the atomic script — negligible against the existing
  ZADD/ZREMRANGEBYSCORE/HSET work.
- The loss mode is invisible per-account (no per-key expiry metric; unbounded cardinality would
  violate security.md's tag rules). Accepted — the aggregate watermark is the signal.

## Residual Risks (explicit)

- **R1 — churn outruns the TTL:** with enough distinct accounts inside 30 days, the bounded set
  can still approach `maxmemory`. The watermark is the tripwire; the knobs are
  `rpe.detection.stats.ttl-ms` down or `maxmemory` up. No hard cap exists by design (a cap =
  refusing new accounts = dropped detection).
- **R2 — watermark depends on its poller:** a failing INFO poll freezes the gauges false-healthy.
  Mitigated the same way as the Bundle-1 DLT gauges: `rpe.redis.info.poll_failures` +
  `RpeRedisInfoPollFailing` (never trust a frozen gauge).
- **R3 — the 30d default is uncalibrated:** synthetic data cannot validate the idle window.
  Production calibration = longest business-legitimate dormancy you want to keep a baseline for.

## Reversal Cost

LOW. Config-only in practice: setting `rpe.detection.stats.ttl-ms` very large approximates the
old behaviour; fully reverting is deleting one `PEXPIRE` line and the `ARGV[11]` plumbing.
`RedisStatsLifecycleIntegrationTest` pins the TTL's existence and its sliding refresh, so a
silent revert fails the build.

## References

- `rpe-detection-service/src/main/resources/lua/gate.lua` — step 2 `PEXPIRE`, `ARGV[11]`.
- `RpeProperties.StatsProperties` / `application.yml rpe.detection.stats.ttl-ms` — the knob.
- `RedisMemoryMetrics` + `monitoring/rules/redis.rules.yml` (k8s copy in `54-prometheus.yaml`) —
  the watermark.
- `RedisStatsLifecycleIntegrationTest` — pins sliding TTL + gauge population against real Redis.
- Redis key-eviction reference (redis.io): "The `volatile-xxx` policies behave like `noeviction`
  if no keys have an associated expiration."
- 2026-07-12 arch-audit finding R5 (Bundle 2); ADR-10 (superseded sub-decision).
