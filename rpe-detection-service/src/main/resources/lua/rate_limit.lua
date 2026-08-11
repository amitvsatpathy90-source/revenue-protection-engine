--[[
  RPE Distributed Rate Limiter — atomic token bucket, one round-trip per acquire (ADR-24).

  Replaces the per-instance Guava RateLimiter so the global limit is `rate_per_sec`
  regardless of replica count N (closes ADR-03's `global = N × per_instance`).

  Token bucket, NOT fixed-window INCR+EXPIRE: a fixed window has the boundary-burst
  exploit (2× rate at the window edge) that this codebase already rejects for the
  velocity detector (lua-gate.md). The bucket is semantically identical to the Guava
  limiter it replaces — smoothed rate + a ~1s burst capacity — so the Java contract
  (tryAcquire → boolean) is unchanged.

  Separate from gate.lua by design: rate-limiting runs BEFORE the gate so a throttled
  or breached event never mutates velocity/Welford/geo state, and gate.lua's step-order
  + return contract (ADR-14) stays frozen.

  Clock: server-authoritative via redis.call('TIME') — NOT a caller-supplied timestamp (ADR-24
  R2). Every replica's refill therefore reads ONE clock (the Redis primary's), so inter-replica
  wall-clock skew cannot perturb the shared bucket. A caller clock (each replica's
  System.currentTimeMillis) made refill = elapsed error ∝ skew × rate — up to ~rate spurious
  tokens on a cross-replica transition. TIME is monotone within a primary and single-sourced,
  which the caller clock never was. (redis.call('TIME') makes the script effects-replicated;
  fine on Redis 7+, the only mode this system runs.)

  KEYS:
    [1] ratelimit:{account_id}   (hash: tokens, ts) — has a TTL ⇒ volatile-lru eviction
                                  candidate (ADR-10); eviction just resets a full bucket.

  ARGV:
    [1] rate_per_sec   (refill rate; also the bucket capacity = 1s smoothed burst)
    [2] key_ttl_ms     (idle-account eviction; bounds memory under high account churn)

  Returns array:
    [1] allowed  (integer 0|1)
    [2] wait_ms  (integer — ms until one token accrues when denied; 0 when allowed)
--]]

local rate   = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])

-- Server-authoritative clock: TIME returns { unix_seconds, microseconds } as strings. One clock
-- for all replicas (see header) — this is the ADR-24 R2 fix, not a caller-supplied now_ms.
local t      = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- Capacity = rate ⇒ at most ~1 second of unused permits may accumulate, matching
-- Guava RateLimiter.create(rate)'s default maxBurstSeconds = 1.0.
local capacity = rate

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local last   = tonumber(state[2])

if tokens == nil then
  -- First sight (or evicted/expired): start with a full bucket.
  tokens = capacity
  last   = now_ms
end

-- Refill by elapsed server time. now_ms and the stored `ts` now come from the SAME clock
-- (redis.call('TIME')), so elapsed is a true interval; max(0, ...) only guards the rare
-- primary-failover-to-lagging-replica regression — never refill negatively.
local elapsed = math.max(0, now_ms - last)
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * rate)

local allowed = 0
local wait_ms = 0
if tokens >= 1.0 then
  tokens  = tokens - 1.0
  allowed = 1
else
  -- ms for the bucket to accrue the (1 - tokens) shortfall at `rate` tokens/sec.
  wait_ms = math.ceil(((1.0 - tokens) / rate) * 1000.0)
end

-- Persist post-refill state. Store tokens as a string so a fractional balance survives
-- the round-trip with full precision (HMGET → tonumber on the next call).
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], ttl_ms)

return { allowed, wait_ms }
