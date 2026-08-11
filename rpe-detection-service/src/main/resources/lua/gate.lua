--[[
  RPE Unified Redis Gate — executes atomically per payment event.

  Execution order is a correctness invariant — NEVER reorder:
    0. Dedup PRE-CHECK (EXISTS — READ ONLY): a redelivery of an already-seen event_id returns here
       having mutated NOTHING. This addresses the common case of the stat-pollution bug: the old
       ordering ran velocity/Welford/geo for EVERY delivery and only checked dedup last, so a Kafka
       at-least-once redelivery double-counted into the Welford baseline even though the alert was
       suppressed. Residual it does NOT close: Redis does not roll back a script's completed writes
       on a mid-script error, so if steps 1-2 succeed and step 3 errors, dedup is never written
       (correct) and the redelivery re-runs steps 1-2 (EXISTS=0). Bounded/self-healing, same class
       as ADR-09; the live trigger is a maxmemory write error under ADR-27 noeviction pressure.
    1. Velocity   (ZADD + prune + PEXPIRE)      -- only when NOT a duplicate
    2. Z-score    (Welford HSET + PEXPIRE stats_ttl — ADR-27 sliding idle TTL)
    3. Geo        (read prev, HSET new + PEXPIRE)
    4. Dedup MARK (SET NX EX dedup_ttl, default 300) — STILL MUST BE LAST
  Crash-safety is unchanged: the dedup key is only WRITTEN at step 4, so if any step 1–3 errors the
  event is NOT marked seen and Kafka redelivers safely. The step-0 EXISTS is a read, so it never marks
  an event seen prematurely — that hazard only ever applied to a dedup WRITE running first.

  KEYS:
    [1] vel:{account_id}
    [2] stats:{account_id}
    [3] geo:{account_id}
    [4] dedup:{event_id}

  ARGV:
    [1] event_id
    [2] broker_ingest_ms  (NOT payload timestamp — payload clocks drift)
    [3] amount
    [4] lat
    [5] lon
    [6] vel_window_ms
    [7] vel_ttl_ms
    [8] geo_ttl_ms
    [9] welford_sample_cap  (rpe.detection.welford.sample-cap — ADR-08)
    [10] dedup_ttl_sec      (rpe.detection.dedup.ttl-seconds — default 300)
    [11] stats_ttl_ms       (rpe.detection.stats.ttl-ms — ADR-27 sliding idle TTL on
                             stats:{account}; appended last so ARGV[1..10] stay frozen)

  Returns array:
    [1] dedupBlocked      (integer 0|1)
    [2] velocityPriorCount (integer — ZCOUNT BEFORE adding this event)
    [3] welfordCount       (integer — post-update count)
    [4] welfordMean        (bulk string)
    [5] welfordM2          (bulk string)
    [6] geoPrevLat         (bulk string | nil — nil when no prior location)
    [7] geoPrevLon         (bulk string | nil)
    [8] geoPrevBrokerMs    (bulk string | nil)
--]]

local now_ms    = tonumber(ARGV[2])
local amount    = tonumber(ARGV[3])
local vel_win   = tonumber(ARGV[6])
local vel_ttl   = tonumber(ARGV[7])
local geo_ttl   = tonumber(ARGV[8])
local cap       = tonumber(ARGV[9]) or 10000
local dedup_ttl = tonumber(ARGV[10]) or 300
local stats_ttl = tonumber(ARGV[11]) or 2592000000  -- 30d default (ADR-27)
local win_start = now_ms - vel_win

-- ── Step 0: Dedup pre-check (READ ONLY — mutate nothing if duplicate) ───────
-- EXISTS, not SET: a redelivery must NOT re-mutate velocity/Welford/geo (that was the stat-pollution
-- bug). The dedup key is still WRITTEN last (step 4), so crash-safety is unchanged. On a duplicate we
-- return the CURRENT read-only state with blocked=1; Java (LuaGateResult) ignores all other fields when
-- blocked, and the detectors never run, so returning live values here is purely shape-preserving.
if redis.call('EXISTS', KEYS[4]) == 1 then
  local vprior = redis.call('ZCOUNT', KEYS[1], '(' .. win_start, '+inf')
  local cur    = redis.call('HMGET', KEYS[2], 'count', 'mean', 'M2')
  local gcur   = redis.call('HMGET', KEYS[3], 'lat', 'lon', 'ts')
  return { 1, vprior, tonumber(cur[1]) or 0,
           tostring(tonumber(cur[2]) or 0.0), tostring(tonumber(cur[3]) or 0.0),
           gcur[1], gcur[2], gcur[3] }
end

-- ── Step 1: Velocity (sliding window) ──────────────────────────────────────
-- Count BEFORE adding this event — the prior window occupancy.
-- Exclusive lower bound: events with score > win_start are in the window.
local prior = redis.call('ZCOUNT', KEYS[1], '(' .. win_start, '+inf')
redis.call('ZADD', KEYS[1], now_ms, ARGV[1])
-- Remove events at or before win_start (outside the window).
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', win_start)
redis.call('PEXPIRE', KEYS[1], vel_ttl)

-- ── Step 2: Welford online stats (z-score) ─────────────────────────────────
local raw = redis.call('HMGET', KEYS[2], 'count', 'mean', 'M2')
local n  = (tonumber(raw[1]) or 0) + 1
local mu = tonumber(raw[2]) or 0.0
local m2 = tonumber(raw[3]) or 0.0
-- effective_count caps at ARGV[9] for numerical stability (ADR-08).
-- Actual n stored and returned for the >=30 gate check in Java.
local ec = math.min(n, cap)
local d1 = amount - mu
mu = mu + d1 / ec
local d2 = amount - mu  -- delta against NEW mean
if n > cap then
  -- ADR-08 AMENDMENT (arch-audit 2026-07-16). Past the cap the mean is an EWMA with alpha = 1/cap,
  -- so M2 MUST decay on the same schedule or the two halves of the z-score describe different
  -- windows. Un-decayed, M2 accumulated over the true (unbounded) n while the mean tracked only the
  -- last `cap` samples; ZScoreDetector then divided a LIFETIME M2 by a LIFETIME n, so after a genuine
  -- regime shift in an account's amounts the mean re-centred within ~cap events but the variance
  -- stayed inflated by the old regime for ~n events. Inflated stddev => suppressed z => alerts
  -- silently missed, with no counter moving. Decaying here keeps M2 ~= ec * sigma^2, i.e. scaled to
  -- the SAME cap-sized window as the mean; ZScoreDetector divides by min(n, cap) to match.
  -- The two changes are a matched pair — deploying either alone is wrong in one direction or the
  -- other (decay without the divisor => runaway false positives).
  m2 = m2 - m2 / ec + d1 * d2
else
  -- n <= cap: exact Welford, unchanged and exactly correct. Decaying here would corrupt the
  -- small-sample regime the >=30 gate depends on.
  m2 = m2 + d1 * d2
end
-- ADR-27 (supersedes ADR-10's no-TTL sub-decision): sliding idle TTL. Every event for the
-- account lands here, so the PEXPIRE refresh means a hot account never expires — only an
-- account idle past the TTL is reclaimed. Without it, stats keys are invisible to volatile-lru
-- and accumulate with account churn: they squeeze the TTL'd dedup/vel/geo working set (silent
-- detection decay) until no volatile keys remain and Redis wedges in noeviction write errors.
-- Loss on expiry/eviction is bounded and self-healing: the account re-enters the <30-sample
-- z-score gate (same failure class as ADR-09 replay drift).
redis.call('HSET', KEYS[2], 'count', n, 'mean', mu, 'M2', m2)
redis.call('PEXPIRE', KEYS[2], stats_ttl)

-- ── Step 3: Geo ────────────────────────────────────────────────────────────
-- Read previous location before overwriting.
local gp = redis.call('HMGET', KEYS[3], 'lat', 'lon', 'ts')
redis.call('HSET', KEYS[3], 'lat', ARGV[4], 'lon', ARGV[5], 'ts', now_ms)
redis.call('PEXPIRE', KEYS[3], geo_ttl)

-- ── Step 4: Dedup MARK — MUST BE LAST ──────────────────────────────────────
-- We already returned early at step 0 if this event_id was seen, so within this atomic script the key
-- cannot exist yet; NX is retained as belt-and-suspenders. Writing it LAST keeps the crash-safety
-- invariant: if any step 1–3 had errored, we never reach here and the event stays un-seen (redeliverable).
redis.call('SET', KEYS[4], '1', 'NX', 'EX', dedup_ttl)

-- Not a duplicate (guaranteed by the step-0 early return) → blocked = 0.
-- gp[1..3] are false (nil) when geo key did not exist; Redis serialises false
-- as nil bulk-string, which Spring Data Redis maps to null in the List<Object>.
return { 0, prior, n, tostring(mu), tostring(m2), gp[1], gp[2], gp[3] }
