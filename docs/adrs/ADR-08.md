# ADR-08: Welford Sample Cap at 10,000 Effective Samples

**Status:** ACCEPTED (amended 2026-07-16) | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

Welford accumulates `M2` (sum of squared deviations). For high-frequency accounts with millions of transactions, `M2` grows without bound. IEEE 754 double precision degrades silently — z-scores become inaccurate at large sample counts. The degradation is not observable in normal testing.

## Decision

`effective_count = math.min(count, 10000)` in Lua gate for all Welford arithmetic. Actual `count` stored separately and returned for the post-update gate check (`returned_count >= 30`). Cap externalised: `rpe.detection.welford.sample-cap=10000`.

## Alternatives

**No cap — accumulate all history:** M2 grows without bound; double precision degrades silently after ~millions of samples; z-scores become meaningless. Rejected.

**Reset stats after cap:** Sudden reset causes 30-event warmup gap during which z-score is blind. Rejected.

## Consequences

- Very high-frequency accounts reflect recent behaviour, not all-time history. For fraud detection this is a feature — old behaviour is not representative of current risk.
- Precision within the 10,000-sample window remains high.
- Statistical history beyond 10,000 samples is gradually phased out as new events arrive (oldest effectively weighted out).

## Failure Modes

- **Cap too low for volatile accounts:** z-score variance inflated by insufficient history. Detection: false positive rate spike for high-frequency accounts. Mitigation: increase `rpe.detection.welford.sample-cap`.

## Amendment (2026-07-16)

**The original decision capped the MEAN but not the variance — the Decision above says "all Welford arithmetic," but the code only ever applied `effective_count` to the mean update (`mu += d1/ec`). `M2` kept accumulating over the true, unbounded `n` (`m2 = m2 + d1*d2`), and `ZScoreDetector` divided that lifetime `M2` by the lifetime `count`.** So past the cap the z-score's two halves described different windows: the mean was an EWMA over the last ~10,000 samples while the stddev was a lifetime statistic. This is the exact unbounded-`M2` growth the Context set out to prevent — it was only ever half-solved (arch-audit 2026-07-16).

**Consequence (silent, and in the dangerous direction):** after a genuine regime shift in an account's amounts (e.g. a switch from ~\$100 to ~\$500 typical spend), the mean re-centred within ~10,000 events but `M2` stayed inflated by the old regime's deviations for ~`n` events. Inflated variance ⇒ deflated z-score ⇒ **real anomalies scored below threshold and silently missed** — no counter moved, no log fired. A worked case: an account at `n=100,000`, amounts ~N(100, 1), shifting to ~N(500, 1) injects ≈8×10⁸ into an un-decayed `M2`, giving stddev ≈ 85 against a true σ of 1; a genuine 3σ event then scores z ≈ 0.035 against a threshold of 3.0. Z-score detection for that account is effectively dead until `n` exceeds ~10⁸.

**Change — a matched pair (deploying either half alone is wrong):**
- `gate.lua`, past the cap only (`n > cap`): decay `M2` on the same `1/cap` schedule as the mean — `m2 = m2 - m2/ec + d1*d2` — so `M2 ≈ ec·σ²`, scaled to the same cap-sized window as the mean. At or below the cap the arithmetic is unchanged exact Welford (decaying the small-sample regime would corrupt the `>= 30` gate).
- `ZScoreDetector`: normalise by `min(count, sampleCap)`, not the true `count`. Dividing a now-cap-scaled `M2` by the true `n` would shrink stddev without bound as `n` grows ⇒ runaway **false positives** — the opposite failure. The two edits are correct only together.

The stored/returned `count` stays the true uncapped value (the `>= 30` gate reads it) — unchanged. `ruleName()` and detector precedence are untouched, so `alert_id` determinism and `processed_alerts` dedup are unaffected; this changes *which* events alert, not their identity. Existing `stats:{account}` keys hold lifetime-scale `M2` and converge to the new cap-scale over ~`cap` subsequent events (or roll over within the ADR-27 30-day idle TTL), self-healing with no migration.

**Rejected alternative — cap the stored `count` too:** would break the `>= 30` insufficient-history gate (ADR-14 / lua-gate.md), which reads the true post-update count. The cap is an arithmetic-window control, not a count control.

**Pinned by:** `ZScoreDetectorTest.pastTheSampleCapStddevNormalisesByTheCapNotTheTrueCount` (the Java divisor) and `RedisStatsLifecycleIntegrationTest.welfordM2StopsAccumulatingPastTheSampleCap` (the Lua decay, against real Redis with a small cap). Both were negative-tested: each fails on the pre-amendment code and passes on the amended code.
