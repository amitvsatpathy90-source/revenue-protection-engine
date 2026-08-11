# ADR-14: Lua Gate Returns Raw State; Java Detectors Apply Thresholds

**Status:** ACCEPTED | **Decided:** 2026-06-09 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

Architecture Spec previously specified "Script returns: verdict (ALERT|CLEAN), rule_name, reason, post-update count" — implying the Lua script owns threshold evaluation. The `Detector` interface design inverts this: Lua returns raw state, each `Detector` implementation evaluates thresholds in Java. These are mutually exclusive models. Silent deviation from the spec is a defect.

## Decision

Lua gate returns a raw state array: `[dedupResult, velocityPriorCount, welfordCount, welfordMean, welfordM2, geoPrevLat, geoPrevLon, geoPrevBrokerMs]`. The Java mapper constructs `LuaGateResult` from this array; it is the single site where `GeoSnapshot` is assembled. If any of the three geo fields is absent or invalid (NaN, out-of-range, or `brokerMs <= 0`), the mapper throws `IllegalArgumentException` — this is a gate contract violation, not a transient error, and routes directly to DLQ without retry. Each `Detector` implementation receives a `LuaGateResult` record and applies rule-specific threshold logic in Java against `@ConfigurationProperties`-bound config values. `Detector.evaluate()` is called only when `dedupBlocked = false`.

**`LuaGateResult` contract:**
- `dedupBlocked`: true = event already seen within TTL. All state mutations (steps 1–3) already ran before this flag is set — velocity re-ZADD is idempotent; Welford/geo re-applied = accepted bounded drift.
- `geoPrev`: type `GeoSnapshot(double lat, double lon, long brokerMs)`, **nullable** — `null` = no prior location for this account. The three geo fields are co-dependent and encoded as a single nullable value object; heterogeneous sentinels (NaN + boxed Long) across three flat fields make desync representable in the type and force every detector to maintain two independent guards for one logical condition. `GeoSnapshot.brokerMs` is primitive `long` — unconditionally valid when the record is non-null.

**`GeoSnapshot` constructor guards (in order — order is load-bearing):**
1. `Double.isNaN(lat) || Double.isNaN(lon)` → throw. NaN must precede the range check: NaN comparisons are silently false, so the range check alone passes NaN through.
2. `lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0` → throw. Catches ±Infinity and sentinel values (e.g. 999) that Lua might emit on error.
3. `brokerMs <= 0` → throw. Rejects Kafka's `-1` no-timestamp sentinel and epoch-0.

**Exception message rule:** messages describe violation shape (boolean flags, magnitude bucket of the offending field only), never coordinate values. Coordinates are PII under GDPR; `getMessage()` outlives the log sink — it surfaces in span events, aggregator fingerprints, and rethrown exception chains where MDC and redaction do not travel. `brokerMs` may appear verbatim (a timestamp without identity is not PII; identity is already in MDC on the same log line, which is the governed surface).

## Alternatives

**ARGV thresholds → Lua decides verdict:** Thresholds passed as ARGV from `@ConfigurationProperties`; Lua applies them and returns verdict. Satisfies config-driven thresholds without Lua redeployment. But collapses all rule logic into one script, makes the `Detector` interface a pass-through with no evaluation logic, and makes per-rule unit testing require Redis. Rejected.

**Keep Lua-returns-verdict (original spec):** Pluggability is theatre — adding a rule requires Lua changes. Rejected.

## Consequences

- Threshold change = config reload only. No Lua script change, no `SCRIPT LOAD`.
- Lua script owns only atomic state mutations — smaller, stable, testable in isolation.
- `Detector` implementations are plain Java — unit-testable with no Redis dependency.
- Architecture Spec "Script returns verdict" line superseded by this ADR.
- `GeoSnapshot` constructor enforces all three geo field invariants at the single construction site; invalid state is unrepresentable in the type, not guarded by convention across N detectors.

## Failure Modes

- **`dedupBlocked` misread as "Redis untouched":** Implementor skips null/NaN guards assuming clean state. Mitigation: `dedupBlocked` Javadoc explicitly states state mutations ran before this flag.
- **`geoPrev` null check missed:** NPE at `geoPrev.lat()` in `GeoDetector`. Loud failure — caught in unit tests. One null check covers all three co-dependent fields.
- **NaN propagation in geo path (removed):** NaN sentinel on flat lat/lon fields is no longer in the design. `GeoSnapshot` fields are unconditionally valid when the record is non-null.
- **`stddev=0` in ZScoreDetector:** `sqrt(M2/count) = 0` → divide-by-zero → Infinity/NaN z-score → silent CLEAN. Mitigation: `evaluate()` Javadoc mandates `stddev == 0.0` early return.
- **`ruleName()` returns null in `DetectionResult.alert()`:** UUIDv5 input becomes `"eventId:null"` — silent dedup key corruption. Mitigation: `Objects.requireNonNull(ruleName)` in `DetectionResult.alert()` factory.
- **Partial Lua output silently coerced to null `geoPrev`:** If the mapper catches `IllegalArgumentException` and returns `null` instead of throwing, a Lua regression becomes invisible missed detections with no signal. Mitigation: mapper catch site increments `rpe.gate.invalid_state{reason=geo}`, throws `GateContractViolation`, routes to DLQ. No retry — the violation is deterministic per event.
- **Double error emission on gate contract violation:** Log-and-rethrow at the catch site followed by a second `log.error` in the DLQ terminal handler produces duplicate error events and inflates alert counts on log-based metrics. Mitigation: catch site owns counter increment and `GateContractViolation` wrap; DLQ terminal handler treats `GateContractViolation` as pre-reported and logs at `debug` only.

## Changelog

| Date | Change |
|---|---|
| 2026-06-09 | Initial — Lua-returns-raw-state model accepted; `GeoSnapshot` replaces flat geo fields |
| 2026-06-11 | Mapper single-construction-site invariant and `GeoSnapshot` constructor guard ordering documented; exception message PII rule added; double-emit and silent-coercion failure modes added |
