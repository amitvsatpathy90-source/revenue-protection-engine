# ADR-04: Corporate Account Handling — Explicit Classification + Dynamic Config

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

Three distinct problems: (1) partition saturation from high-frequency accounts, (2) static elevated-limit config requiring redeploy to onboard new corporate accounts, (3) geo rule incorrectly applied to sharded accounts where per-account ordering is relaxed (composite key).

## Decision

1. Rate limiting caps throughput; partition saturation accepted as a known limitation. Composite-key scale-out documented as production path — not implemented in RPE.
2. Elevated classification and geo-exempt status managed via Redis SETs (`rpe:config:elevated_accounts`, `rpe:config:geo_exempt_accounts`) — refreshable without redeploy. Local Caffeine cache (60s TTL) prevents per-event Redis lookup.
3. Geo-exempt opt-in is always explicit. Never silently bypass the geo rule. Geo-exempt flag is logged at lane creation + exposed in metric tag (`rpe.geo.exempt=true`).

## Alternatives

**Static config for elevated accounts:** Requires redeploy to onboard a new corporate account. Operational friction. Rejected.

**Silently skip geo for composite-key accounts:** Breaking geo ordering without an explicit opt-in is invisible to operators and future engineers. Rejected.

## Consequences

- Composite key + geo-exempt is a documented production scale-out path; composite key breaks geo ordering — this trade-off is explicit and logged.
- Redis-based classification requires Redis availability; 60s Caffeine cache covers short outages.
- Geo-exempt is observable and auditable.

## Failure Modes

- **Redis classification SET unavailable:** Caffeine cache serves stale classification for 60s; after TTL, falls back to non-elevated limits. Logged at WARN. No silent misclassification.
- **Composite key deployment without geo-exempt flag:** Geo rule fires incorrectly on split-account events. Detection: unexpected geo alert rate spike for that account. Mitigation: add account to `rpe:config:geo_exempt_accounts`.
