<!-- edit-log (newest first): v1.0 | 2026-07-15 | Initial. ACCEPTED — backfill of an already-implemented 2026-07-12 arch-audit finding (R4), documenting the invariant ProcessedAlertsPurge.java has enforced since that date. -->

---
asset_id: adr-28-dedup-guard-horizon
asset_path: docs/adrs/ADR-28.md
asset_type: adr
version: 1.0.0
created: 2026-07-15
last_updated: 2026-07-15
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [postgres, dedup, purge, retention, dlt, re-drive, correctness, adr-23-follow-on, adr-27-sibling]
---

# ADR-28 — Dedup-guard horizon: `processed_alerts` purge must outlive every redelivery/re-drive window

## Status

`ACCEPTED`

Supersedes: nothing. Related: ADR-06 (relay at-least-once delivery — the reason a duplicate `alert_id`
can reach `payment.alerts` at all), ADR-13 (deterministic UUIDv5 `alert_id` — what makes the guard a
single `ON CONFLICT` check), ADR-17 §3.4 (alert-service is sole writer of `processed_alerts`), ADR-23
(DLT re-drive strategy — the operational path that can re-present an old `alert_id`), ADR-27 (sibling
2026-07-12 arch-audit finding R5, stats-key TTL — same audit cycle, same backfill-ADR treatment).

---

## Context

`processed_alerts` is the exactly-once guard for RPE's at-least-once `payment.alerts` delivery: the
relay can re-send a record after a crash between its Kafka commit and the outbox status UPDATE
(ADR-06), and every consumer of `payment.alerts` is required to dedupe via `INSERT ... ON CONFLICT DO
NOTHING` keyed on the deterministic UUIDv5 `alert_id` (ADR-13). That guard is only idempotent while
the original row still exists in the table. Several paths can re-present an old `alert_id` after the
fact: an operator-initiated DLT/parked re-drive (retention pinned at 14 days + `segment.ms` 1 day in
all three infra files, per ADR-23), a relay-outage backlog drain, or a consumer offsets-expiry replay
(`auto-offset-reset=earliest` × topic retention — currently 7 days broker-default on the pinned
`redpanda:v24.1.11` image, verified live via `rpk cluster config get log_retention_ms` → `604800000`).
Purge `processed_alerts` too aggressively and a legitimate late re-drive or replay silently double-acts
on an alert whose guard row is already gone.

This is a 2026-07-12 arch-audit finding (R4), from the same audit cycle and bundle that produced R5
(stats-key TTL, `ADR-27`). It was implemented directly — `ProcessedAlertsPurge.java` in
`rpe-alert-service`, an hourly `@Scheduled` purge — and documented in Architecture Spec's Known Limitations
and the class's own javadoc, but unlike R5 it never got a standalone ADR. This backfills that gap.

## Decision

`processed_alerts` purge horizon is 30 days (`ProcessedAlertsPurge`,
`DELETE FROM processed_alerts WHERE acted_at < now() - interval '30 days'`, hourly `fixedDelay` with a
5-minute startup offset; alert-service is sole writer per ADR-17 §3.4). Binding invariant: the purge
horizon must strictly exceed every window that can re-present an old `alert_id` — currently DLT/parked
retention (14 days) plus operator re-drive latency, and the offsets-expiry replay leg (7 days broker
topic retention). This is a correctness contract, not a tunable default: changing DLT retention, the
re-drive SLA, or topic retention requires re-verifying the inequality against the purge horizon — the
same discipline Architecture Spec already applies to the shutdown-timeout trio.

## Alternatives Considered

- **Shorter purge horizon matching the original 7-day housekeeping-only design** — rejected. A row
  could be purged before a legitimate late re-drive re-presents its `alert_id`, reopening silent
  double-acting on late re-drives.
- **Indefinite retention / no purge** — rejected. Unbounded table growth for no correctness benefit
  once every redelivery/re-drive path is exhausted.
- **Derive the purge horizon dynamically from DLT retention config at runtime** instead of a fixed 30d
  constant — deferred, not rejected. Unnecessary complexity at current retention values (30d gives
  comfortable margin over 14d + 7d); revisit only if DLT retention or the re-drive SLA changes enough
  to erode that margin.

## Consequences

**Positive:**
- `processed_alerts` holds ~30 days of rows — the table is small (~1% of all events), so this is cheap.
- The purge horizon is now an explicit, documented inequality rather than an assumed-safe constant —
  any future change to DLT/parked retention, re-drive SLA, or topic retention has a concrete check to
  run before shipping.

**Negative:**
- A relay outage or a parked record older than the purge horizon can still theoretically double-act —
  bounded by the 30-day margin, not eliminated (already surfaced in Architecture Spec Known Limitations).
- The invariant is enforced by convention and code comment, not by an automated cross-check between
  `ProcessedAlertsPurge`'s constant and the DLT retention values declared in the three infra files —
  a future change to one side could drift from the other without a build-time signal.

## Residual Risks (explicit)

- **R1 — No automated inequality check.** The 30d constant and the 14d DLT retention live in different
  files (`ProcessedAlertsPurge.java` vs. `docker-compose.yml` / `deploy/docker-compose.services.yml` /
  `deploy/k8s/infra/20-redpanda.yaml`). Nothing fails a build if they drift out of the required
  relationship. Mitigation today is code comment + this ADR + Architecture Spec; a CI assertion is a candidate
  follow-up, not yet built.
- **R2 — Margin, not elimination.** As stated in Consequences/Negative — this bounds the double-act
  window to "purge horizon minus redelivery window," it does not remove the possibility entirely.

## Reversal Cost

`LOW` — the change is a single SQL interval literal in `ProcessedAlertsPurge.java`. No schema
migration, no topic or contract change. Do not lower the interval below the DLT retention + re-drive
latency margin without re-verifying the inequality this ADR states.

## References

- `rpe-alert-service/src/main/java/com/example/rpe/alert/consumer/ProcessedAlertsPurge.java` — the
  implementation; javadoc cites "2026-07 arch-audit R4" verbatim.
- Architecture Spec Known Limitations — "The dedup-guard horizon is a timing contract (arch-audit R4)" bullet.
- `ADR-23.md` — the re-drive loop this purge horizon must outlive.
- `ADR-27.md` — the 30d purge / 14d DLT retention pairing, and the pinned
  `retention.ms`/`segment.ms` values in all three infra files.
- ADR-23 (DLT re-drive strategy), ADR-27 (sibling backfill ADR for the same audit cycle).

## Changelog

| Date | Version | Author | Change |
|---|---|---|---|
| 2026-07-15 | 1.0.0 | amit | Initial — ACCEPTED. Backfills the 2026-07-12 arch-audit R4 finding; documents the purge-horizon-must-exceed-redelivery-window invariant `ProcessedAlertsPurge.java` has enforced since that date. |
