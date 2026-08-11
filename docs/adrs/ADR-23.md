<!-- edit-log (newest first): v1.0 | 2026-06-29 | Initial. ACCEPTED. -->

---
asset_id: adr-23-dlt-operational-strategy
asset_path: docs/adrs/ADR-23.md
asset_type: adr
version: 1.0.0
created: 2026-06-29
last_updated: 2026-06-29
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [kafka, dlt, dead-letter, re-drive, operations, observability, runbook, adr-18-follow-on]
---

# ADR-23 — DLT operational strategy: operator-gated loop-safe re-drive, per-writer depth SLO alerting, re-process-vs-discard decision tree

## Status

`ACCEPTED`

Supersedes: nothing.
Related: ADR-10 (no auto-replay from DLT — this ADR is the human-gated counterpart), ADR-13
(deterministic UUIDv5 `alert_id` — what makes re-drive idempotent), ADR-17 (single-writer
ownership), ADR-18 (one DLT per consumer group; the triage DLT owner), ADR-19 (Actuator is the
only HTTP surface — why re-drive is NOT an endpoint), ADR-20 (least-privilege ACLs),
ADR-22 (graceful shutdown — the other "narrow the loss window, don't replace durability" story),
`ADR-23.md`, `ADR-24.md`.

## Context

ADR-18 fixed DLT *ownership* (one DLT per consumer group, single-writer). But an audit found the
DLT surface was **write-only with no operational story**:

1. **No re-drive path.** Once a record hit a DLT there was no sanctioned way back. "Replay the DLT"
   was tribal knowledge — and ADR-10 rightly forbids *automatic* replay, but never defined the
   *manual* path, the loop-safety, or the decision criteria.
2. **A latent ownership bug.** `rpe-triage-agent`'s `DeadLetterPublishingRecoverer` resolved its
   destination as `record.topic() + ".DLT"` → **`payment.alerts.DLT`**, which is
   `rpe-alert-service`'s sole-writer topic. This directly violated ADR-18 and the Architecture Spec
   Immutable Constraint, and ai-triage-rules §7's mandated routing test was **absent**, so nothing
   caught it. In a SASL stack the broker would reject the misdirected write (ADR-20 ACLs); in lab
   PLAINTEXT it silently wrote to the wrong owner's topic.
3. **Incomplete, mis-owned depth observability.** `DltDepthMetrics` lived only in
   `rpe-detection-service` and polled `payment.events.DLT` **and** `payment.alerts.DLT` — a topic
   detection does not own — while `payment.alerts.triage.DLT` was monitored by **no one**.
4. **No alerting.** `monitoring/` had scrape configs but **zero alert rules**. A DLT could fill
   indefinitely with no signal beyond a log line.

A DLT strategy that ignores the re-drive loop problem is a footgun: a genuinely-poison record
re-driven to its source fails again and re-DLTs forever. And re-drive must never become an
always-on automated consumer (ADR-10) — that re-introduces the poison-loop and an unbounded retry
of records that are broken by definition.

## Decision

Make the DLT a **closed operational loop**: deliberate human re-drive, loop-safe by construction,
idempotency-backed, per-writer observable, with codified decision criteria. Enforcement detail in
`ADR-23.md`.

**1. Re-drive is operator-initiated, never automated** (extends ADR-10). The only sanctioned path
from a DLT back to its source is `deploy/kafka/dlt-redrive.sh`, run by a human after the decision
tree. There is no DLT-consuming service and no re-drive HTTP endpoint — adding one would breach
ADR-19 (Actuator is the only HTTP surface) and re-introduce the poison-loop ADR-10 rejected. The
tool is **dry-run by default** (`--execute` required to act) and matches the existing
`provision-acls.sh` operator-script pattern.

**2. Loop-safety via attempt-cap → parking topic.** Each re-driven record carries an
`x-redrive-attempts` header. At the cap (default 3, `REDRIVE_MAX_ATTEMPTS`) the record is sent to a
**parking topic** `<dlt>.parked`, **never** back to source. A poison record can therefore cycle at
most N times, then halts in a quarantine that pages. The tool's DLT→source mapping is a
**fail-closed allowlist** — an unlisted DLT cannot be re-driven.

**3. Re-drive is safe by construction** because every source path already dedups on replay — this
is why automatic *redelivery* (ADR-02/09/13) was always safe, and re-drive rides the same guards:

| Source topic | Idempotency guard on re-drive |
|---|---|
| `payment.events` | detection Lua dedup window + deterministic UUIDv5 `alert_id` (ADR-13) |
| `payment.alerts` (alert-service) | `processed_alerts` `ON CONFLICT DO NOTHING` (UUIDv5 PK) |
| `payment.alerts` (triage) | triage inbox `triaged_alerts` `ON CONFLICT DO NOTHING`, before the LLM (ADR-15) |

A re-driven duplicate is absorbed, not double-actioned. The tool preserves the original
`kafka_dlt-*` failure headers (forensic trail) and only *adds* the re-drive stamp.

**4. Per-writer depth ownership.** Each service exposes `rpe.dlt.depth{topic=...}` **only for the
DLTs it solely writes** (per-service `DltDepthMetrics` copy — no shared jar, symmetric with
`Pii`/`KafkaSecurity`/`BoundaryHandler`):

| Service | Monitors |
|---|---|
| `rpe-detection-service` | `payment.events.DLT` + `payment.events.DLT.parked` |
| `rpe-alert-service` | `payment.alerts.DLT` + `payment.alerts.DLT.parked` |
| `rpe-triage-agent` | `payment.alerts.triage.DLT` + `payment.alerts.triage.DLT.parked` |

This fixes both the coverage gap (triage DLT now monitored) and the boundary smell (detection no
longer polls a topic it does not own).

**5. Depth SLO alerting** (`monitoring/rules/dlt.rules.yml`, mounted into every Prometheus): a
sustained non-empty DLT (`RpeDltBacklog`, warn), a growing DLT (`RpeDltBacklogGrowing`, warn — a
systematic reject, fix root cause first), and **any** non-empty parking topic (`RpeDltParked`,
critical/page — re-drive gave up).

**6. The triage routing bug is fixed in the same change** (prerequisite): the destination is pinned
to the constant `payment.alerts.triage.DLT` via a testable resolver, with a unit test asserting it
never resolves to `payment.alerts.DLT` (satisfies ai-triage-rules §7 without a broker — the triage
suite is container-free by design).

**7. Least-privilege re-drive principal.** Re-drive is an *operator* action, not a service: an
optional `rpe-operator` SCRAM principal (provisioned only where the script runs) gets READ on the
DLTs and WRITE on the sources + parking topics. Unset ⇒ re-drive is simply not possible (fail-safe).

## Alternatives Considered

| Option | Decision | Reason |
|---|---|---|
| In-app re-drive REST endpoint | Rejected | Breaches ADR-19 (Actuator is the only HTTP surface); adds an authenticated mutation API + attack surface for a rare human action. |
| Always-on DLT-consumer that auto-re-drives | Rejected | Re-introduces the exact poison-loop ADR-10 rejected; unbounded retry of records broken by definition; burns budget; no human in the loop for the fix-vs-discard call. |
| Re-drive with no attempt cap | Rejected | A genuinely-poison record ping-pongs DLT↔source forever. The cap→parking topic is the single most important safety property of any re-drive tool. |
| Keep detection polling all DLTs | Rejected | Detection monitoring `payment.alerts.DLT`/`triage.DLT` is a cross-ownership smell; per-writer ownership (microservices.md §6) is the correct model and was needed anyway to cover the triage DLT. |
| One shared parking topic | Rejected | Conflates failure domains exactly as ADR-18 rejected for the DLTs themselves; per-DLT parking keeps ownership and retention scoped. |
| Suffix-derive the triage DLT (`record.topic()+".DLT"`) | Rejected | That **is** the bug — it resolves to another service's sole-writer topic. Pinning to the owned constant is the fix. |

## Consequences

**Positive:** the DLT is now a closed loop — fill, observe, decide, drain or quarantine — with every
step bounded and observable. The ADR-18 single-writer invariant is now actually upheld for the
triage DLT (and test-guarded). Re-drive cannot loop, cannot corrupt (idempotency table per path),
and cannot run by accident (dry-run default, fail-closed allowlist, optional principal). Depth is
correctly owned and fully covered; a stuck DLT and an exhausted re-drive both page. All suites green
(detection 76, relay 5, alert 10, triage **31** — +3 routing tests).

**Negative:** three more topics per environment (`*.DLT.parked`) to declare; two more per-service
`DltDepthMetrics` copies to keep in sync (no-shared-jar tax); the re-drive script is operator tooling
outside `mvn verify` (like `provision-acls.sh`) — its correctness rests on the documented
idempotency guards (which *are* tested: alert poison→DLT, triage replay) plus a manual verification
procedure, not an automated test.

## Residual Risks (explicit)

- **R1 — Script not exercised by CI.** `dlt-redrive.sh` is bash+rpk operator tooling; `mvn verify`
  does not run it. Mitigation: the routing correctness and replay-idempotency it depends on *are*
  unit/integration tested; the script is dry-run-default and fail-closed. A live re-drive smoke test
  is a manual-profile follow-up, like the SASL smoke test (ADR-20).
- **R2 — rpk version drift.** The script assumes rpk v24.1.x `--format json` (matches the
  compose/k8s images). A future rpk that changes the consume JSON shape would need the parse updated;
  the script documents the fallback (`--print-headers`).
- **R3 — Re-drive into an unfixed root cause.** Re-driving a *systematic* failure (bad deploy, schema
  break) just re-fills the DLT and burns attempts toward parking. The decision tree mandates root-
  cause-first for `RpeDltBacklogGrowing`; `RpeDltBacklog` (stale, not growing) is the re-drivable case.
- **R4 — Parking is quarantine, not durability.** Parked records sit on a topic with normal
  retention. A parked record left past retention is lost. `RpeDltParked` pages at `for: 0m` precisely
  so the fix-vs-discard decision happens well inside the retention window.
- **R5 — Operator principal crosses ownership by design.** `rpe-operator` writes topics it does not
  "own" — that is intrinsic to re-drive and is why it is human-gated, optional, and provisioned only
  where the script runs. It is explicitly not a service principal.

## Reversal Cost

`LOW` — delete the script, the rules file (+ its mounts), the two new `DltDepthMetrics` copies, and
the parking-topic declarations; revert detection's topic list. The triage routing fix is a
strict correctness improvement and would stay regardless. No data-format or contract change.

## References

- `ADR-23.md` — re-drive binding rules + the re-process-vs-discard decision tree
- `deploy/kafka/dlt-redrive.sh` — the re-drive tool; `deploy/kafka/provision-acls.sh` — `rpe-operator` ACLs
- `monitoring/rules/dlt.rules.yml` — depth SLO alert rules (and the k8s ConfigMap copy)
- ADR-10 (no auto-replay), ADR-13 (deterministic alert_id), ADR-18 (DLT ownership), ADR-19 (HTTP surface)
