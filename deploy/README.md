# `deploy/` — RPE microservices deployment topology (ADR-17)

This directory holds the **canonical** deployment topology for RPE as four independently
deployable services — the topology the ADR-17 §7 strangler extraction converged on
(Stages 1–5 complete).

- **Decision:** [`docs/adrs/ADR-17.md`](../docs/adrs/ADR-17.md)
- **Rules:** [`ADR-17.md`](ADR-17.md)
- **Topology file:** [`docker-compose.services.yml`](docker-compose.services.yml)
- **`k8s/` is frozen** — delivered Stage-6 exhibit, not maintained; cloud canonical is ECS (`chaosforge-infra/rpe/`). See [`k8s/README.md`](k8s/README.md)

## Services

| Service | Port | Bounded context | Sole writer of | Build context |
|---|---|---|---|---|
| `rpe-detection-service` | 8080 | Ingest + atomic Lua gate + Detectors + outbox **writer** + HTTP surface | all Redis keys; `outbox` rows | ``rpe-detection-service`` ✅ exists (Stage 3) |
| `rpe-relay-service` | 8082 | `outbox` → `payment.alerts`, Kafka-transactional, exactly-once | `outbox` **status** only | `../rpe-relay-service` ✅ exists (Stage 1) |
| `rpe-alert-service` | 8083 | `payment.alerts` → `processed_alerts`, idempotent | `processed_alerts` | `../rpe-alert-service` ✅ exists (Stage 2) |
| `rpe-triage-agent` | 8081 | Advisory LLM enrichment → `payment.alerts.triaged` | `triaged_alerts` | `../rpe-triage-agent` ✅ exists |

All four services are extracted sibling modules (ADR-17 §7 Stages 1–5 complete: extraction,
contract/ArchUnit guards, and this compose as the canonical topology). The repo root carries
no pom — each builds via `mvn -f <svc>/pom.xml verify` and ships its own `Dockerfile`. The alert service
owns `processed_alerts` (its own Flyway, `flyway_schema_history_alert`); detection owns the `outbox`
writer + all Redis keys. Stage 6 (`k8s/` in this directory — CNPG, per-service schemas/roles/GRANTs,
NetworkPolicies, OTel/Tempo stack) is **authored but its in-cluster e2e run is still pending**; until
that run is attested, this compose file remains the only verified topology.

## Principles (enforced by `ADR-17`)

- **Kafka-only, async** between services. A peer down ⇒ backlog (uncommitted offsets / PENDING
  rows that drain on recovery), never an error upstream.
- **Schema-only contracts.** Each service re-declares the messages it uses as local
  `@JsonIgnoreProperties` + `schema_version` records. **No shared `rpe-common` code jar.**
- **One owning writer per table.** `outbox` and `processed_alerts` are *published interfaces*
  (read-only / status-only cross-service access), not shared mutable state.
- **Independent deployability.** Each service boots, `mvn -f <svc>/pom.xml verify`s, and runs with
  every peer absent. Build lifecycles are not reactor-aggregated.
- **Detection core is indivisible.** Never split velocity/z-score/geo/dedup across services — it is
  one atomic Redis round-trip behind a per-account single-threaded lane.

## Usage

```bash
# Prereq: cp .env.example .env  (POSTGRES_PASSWORD, GRAFANA_PASSWORD required)
# Run from the repo root. --env-file is explicit because `-f deploy/…` makes deploy/
# the compose project dir, so .env in the repo root is not auto-discovered.

# Canonical four-service topology (exceeds 8GB lab host comfortably — not alongside load benchmarks)
docker compose --env-file .env -f deploy/docker-compose.services.yml up -d

# Independent-deployability check (rules §4): one service + its infra, peers absent
docker compose --env-file .env -f deploy/docker-compose.services.yml up -d postgres redpanda redis rpe-relay-service

# Single-host all-in-one DEV convenience (infra + detection-on-host via mvn + triage profile)
docker compose up -d                      # repo root
```

## Data isolation

One physical Postgres / Redis / Kafka. Logical isolation is by **single-writer table ownership +
contract-grade access** (ADR-17 §3.4), not database-per-service — the transactional outbox requires
detection (writer) and relay (reader) to share `outbox`. Production isolation path: per-service
Postgres schema + role with GRANTs scoped to the contract columns (ADR-17 §5.3):

```sql
-- illustrative production grants (not applied in the lab artifact)
GRANT SELECT, UPDATE (status, attempts) ON outbox            TO relay_role;
GRANT SELECT                            ON processed_alerts   TO triage_role;
```

## Observability

This canonical compose mounts [`../monitoring/prometheus.services.yml`](../monitoring/prometheus.services.yml),
which scrapes all four services by container DNS (`rpe-detection-service:8080`, `rpe-relay-service:8082`,
`rpe-alert-service:8083`, `rpe-triage-agent:8081`). The repo-root dev compose keeps the host-oriented
[`../monitoring/prometheus.yml`](../monitoring/prometheus.yml) (detection on host at `host.docker.internal:8080`).
Each service exposes its own `/actuator/prometheus`; aggregation is at the scrape layer, not in-app
(ADR-17 §6). No metric may carry `account_id` or any PII / high-cardinality tag (security.md).

## Follow-ups

- Cross-service distributed tracing is wired app-level (ADR-25: Observation → OTel → OTLP, fail-open);
  the collector/Tempo backend ships in `k8s/observability/` only — compose runs drop spans harmlessly.
- Per-service Postgres schemas + scoped GRANTs (ADR-17 §5.3) are authored in `k8s/db/` — Stage 6,
  pending the in-cluster e2e attestation above.
