# `deploy/k8s/` — FROZEN (ADR-17 Stage-6 exhibit; not maintained)

**Status: frozen as delivered.** The canonical **cloud** deployment of RPE is ECS Fargate
(`chaosforge-infra/rpe/*.tf`, sibling repo); the canonical **local** topology is
[`../docker-compose.services.yml`](../docker-compose.services.yml). This k8s stack (CNPG HA,
default-deny NetworkPolicies, Tempo/OTel observability) is the delivered ADR-17 Stage-6 artifact,
kept as a portfolio exhibit — it is **not** kept in parity with either canonical topology.

Consequences of the freeze:

- **Sizing here is not authoritative.** Resource requests/limits in `services/*.yaml` are frozen
  at delivery values; the ECS task definitions are the sole cloud sizing truth (they diverge —
  e.g. detection 512Mi here vs 1024MB on ECS — deliberately unreconciled).
- **The Prometheus config here (`observability/54-prometheus.yaml`) is exempt from the
  cross-repo scrape/rules parity discipline** (that discipline covers `../../monitoring/*.yml`
  and `chaosforge-infra/observability/prometheus.yml` only).
- New alert rules, dashboards, service env vars, or schema changes do **not** get back-ported
  here. If this stack is ever revived, re-derive it from the compose topology + ECS env vars
  rather than trusting these manifests.

The superseded OTel Instrumentation CR formerly at `observability/52-instrumentation.yaml`
lives at `../../docs/adrs/superseded/52-instrumentation.yaml` (ADR-25).
