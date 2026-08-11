---
adr_id: ADR-17
adr_status: ACCEPTED
adr_decided_on: 2026-06-17
adr_reversal_cost: MEDIUM
adr_supersedes: null
adr_superseded_by: null
---

<!-- edit-log (newest first): v1.0 | 2026-06-17 | Initial — ACCEPTED. Decomposes the modular monolith into four independently deployable services along correctness-safe seams. Ratifies ADR-17.md. -->

# ADR-17 — Decompose RPE into independently deployable services along correctness-safe seams

## Status
ACCEPTED

## Owner
amit

## Related
- ADR-01 (runtime surface split — survives intact, now per-service)
- ADR-05 (`RelayTrigger` interface — the relay's designed extraction seam)
- ADR-06 (`RELAY_INSTANCE_ID` per-instance `transactional.id` — already multi-instance safe)
- ADR-11 (opaque relay payload + `@JsonIgnoreProperties` + `schema_version` — the schema-only contract mechanism)
- ADR-13 (deterministic UUIDv5 `alert_id` — makes cross-service idempotency free)
- ADR-15 (triage agent — the first service split; this ADR generalizes its pattern)
- ai-triage-rules.md §1.4 (shared contract = DTO schema, not code — the precedent we standardize)

---

## 1. TL;DR

RPE is already a **modular monolith with one extracted service** (`rpe-triage-agent`, ADR-15).
We make the decomposition explicit and complete it: **four independently deployable services**
bounded by the correctness contexts that already exist in the code —

| Service | Bounded context | Owns (data) | Topics |
|---|---|---|---|
| `rpe-detection-service` | Ingest + atomic detect + persist alert-intent | **all** Redis state; **writes** `outbox` | consumes `payment.events`; → `payment.events.DLT` |
| `rpe-relay-service` | Exactly-once delivery (outbox → topic) | reads/transitions `outbox` status | produces `payment.alerts` |
| `rpe-alert-service` | Idempotent alert actioning | **owns** `processed_alerts` | consumes `payment.alerts`; → `payment.alerts.DLT` |
| `rpe-triage-agent` (ADR-15) | Advisory LLM enrichment | **owns** `triaged_alerts` | consumes `payment.alerts`; → `payment.alerts.triaged` |

Services communicate **only** over Kafka (async). Contracts are shared as **versioned message
schemas, never as a shared code jar** (ADR-11 / ai-triage-rules §1.4, generalized). Each service
boots, tests, and ships with every other service absent.

**This ADR draws the boundaries and ratifies the rules; the physical extraction is a sequenced
strangler-fig migration (§7) — relay first, then alert, then detection-is-what-remains — because
each lift must keep the others' test suites green and cannot be big-banged.**

**Two hard non-goals, stated up front, because they are the whole point of doing this with judgment:**

1. **The detection core is NOT decomposed further.** Velocity, z-score, geo, and dedup run in
   **one atomic Redis Lua round-trip** behind a **per-account single-threaded lane**. Splitting
   them into "velocity-service / zscore-service / geo-service" destroys atomicity, ordering, and
   the dedup-last correctness invariant, and multiplies Redis round-trips. That is a distributed
   monolith wearing a microservices costume. We do not build it.
2. **No shared business-code module ("`rpe-common`" / "`rpe-contracts`" jar).** A shared DTO jar
   recouples every service at compile time: one schema change forces a lockstep rebuild and
   redeploy of all four — the distributed-monolith anti-pattern. Contracts are duplicated per
   service as `@JsonIgnoreProperties` + `schema_version` records (the triage module already does
   exactly this with `PaymentAlert`).

---

## 2. Context

### 2.1 What the codebase actually is today

- **Core (`revenue-protection-engine`)** — a single Spring Boot deployable containing four
  distinct responsibilities that already talk to each other through durable, async boundaries
  (Kafka topics + the Postgres `outbox`), not in-process method calls on the hot path:
  1. **Ingest + detect** — `PaymentEventConsumer` → per-account VT lane → `RedisGate` (atomic Lua)
     → `Detector`s → `OutboxBatchWriter`. The HTTP/actuator surface (WebFlux/Netty) lives here.
  2. **Relay** — `OutboxRelay` + `AlertPublisher` + `PostgresNotifyRelayTrigger`: a background
     loop that drains `outbox` → `payment.alerts` with a Kafka-transactional producer.
  3. **Alert actioning** — `AlertConsumer`: `payment.alerts` → `processed_alerts`
     (`ON CONFLICT DO NOTHING`).
- **Triage (`rpe-triage-agent`)** — already a separate Spring Boot module, **not** aggregated by
  the core pom, own port (8081), own Dockerfile, own DB table, own compose profile (ADR-15).

The seams are not aspirational — they are physical. The relay and the consumer never share a
thread or a transaction with detection; they share **Kafka topics** and the **`outbox` table**.

### 2.2 The architecture has been telling us to do this

The code carries explicit extraction affordances that only make sense if separation was always
the intent:

- `RelayTrigger.signal()` is documented: **"no-op when relay is extracted"** (ADR-05).
- The relay producer takes its `transactional.id` from `RELAY_INSTANCE_ID` env, **"never shared"**,
  precisely so N relay instances can run as independent processes (ADR-06).
- The relay treats `outbox.payload` as an **opaque `JsonNode`** and never deserializes it to a DTO,
  so "event DTO evolution has zero relay impact **by design**" (ADR-11).
- The triage module shares **only the alert schema, not code** — it re-declares `PaymentAlert`
  rather than importing `AlertMessage` (ai-triage-rules §1.4). This is the contract pattern for
  every future service.

### 2.3 Forces in tension

| Force | Direction | Notes |
|---|---|---|
| Independent deployability / scaling | Split into services | Relay and detection have opposite scaling profiles (DB-bound batch vs. CPU/VT fan-out) |
| Atomic-gate correctness | Keep detection indivisible | One Redis round-trip, per-account ordering, dedup-last are non-negotiable |
| Exactly-once delivery | Keep outbox shared writer↔reader | Transactional outbox *requires* a shared table between producer and relay |
| Loose coupling / independent evolution | No shared code jar | Schema-only contracts; additive evolution (ADR-11) |
| Lab artifact constraints (8GB host, synthetic data) | Don't over-engineer | No service mesh, no per-service DB cluster, no saga framework |

---

## 3. Decision

Adopt a **service-per-bounded-context** topology with the four services in §1. Bind the following
sub-decisions; the enforceable detail lives in `ADR-17.md`.

### 3.1 Service boundaries are correctness boundaries

A service == one bounded context == one deployable. The four are fixed. The **detection core is a
single indivisible service** (non-goal #1). New detection rules are new `Detector`s **inside**
detection-service, never new services.

### 3.2 Inter-service communication is Kafka-only and asynchronous

No synchronous service-to-service HTTP on any path. The hot path already commits the Kafka offset
without waiting on Postgres or on any downstream service; that property is now a cross-service
guarantee. A service being down adds latency or backlog, never an error to its upstream.

### 3.3 Contracts are versioned schemas, not shared code (generalizes ADR-11)

Every message a service consumes or produces is declared as a **local** record in that service,
annotated `@JsonIgnoreProperties(ignoreUnknown = true)` with a `schema_version` field. Producers
evolve schemas **additively only**. There is no `rpe-common` code module (non-goal #2). Dependency
*version* alignment (Boot, R4j, Jackson pins) is handled by convention/per-pom, not by a shared
business jar.

### 3.4 Data ownership: one owning writer per table; shared tables are published interfaces

Strict database-per-service is **deliberately not adopted** (see §4 and §5.2) because the
transactional-outbox spine requires detection (writer) and relay (reader) to share the `outbox`
table. Instead we decompose the database into **single-owner schemas with contract-grade
cross-service access**:

| Table / store | Owning service | Cross-service access | Contract |
|---|---|---|---|
| Redis (all keys) | detection | triage `redisAccountHistory` tool — **read-only** | key shapes (lua-gate.md) |
| `outbox` | detection (**sole writer of rows**) | relay — reads + transitions `status` only | row schema + `PENDING→PUBLISHED/FAILED` state machine |
| `processed_alerts` | alert-service (**sole writer**) | triage `recentAlertsForAccount` tool — **read-only** | row schema |
| `triaged_alerts` | triage | none | — |

The `outbox` table is treated as a **published interface** (like a durable queue), not as shared
mutable state. Its schema and status state machine are the contract; ADR-11's `FOR EACH STATEMENT`
trigger and opaque payload already make it schema-evolution-safe.

### 3.5 Each service is independently runnable, buildable, and testable

- Own `pom.xml` on the Spring Boot parent; **no reactor aggregation** that couples build lifecycles
  (consistent with ADR-15: "the core build never sees Spring AI"). `mvn -f <svc>/pom.xml verify`
  builds any service alone.
- Own `application.yml`, own Flyway migrations **scoped to the tables it owns**, own Dockerfile,
  own actuator/Prometheus surface, own Kafka consumer group, own Resilience4j boundaries
  (no global defaults — existing rule).
- **Liveness with peers absent:** detection runs with relay/alert/triage all down (the `outbox`
  simply accumulates; `rpe.outbox.pending.age_seconds` rises and alerts). Relay runs with detection
  down (drains whatever `outbox` already holds). Alert-service runs with everything else down
  (consumes whatever is on `payment.alerts`). Triage already mandates this (ADR-15 §3.7). This is
  now a **rule**, asserted per service, not an accident.

### 3.6 The runtime surface split (ADR-01) survives — now per service

Each service keeps the thread model its workload demands: detection = WebFlux/Netty HTTP +
VT lanes + reactive Lettuce + platform-pool JDBC; relay = VT loop + platform-pool JDBC +
transactional producer; alert-service = VT consumer + platform-pool JDBC; triage = MVC + VT
(ADR-15). ADR-01 is unchanged; it is simply applied four times.

### 3.7 Scope of binding

- **Applies to:** the whole workspace — every service module, the topic contracts, the table
  ownership map, the build/deploy topology.
- **Does not change:** any detection-path correctness invariant (ADR-13/14, lua-gate.md), the
  exactly-once delivery mechanism (ADR-02/05/06), or ADR-15's triage rules. This ADR is a
  **structural** decision; it moves code across deployables, it does not alter behavior.
- **Exceptions via:** successor ADR only.

---

## 4. Alternatives Considered

| Option | Chosen / Rejected | Reason |
|---|---|---|
| **Service-per-bounded-context (4 services), Kafka-only, schema contracts, shared outbox as published interface** | **Chosen** | Maps deployables to the correctness contexts that already exist; preserves the atomic gate, ordering, and exactly-once; matches the affordances the code was built with (ADR-05/06/11/15) |
| Keep the modular monolith as-is | Rejected | The seams, scaling profiles, and the already-extracted triage module make the monolith boundary arbitrary; relay/detection have opposite scaling needs |
| Nano-services: split detection into velocity/zscore/geo services | **Rejected (hard non-goal)** | Destroys the atomic single-round-trip Lua gate, per-account ordering, and the dedup-last invariant; multiplies Redis round-trips; reintroduces distributed-transaction problems the design exists to avoid |
| Shared `rpe-common`/`rpe-contracts` code jar for DTOs | **Rejected (hard non-goal)** | Compile-time recoupling → lockstep rebuild/redeploy of all services on any schema change = distributed monolith. Contradicts ADR-11 / ai-triage-rules §1.4, which the triage module already proves works |
| Strict database-per-service (own DB per deployable) | Rejected for this artifact; documented as production path | Would require abandoning the transactional outbox (losing exactly-once) **or** an event-carried-state-transfer rebuild — out of scope for a synthetic lab artifact. Production path: per-service Postgres schemas + roles, GRANTs scoped to the contract tables (§5.3) |
| Synchronous REST/gRPC between services | Rejected | Couples availability and latency; the hot path is built to commit offsets without waiting downstream — sync calls would throw that away |
| Service mesh / saga orchestrator | Rejected | No distributed write transaction exists to coordinate (the outbox + idempotent consumers already give exactly-once); a mesh is pure overhead on an 8GB lab host |

---

## 5. Consequences

### 5.1 Positive

- **Independent scaling and deployment.** Relay (DB-batch-bound) scales on `RELAY_INSTANCE_ID`
  instances; detection (VT fan-out) scales on partitions; triage (LLM-bound) scales under its
  RateLimiter — each on its own cadence, each redeployable without touching the others.
- **Blast-radius isolation made structural.** A crash, bad deploy, or memory spike in one service
  cannot take down another. Triage already proves it (kill the LLM, alerts still flow); now it
  holds for relay and alert-service too.
- **The architecture's existing dividends pay out.** UUIDv5 (ADR-13) makes every consumer
  idempotent for free; opaque payload (ADR-11) makes the relay immune to schema drift; the
  `RelayTrigger` seam (ADR-05) was built for this.
- **Clear ownership.** One service writes each table; one team could own each deployable.

### 5.2 Negative / accepted trade-offs

- **Shared Postgres instance, not database-per-service.** detection+relay share `outbox`;
  alert-service+triage read each other-adjacent tables. This is the honest, documented cost of
  keeping the transactional outbox. The mitigation is **single-writer ownership + contract-grade
  access**, not physical DB isolation. Anyone reading this expecting textbook DB-per-service should
  read §4 row 5 and §5.3 first — it is a deliberate decision, not an omission.
- **Operational footprint grows.** Four deployables, four health surfaces, four consumer lags to
  watch. On the 8GB lab host the full topology is not meant to run alongside load benchmarks
  (same constraint ADR-15 already noted for triage).
- **Schema duplication.** The alert contract is declared in three places (detection's
  `AlertMessage`, triage's `PaymentAlert`, and now alert-service's own copy). This is the
  intended cost of decoupling — additive evolution + `schema_version` keeps them compatible
  without a shared jar. A contract-test per consumer (rules §7) is the safety net.
- **Distributed debugging.** A delayed alert now spans three services; correlation rides the
  masked `event_id`/`account_id` in structured logs (existing MDC propagation) plus the
  deterministic `alert_id` as the cross-service join key.

### 5.3 Operational / production path

- **DB isolation upgrade:** per-service Postgres schema + role; GRANT relay `SELECT`/`UPDATE(status,
  attempts)` on `outbox` only; GRANT triage `SELECT` on `processed_alerts` only. The single-writer
  contract (§3.4) is enforced by the DB, not just by convention.
- **Topology durability:** lab is RF=1; production is RF≥3 / `min.insync.replicas=2` (unchanged
  from the existing Known Limitations).
- **Observability:** each service exposes its own `/actuator/prometheus`; no metric may carry
  `account_id` or any PII/high-cardinality tag (security.md, unchanged). Cross-service tracing is
  the documented next step.

---

## 6. Failure Modes

### 6.1 A downstream service is down
- **Trigger:** relay, alert-service, or triage offline.
- **Blast radius:** backlog only. detection keeps ingesting; `outbox` grows (relay down),
  `payment.alerts` lag grows (alert-service down), `payment.alerts.triaged` stalls (triage down).
- **Detection:** `rpe.outbox.pending.age_seconds`, `rpe.consumer.lag`, triage lag gauges.
- **Mitigation:** built-in — uncommitted offsets / PENDING rows drain on recovery. No data loss
  because nothing is dropped to make room.

### 6.2 Someone tries to split the detection core
- **Trigger:** a PR adds `rpe-velocity-service` or moves a `Detector` out of detection-service.
- **Blast radius:** would break gate atomicity, ordering, and dedup-last silently.
- **Detection:** `ADR-17.md` §1 + this ADR §3.1 are the gate; review must
  reject it. (An ArchUnit/test guard that fails if the Lua gate or `Detector` set is referenced
  across a service boundary is the codified backstop — rules §7.)
- **Mitigation:** non-goal #1 is binding; exceptions require a successor ADR.

### 6.3 A shared-code "contracts" jar is introduced
- **Trigger:** a PR adds `rpe-common` and points services at it for DTOs.
- **Blast radius:** silent recoupling; the next schema change forces a lockstep multi-service
  redeploy.
- **Detection:** rules §2; review rejects any cross-service `import` of another service's package.
- **Mitigation:** non-goal #2 is binding; contracts stay duplicated + versioned.

### 6.4 Central assumption wrong — the services don't actually deploy independently
- **Trigger:** extracting relay breaks detection's tests because they were entangled.
- **Blast radius:** the migration stalls at that step (which is why it is sequenced, not big-bang).
- **Detection:** each extraction step (§7) must leave every other service's `mvn verify` green
  before it merges.
- **Mitigation:** strangler sequence with per-step green gate; a step that can't keep peers green
  reveals a hidden coupling to fix before proceeding.

---

## 7. Migration sequence (strangler-fig — owner: amit)

Establishing the boundaries (this ADR + rules + Architecture Spec + `deploy/` topology) is **done now**.
The physical code extraction follows the sequence below. It is **explicitly not big-banged**: each
step must keep every other service's test suite green before merge, because the core integration
tests (e.g. `scenario6_relayExactlyOnce`) exercise the relay end-to-end and their Testcontainers
topology must be re-homed as ownership moves.

- [x] **Stage 0 — Govern.** ADR-17 ACCEPTED; `ADR-17.md` ratified; Architecture Spec
      Service Topology section; `deploy/docker-compose.services.yml` target topology authored.
- [x] **Stage 0 — Recognize triage.** `rpe-triage-agent` formally counted as service #4 (already
      extracted under ADR-15; no code move).
- [x] **Stage 1 — Extract `rpe-relay-service`** (cleanest seam, designed for it: ADR-05/06/11).
      Moved `OutboxRelay`, `AlertPublisher`, `RelayTrigger`/`PostgresNotifyRelayTrigger`,
      `RelayListenerHealthIndicator`, the `@Qualifier("relay")` transactional producer (→
      `RelayKafkaConfig`), the `jdbcScheduler` (→ `RelaySchedulerConfig`), the relay slice of
      `RpeProperties` (→ standalone `RelayProperties`), the `purge_old_records()` schedule; copied
      `Pii` (no shared code). Relay owns the `outbox` *reader*/status contract; detection keeps the
      *writer*. Core's in-process `RelayTrigger.signal()` was **removed** (not stubbed): cross-process
      wakeup is the V4 `pg_notify` trigger — the literal "no-op when relay is extracted" of ADR-05.
      Relay runs Flyway-disabled (owns no tables; reads the published outbox contract).
      `scenario6_relayExactlyOnce` re-homed as the relay's own seed-outbox→exactly-once test; core
      scenarios 1 & 7 re-scoped to assert at the outbox boundary. **Verified:** core `mvn verify`
      52/52 green with the relay absent; relay `mvn verify` 3/3 green; triage untouched (20/20).
- [x] **Stage 2 — Extract `rpe-alert-service`.** Moved `AlertConsumer` (→ `com.example.rpe.alert.*`)
      + the `alert*` Kafka factory beans + the DLT producer/recoverer (→ `AlertKafkaConfig`) +
      `jdbcScheduler` (→ `AlertSchedulerConfig`); alert-service declares its own `AlertMessage`
      schema copy and owns `processed_alerts` (core V2 migration → alert-service V1, with a separate
      `flyway_schema_history_alert` history table, mirroring triage). Core's `payment.alerts`
      consumer + `AlertMessage` import removed from `KafkaConfig`. Re-homed scenarios 2 (dedup) & 4
      (poison→DLT) as the alert-service's own consumer-level tests; core kept scenarios 1/3/5/7.
      **Known shared-DB coupling (accepted, §5.2):** `purge_old_records()` (detection's V3) still
      DELETEs from `processed_alerts`, a table alert-service now owns — a shared retention utility on
      the shared Postgres, invoked by the relay. Splitting it cleanly is a Stage 6 (DB-per-service)
      concern. **Verified (clean builds):** core 50/50 (alert + relay absent), alert 5/5, relay 3/3,
      triage 20/20.
- [x] **Stage 3 — `rpe-detection-service` is what remains.** The root module's `src/` + `pom.xml`
      were `git mv`'d into a top-level `rpe-detection-service/` sibling (resolving the deploy compose's
      `rpe-detection-service` build context); artifactId/name → `rpe-detection-service`; own
      `Dockerfile` added (EXPOSE 8080, WebFlux). The Java package (`com.example.rpe`) is unchanged, so
      **zero source edits** — the atomic Lua gate, per-account lanes, and `Detector`s are byte-identical
      (git tracked every file as a pure rename). The root no longer carries a pom (no reactor
      aggregation, §3.5); all four services are now symmetric `mvn -f <svc>/pom.xml verify` siblings.
      It owns Redis + the `outbox` *writer* + the HTTP/actuator surface only. **Verified (clean
      builds):** detection 50/50, relay 3/3 (peers unaffected — empty `<relativePath/>`, no linkage to
      the old root pom; alert 5/5 + triage 20/20 unchanged from Stage 2).
- [x] **Stage 4 — Contract tests + ArchUnit guards.** Per-consumer additive-evolution contract
      tests: `PaymentEventContractTest` (detection / `payment.events`), `AlertMessageContractTest`
      (alert-service / `payment.alerts`), `PaymentAlertContractTest` (triage / `payment.alerts`) —
      each asserts unknown-field tolerance (`@JsonIgnoreProperties`, ADR-11) + optional-field default,
      using the Jackson 2 mapper that mirrors Spring Kafka's `JsonDeserializer`. ArchUnit
      `noCrossServicePackageDependencies()` added to all four services (anchored package markers —
      `com.example.rpe.consumer..` etc. — to avoid colliding with siblings' own `consumer`/`config`
      subpackages); detection adds `detectorImplementationsResideOnlyInDetectionPackage()` — the
      Lua-gate/`Detector`-set confinement guard with real subjects (the three `Detector`s) that fails
      the moment a rule is relocated toward extraction (§6.2). Triage gained its first ArchTest
      (archunit dep added), bringing it to sibling parity. **Verified (clean builds):** detection
      55/55, alert 9/9, relay 4/4, triage 27/27.
- [x] **Stage 5 — Flip deployment.** `deploy/docker-compose.services.yml` is now the canonical
      four-service topology (header reworded from "TARGET" to "CANONICAL"; all build contexts exist).
      It mounts a dedicated `monitoring/prometheus.services.yml` that scrapes all four services by
      container DNS (`rpe-detection-service:8080`, `rpe-relay-service:8082`, `rpe-alert-service:8083`,
      `rpe-triage-agent:8081`) — closing the deploy/README "not yet wired" follow-up for relay/alert
      scrape targets. The root `docker-compose.yml` is retained and explicitly re-labeled the
      all-in-one **dev convenience** (infra + detection-on-host via `mvn` + triage profile; keeps its
      host-oriented `monitoring/prometheus.yml`). **Verified:** `docker compose --env-file .env -f
      deploy/docker-compose.services.yml config` renders valid; the canonical Prometheus mount and all
      four service targets resolve.
- [x] **Stage 6 — Kubernetes canonical topology with DB isolation + observability.**
      `deploy/k8s/` delivers: CNPG v1.29.1 HA cluster (2-instance; `rpe-pg-1` primary + hot standby);
      per-service Postgres schemas (`detection`, `alert`, `triage`) with login roles (`detection_role`,
      `relay_role`, `alert_role`, `triage_role`); schema ownership + `search_path` via bootstrap Job
      (`12-bootstrap-grants.yaml`); post-Flyway column-level tightening (`13-post-flyway-grants.yaml`:
      relay restricted to `GRANT UPDATE(status,attempts)` on `detection.outbox`); Flyway scoped to owning
      schema per service (`schemas:`, separate `flyway_schema_history_*` tables); per-service
      `kubernetes.io/basic-auth` Secrets; purge split to owning services (`OutboxBatchWriter.purgeOutbox`
      in detection, `ProcessedAlertsPurge.purge` in alert — relay role has no DELETE). Kubernetes
      topology: Redpanda + Redis infra; four service Deployments with `imagePullPolicy: Never` (OrbStack);
      default-deny NetworkPolicies with per-service egress/ingress allow-paths (ADR-17 "Kafka-only async"
      enforced at network layer); OTel Operator + Collector (PII-suppress processor for `account_id`) +
      Tempo + Grafana 12.0.1 + Prometheus (5/5 targets `up`; NodePorts 31003/31090). OTel Java
      auto-instrumentation annotations in all service manifests (init container injection verified;
      OrbStack single-node memory constraint prevents co-resident old+new pod during rollout).
      **Verified e2e in k8s:** velocity alert → `detection.outbox PUBLISHED` → `payment.alerts` →
      `alert.processed_alerts`; alert UUIDs deterministic across all three service boundaries.

**Code/config anchors:** module poms (one per service), `application.yml` per service, Flyway
migrations re-homed to owning service, per-service Dockerfile, `deploy/docker-compose.services.yml`,
`ADR-17.md`.

---

## 8. Reversal Cost

**Declared:** MEDIUM. The decision (this ADR + rules + topology) reverts by superseding ADR. Each
extraction stage is independently revertible by re-merging the moved module back into the core pom;
no data migration is required (table ownership is logical, the physical Postgres instance is shared),
no topic contract changes, no external API. Cost is MEDIUM rather than LOW only because, once Stages
1–3 land, four build/deploy pipelines exist where one did — collapsing them back is mechanical but
non-trivial.

---

## 9. Validating Assumptions

| # | Assumption | Status | What would invalidate it |
|---|---|---|---|
| 1 | Relay extracts cleanly via the ADR-05/06/11 seams with no hidden in-process coupling to detection | INFERRED — strongly supported by opaque-payload + per-instance `transactional.id` design | A compile/wiring dependency from `OutboxRelay` into detection internals surfaces during Stage 1 |
| 2 | Schema-only contracts (no shared jar) keep three copies of the alert schema compatible | VERIFIED in principle — triage already does this with `PaymentAlert` under ADR-11 | A non-additive schema change ships without a contract test catching the break |
| 3 | The shared-Postgres / single-writer-per-table model is sufficient isolation for the artifact | ACCEPTED trade-off — not a correctness risk, a purity trade-off | A real multi-tenant/regulated deployment needs DB-per-service → take Stage 6 |
| 4 | Each service can boot and pass tests with all peers absent | INFERRED — true for triage today; to be asserted per service in Stages 1–3 | An extraction step cannot keep peers green → hidden coupling to resolve first |

---

## 10. Open Questions / Followups

- **Cross-service distributed tracing — Stage 6 delivered.** OTel Operator + Collector + Tempo +
  Grafana deployed in `deploy/k8s/observability/`. Java auto-instrumentation CR (superseded by ADR-25, now archived at `docs/adrs/superseded/52-instrumentation.yaml`)
  with PII-suppress processor (`account_id` masked before leaving pod boundary). OrbStack single-node
  memory constraint prevents simultaneous rolling rollout; production path requires `Recreate` or
  higher per-node memory. Correlation via masked `event_id`/`account_id` + deterministic `alert_id`
  continues as the lightweight path on compose topology.
- **`deploy/k8s/` frozen as the Stage-6 exhibit (2026-07-25).** The canonical cloud deployment is
  ECS Fargate (`chaosforge-infra/rpe/`, sibling repo); local canonical stays
  `deploy/docker-compose.services.yml`. The k8s stack is retained as delivered, exempt from
  sizing/scrape/rules parity — see `deploy/k8s/README.md` for the freeze terms.
- **Per-service Postgres schema + scoped roles — Stage 6 delivered.** See §7 Stage 6 entry above.
  Physical DB-per-service is still a non-goal (§4 / §5.2); schema isolation with scoped GRANTs is the
  documented boundary for this artifact.
- Schema-registry-backed contract enforcement for topics — candidate once more consumers exist.

---

## Changelog

| Date | Version | Author | Change |
|---|---|---|---|
| 2026-06-17 | 1.0.0 | amit | Initial — ACCEPTED. Four-service decomposition along correctness-safe seams; ratifies `microservices.md`. |
