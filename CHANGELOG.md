# Changelog

Version history for revenue-protection-engine. Newest first. Each entry is a documentation/design
milestone; test counts are per service (detection / relay / alert / triage) at that point in time.

> This file was extracted from the project's working edit-log so that version history lives here
> (where a reader expects it) rather than inline in the AI-context file, where a reader — human or
> AI — can mistake past states for current ones.

## v1.16 — 2026-07-25

Audit closure remainder + v1.15 verification debt cleared:

- **v1.15's "honest caveat" is closed:** full `mvn clean verify` ×4 against the renamed/collapsed
  tree, Testcontainers ITs running for real (OrbStack socket exported, not self-skipped) —
  detection 109 (4 = env-gated `SaslAclSmokeTest` skips), relay 5, alert 12, triage 40;
  0 failures, 0 errors.
- **Per-service `.dockerignore` ×4** (audit finding): compose builds use each service dir as
  context (`build: ../rpe-<svc>`), where the root `.dockerignore` never applied — `target/`
  (full build output) uploaded with every image build context. Image contents were never
  affected (Dockerfiles COPY pom.xml/src selectively); this trims context transfer.
- **Root `.dockerignore` patterns made recursive** (`**/target/`, `**/build/`, `**/out/`): its
  own COPY-regression guard only matched root-level dirs, not the four service `target/` trees.
- **kafka-consumer.md §Relay Trigger de-drifted** to the v1.15 collapse (interface snippet →
  concrete `PostgresNotifyRelayTrigger.awaitSignal` contract). `check-doc-drift.sh` green.

## v1.15 — 2026-07-25

Three-repo over-engineering audit closure (build hygiene + rename; no behavior change intended):

- **Package/coordinates rename `com.example.rpe` → `io.rpe`** (groupId `com.example` → `io.rpe`, all
  four services). A coordinated cut: package trees, imports, ArchUnit package strings, Kafka
  `TRUSTED_PACKAGES`, and the detection CB `ignoreExceptions` FQN in `application.yml` — local and
  AWS change together, so no parity fork; LOCAL-PARITY's "the AWS work changed zero yml" claim is
  about the AWS work and stands. ADRs untouched (historical record of the `com.example` era).
  Verified: `mvn test-compile` ×4 green; all four `*ArchTest` suites green (7/4/5/4 = 20 tests).
- **Detection boot jar no longer ships dev diagnostics:** BlockHound + reactor-tools excluded at
  repackage (compile scope retained — `DevConfig` imports them; dev `spring-boot:run` runs off the
  classpath and is unaffected). Enabling `rpe.dev.*` against a repackaged jar now fails fast.
- **Triage Resilience4j pins** moved under a `resilience4j-bom` 2.4.0 import (six per-module
  `<version>` tags dropped).
- **`RelayTrigger` collapsed into `PostgresNotifyRelayTrigger`:** single impl, and `signal()` had
  zero callers — its own javadoc called it dead since the ADR-17 §7 Stage 1 extraction. The ADR-05
  signal-is-an-optimisation invariant now lives on `awaitSignal`'s javadoc.
- **k8s manifests gain CPU/memory `requests`** (memory request = existing limit, so scheduling
  semantics are unchanged; CPU uncapped to avoid throttling); `deploy/README.md` de-staled
  (Stages 1–5 complete, this compose canonical, Stage 6 authored/e2e-pending).
- Repo newly git-tracked (baseline + per-refactor commits; oauth `keys/` verified ignored).
- **Pin-drift gate + Renovate** (the audit's duplicate-pins finding, closed without a shared BOM):
  `scripts/check-pin-drift.py` (ci.yml `pin-drift` job) fails the build when a coordinate declared
  in ≥2 POMs — or any `io.github.resilience4j` artifact repo-wide (the POMs pin disjoint r4j
  artifactIds, invisible per-coordinate) — resolves to more than one version; `renovate.json`
  keeps pins aligned on upgrade (one Renovate PR updates every declaring POM). A shared BOM was
  rejected deliberately: a local one breaks the `mvn -f` matrix and per-service Docker contexts
  (ADR-17 §3.5 module-deletion=rollback), and the only $0 registry (GitHub Packages) requires PAT
  auth even for public consumption — polluting the credential-free CI/Docker posture. Verified:
  positive pass on the real POMs (3 shared coordinates incl. the Boot parent), negative run
  catches seeded coordinate and group drift.
- **Honest caveat:** full `mvn verify` (testcontainers suites) has NOT been run against these
  changes; compile + ArchUnit is the verification level recorded here.

## v1.14 — 2026-07-06

ADR-26 (detection ordering-and-recovery correctness; EADIE audit; ACCEPTED):

- `asyncAcks=true` on the payment container — lane VTs ack in completion order, not offset order;
  without it the committed watermark can pass an in-flight event (silent permanent loss on crash).
  Pinned by `KafkaConfigAckPropertiesTest`.
- `automaticTransitionFromOpenToHalfOpenEnabled: true` on the `redis` CB — the ADR-02 fallback stops
  all traffic to an open breaker, so R4j's lazy OPEN→HALF_OPEN never fires without it (permanent
  wedge). Pinned by `RedisCircuitBreakerAutoTransitionTest`.
- `CbFallbackHandler` rewritten around a single lock-owned `dispatch(accountId, task)` routing API,
  closing two transition races (stranded offer, mid-drain ordering inversion).
- Post-gate DLT records now carry `x-rpe-outcome`/`-rule`/`-reason` headers so a dedup-blocked
  re-driven `ALERT_UNDURABLE` alert can be reconstructed deterministically (same UUIDv5) instead of
  lost silently; `dlt-redrive.sh` now re-emits original headers on re-produce.
- Known Limitations: the SIGKILL safety-net and CB-buffer-loss-≠-data-loss claims are now genuinely
  true (were aspirational before ADR-26).

Code unchanged by this doc pass — all four services green (detection 91, relay 5, alert 10, triage 31).

## v1.13 — 2026-07-02

Arch-audit fixes (all four full-verify green: detection 82, relay 5, alert 10, triage 31):

- ADR-24 R2 resolved — `rate_limit.lua` clock moved to `redis.call('TIME')` (one server clock for all
  replicas), removing inter-replica skew from the shared token bucket.
- Detectors pinned `@Order` (velocity 10 → zscore 20 → geo 30) + `DetectorPrecedenceTest` — multi-fire
  precedence is part of the `alert_id` contract.
- All three DLT recoverers use a `byte[]`→`ByteArraySerializer` type-map so poison records stay
  byte-faithful.
- Relay `AlertPublisher` javadoc corrected — `payment.alerts` is at-least-once-on-topic + mandatory
  consumer dedup, not exactly-once.
- +3 Known Limitations (alert-emission vs state-mutation ordering, at-least-once topic, unbounded
  PENDING outbox on relay outage).

## v1.12 — 2026-06-30

ADR-25 (distributed tracing; ACCEPTED): app-level Micrometer Observation → OTel → OTLP supersedes the
k8s OTel Java agent (only in-code instrumentation stitches the outbox hop); W3C
`traceparent`/`tracestate` persisted as `outbox` columns (Flyway V5) + restored by the relay; Kafka
`setObservationEnabled` on all containers/templates; fail-open. `inject-java` annotations removed from
all four Deployments.

## v1.11 — 2026-06-27

ADR-17 §7 Stage 6 — CNPG HA cluster; per-service schemas + login roles + least-privilege GRANTs;
Flyway scoped to owning schema; per-service DB credential Secrets; OrbStack k8s canonical topology
(`deploy/k8s/`: CNPG + Redpanda + Redis, four service Deployments, default-deny NetworkPolicies, OTel
Operator + Collector + Tempo + Grafana + Prometheus). Full pipeline e2e verified across schema
boundaries.

## v1.10 — 2026-06-18

ADR-17 §7 Stage 5 — `deploy/docker-compose.services.yml` flipped to canonical (+
`monitoring/prometheus.services.yml` scraping all four services); root `docker-compose.yml` re-labeled
the all-in-one dev convenience.

## v1.9 — 2026-06-18

ADR-17 §7 Stage 4 — per-consumer additive-evolution contract tests + ArchUnit cross-service-import
guards on all four services + detection-core confinement guard; triage gained its first ArchTest.
Test counts: detection 55, alert 9, relay 4, triage 27.

## v1.8 — 2026-06-18

ADR-17 §7 Stage 3 — the root module's `src/` + `pom.xml` moved into the `rpe-detection-service/`
sibling (zero source edits); root carries no pom; all four services are symmetric
`mvn -f <svc>/pom.xml verify` siblings. detection 50/50, relay 3/3.

## v1.7 — 2026-06-18

ADR-18 (one DLT per consumer group on `payment.alerts`; `payment.alerts.triage.DLT` sole-writer
`rpe-triage-agent`; custom `DeadLetterPublishingRecoverer` required; ACCEPTED).

## v1.6 — 2026-06-18

ADR-17 §7 Stage 2 — alert-actioning extracted to `rpe-alert-service` (owns `processed_alerts`; core
50/50, alert 5/5).

## v1.5 — 2026-06-17

ADR-17 §7 Stage 1 — relay extracted to `rpe-relay-service` (core 52/52, relay 3/3).

## v1.4 — 2026-06-17

ADR-17 (decompose into 4 independently deployable services; ACCEPTED); + `microservices.md` rules
file; + Service Topology section; + `deploy/` target topology.

## v1.3 — 2026-06-14

ADR-16 implemented — core at Boot 4.1.0 + Spring Kafka 4.0, triage at Boot 4.1.0 + Spring AI 2.0.0.

## v1.2 — 2026-06-13

ADR-16 (Spring Boot 4.1.x migration; ACCEPTED); + `spring-boot-4.md` rules file.

## v1.1 — 2026-06-12

+ `rpe-triage-agent` module (Spring AI, ADR-15): stack entry, runtime rows, constraints, limitations,
commands.

## v1.0

Trimmed core (~135 lines, hard constraints only).
