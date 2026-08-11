<!-- edit-log (newest first): v1.0 | 2026-06-30 | Initial. ACCEPTED. -->

---
asset_id: adr-25-distributed-tracing
asset_path: docs/adrs/ADR-25.md
asset_type: adr
version: 1.0.0
created: 2026-06-30
last_updated: 2026-06-30
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [observability, tracing, opentelemetry, micrometer, outbox, kafka, w3c-traceparent, spring-boot-4]
---

# ADR-25 — Distributed tracing: app-level Micrometer Observation, trace-context through the outbox

## Status

`ACCEPTED`

Supersedes the **agent-only** tracing posture of ADR-17 Stage 6 (the k8s OTel Operator
`Instrumentation` CR + `instrumentation.opentelemetry.io/inject-java` annotations). The collector →
Tempo → Grafana pipeline from Stage 6 is **retained**; only the *instrumentation source* changes.
Related: ADR-01 (runtime-surface split — tracing is wired per surface), ADR-11/17 (the outbox is a
schema contract; the new columns are additive), ADR-13 (deterministic `alert_id` — the other
cross-service join key), ADR-15 (the triage LLM span), ADR-19 (Actuator is the only HTTP surface;
tracing adds no endpoint), security.md (PII rule — no `account_id`/payload in spans),
`ADR-25.md` (enforcement detail).

## Context

Stage 6 stood up the **collector + Tempo + Grafana** and an OTel Operator `Instrumentation` CR that
injects the **OTel Java agent** into each pod. But the application code carried **zero** tracing:
only `micrometer-registry-prometheus` (metrics) + `context-propagation` (MDC across reactive hops).

The agent-only approach has a fatal blind spot **for this architecture**. RPE's defining seam is the
**transactional outbox**: detection commits the Kafka offset and writes an `outbox` row, and a
*separately deployed* relay later reads that row and produces to `payment.alerts`. To the OTel Java
agent, detection's `INSERT INTO outbox` and the relay's `SELECT … FROM outbox` are two **unrelated
JDBC operations** — it has no notion that the row *continues* a trace. So the agent produces a trace
that **dies at the outbox** and an unrelated new trace from the relay onward — broken exactly at the
async exactly-once hop that distributed tracing exists to illuminate (how long an alert sat in the
outbox; which originating event produced which `payment.alerts` record; the full
event → detect → relay → action / triage → LLM path).

Two further problems with agent-only: it exists **only in k8s** (not compose, not `mvn verify`, not
on-host dev), so traces can't be asserted in CI; and bytecode-injection magic is the antithesis of
RPE's explicit-over-magic philosophy (per-boundary R4j, no global defaults, no feature flags).

## Decision

**App-level Micrometer Observation tracing is the single source of truth**, exported via OTLP to the
existing collector. The OTel Java agent is **removed** (annotations stripped from all four
Deployments). Each service adds `spring-boot-starter-opentelemetry` (Boot 4's modular split puts the
`Tracer` bridge + OTel SDK auto-config there — the `micrometer-tracing-bridge-otel` artifact alone
does **not** bring Boot's autoconfig, so there is no `Tracer` bean without the starter) +
`micrometer-tracing-bridge-otel` + `opentelemetry-exporter-otlp`; W3C propagation; sampling `1.0`
(lab); OTLP endpoint from env.

**1. Tracing is fail-open — the deliberate inverse of the security ADRs.** A missing/unreachable
collector drops spans on a background exporter thread; it **never** blocks the hot path. ADR-19/20
fail *closed* (a missing JWT issuer / Kafka cred aborts startup) because the alternative is a
silent-insecure surface. Tracing fails *open* because the alternative — letting an observability
outage stop fraud detection — is far worse than a missing span. No `tracing.enabled=false` kill
switch beyond the standard Boot property; degradation is automatic.

**2. Kafka boundaries auto-propagate.** `setObservationEnabled(true)` on every listener container
and `KafkaTemplate` makes Spring Kafka start/continue a span per record and inject/extract
`traceparent` on the message headers — so the trace rides the bus across detection → (relay) →
alert / triage with no manual header handling.

**3. The outbox stitch (the part the agent cannot do).** The durable-queue hop is bridged
explicitly:
- detection captures the **consume span's** W3C `traceparent`/`tracestate` **on the listener
  thread** (where that span is the active context) and threads them **explicitly** through to the
  `AlertIntent`. The per-account lane is a raw `ThreadPoolExecutor` (ADR-07), so the ThreadLocal
  trace context does **not** survive `lane.submit()` — explicit threading (like `brokerIngestMs` and
  MDC already are) is correct and robust to the reactive Redis hop, not ambient magic.
- the writer persists them as **two new nullable `outbox` columns** (`traceparent`, `tracestate`;
  Flyway `V5`, additive per ADR-11 / microservices.md §2).
- the relay **restores** the persisted context as a remote parent (`TraceContextReader.continueTrace`)
  and publishes within it; the observation-enabled relay template then emits the `payment.alerts`
  produce span as a **continuous child** and injects the header downstream. One trace, end to end,
  and the outbox latency is a visible gap **inside** that trace.

**4. The triage LLM span is free.** Spring AI 2.0's OpenAI model is observation-aware; with a
`Tracer` on the classpath it emits the `gen_ai` client span under the triage consume span — no change
to `TriageLlmClient` (its R4j decorator chain and `triage.llm.latency` metric are untouched).

**5. PII discipline (security.md, unchanged).** Span names are operation names (`rpe.outbox.relay`,
`payment.alerts send`); no `account_id`, no payload, no coordinates as span attributes. Correlation
rides the masked `event_id`/`account_id` in logs + the deterministic `alert_id` (ADR-13). The
collector's `attributes/pii-suppress` processor stays as belt-and-suspenders.

## Alternatives Considered

| Option | Decision | Reason |
|---|---|---|
| Keep agent-only (Stage 6 state) | Rejected | Trace dies at the outbox (INSERT and the relay's SELECT look unrelated); k8s-only, untestable in `mvn verify`. The gap this ADR exists to close. |
| Agent + app-level hybrid | Rejected | Double-instruments Kafka/JDBC; two context sources race. Pick one primary — app-level, because only it controls the outbox seam. |
| Put `traceparent` in the alert **payload** (not a column) | Rejected | The relay forwards `outbox.payload` byte-identically and never deserializes it (ADR-11); trace metadata is not part of the alert contract. A sidecar column the relay already reads is the right home. |
| Propagate via a context-wrapped lane executor (ambient) | Rejected | The lane is a per-account raw `ThreadPoolExecutor` created in a Caffeine cache; wrapping every one to carry ThreadLocals is more magic and more overhead than threading two strings explicitly. |
| Manually inject the stored header in the relay, no restored span | Rejected | The relay would not appear in the trace and the outbox latency would be invisible; restoring the parent span shows the hop as a real edge. |
| Fail-closed tracing (block on collector down) | Rejected | An observability outage must never stop fraud detection (mirrors ADR-03/24 degrade-never-drop). |

## Consequences

**Positive:** one continuous trace across the system's hardest seam — `payment.events`
consume → detect → **outbox (latency visible)** → relay produce → `payment.alerts` → alert actioning
*and* triage → LLM. Uniform across compose / `mvn verify` / dev / k8s; the stitch is **asserted in
CI** (detection: the alert row carries a well-formed W3C `traceparent`; relay: the produced
`payment.alerts` record carries a `traceparent` header in the **same** trace). No new failure mode
on the hot path (fail-open). Reuses the Stage-6 collector → Tempo → Grafana pipeline unchanged.
Incidentally fixed a latent bug: the relay's `RelayListenerHealthIndicator` lived in
`com.example.rpe.observability`, a sibling of the relay's default scan root, so it was never
registered — broadening the scan to `com.example.rpe` (matching detection) registered it.

**Negative:** two new (nullable) `outbox` columns and a small amount of explicit trace-threading on
the detection hot path. Sampling at `1.0` is lab-only (production must tie it to volume). The OTel
Java agent path is retired — anything it auto-instrumented that we do **not** manually span (e.g.
incidental third-party clients) is no longer traced; for RPE's surface (Kafka, JDBC, Redis, the LLM)
that surface is covered by Spring observation + the manual outbox span.

## Residual Risks (explicit)

- **R1 — Legacy / un-instrumented producers ⇒ NULL traceparent.** Rows written before V5, or any
  producer with tracing off, carry NULL; the relay starts a fresh trace for them (fail-open). No
  backfill. Bounded and self-correcting as new rows flow.
- **R2 — Caller-side capture, not broker-authoritative.** The persisted `traceparent` is the
  consume span the detection instance held; correct by construction (it *is* the originating trace).
  Sampling is parent-based, so the relay/alert/triage honour the original sampled flag — no
  re-sampling divergence.
- **R3 — Sampling at 1.0 is lab scope.** At production volume this is too much; tie the probability
  to traffic (and consider tail sampling in the collector). The persisted `traceparent` carries the
  sampled flag, so a not-sampled trace still stitches correctly; only export volume changes.
- **R4 — Span cardinality / PII.** Span attributes must never carry `account_id`/payload (security.md);
  enforced by review + the collector's `attributes/pii-suppress`. New manual spans must keep this bar
  (`ADR-25.md`).
- **R5 — Exporter back-pressure.** A wedged collector fills the batch span processor queue; OTel
  drops spans (logged), it does not block. Acceptable — observability degrades, the pipeline does not.

## Reversal Cost

`LOW` — drop the three tracing deps + the `management.tracing`/`management.otlp.tracing` config per
service; remove `setObservationEnabled(true)`, `TraceContextWriter`/`TraceContextReader`, and the
`AlertIntent`/outbox-column threading; the two `outbox` columns are nullable and can be left in place
or dropped. To return to agent-only, re-add the `inject-java` annotations (not recommended — the
outbox seam stays broken). No topic, payload, or alert-contract change.

## References

- `ADR-25.md` — enforcement detail (capture/restore, observation toggles, fail-open, PII)
- `rpe-detection-service` — `TraceContextWriter`, `AlertIntent` (+traceparent/tracestate),
  `OutboxBatchWriter` (column write), `KafkaConfig` (consumer observation), `V5__outbox_trace_context.sql`
- `rpe-relay-service` — `TraceContextReader`, `AlertPublisher` (restore + publish),
  `OutboxRelay` (column read), `RelayKafkaConfig` (template observation)
- `RpeIntegrationTest` scenario 9 (alert row carries a W3C traceparent) +
  `RelayServiceIntegrationTest` (produced record carries a same-trace traceparent header) — the CI stitch proofs
- `deploy/k8s/services/*.yaml` (inject-java removed + `OTEL_EXPORTER_OTLP_ENDPOINT`),
  `docs/adrs/superseded/52-instrumentation.yaml` (superseded CR — relocated out of the live manifest tree; it injected nothing once the annotations were removed)
- ADR-17 §7 (Stage 6 observability stack), ADR-11 (outbox contract, additive evolution), ADR-13
  (deterministic `alert_id`), ADR-15 (triage LLM), ADR-19 (Actuator-only HTTP surface)
