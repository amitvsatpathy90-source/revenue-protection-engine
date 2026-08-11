# ADR Index

Single source of truth for RPE's Architectural Decision Records. Both the root
[`README.md`](../../README.md) and `Architecture Spec` link here instead of duplicating this table — see
the changelog at the bottom for why.

| ID | Decision | File |
|---|---|---|
| ADR-01 | HTTP = WebFlux/Netty; pipeline = MVC + VTs; Redis = reactive Lettuce; JDBC = isolated platform pool | `ADR-01.md` |
| ADR-02 | C+D hybrid CB fallback; DLT rejected for transient failures | `ADR-02.md` |
| ADR-03 | Rate limiter inside lane, `tryAcquire(5s)`; DLT only on sustained breach | `ADR-03.md` |
| ADR-04 | Corporate accounts via Redis SET config; geo-exempt explicit opt-in only | `ADR-04.md` |
| ADR-05 | LISTEN/NOTIFY + adaptive polling behind `RelayTrigger` interface | `ADR-05.md` |
| ADR-06 | `transactional.id` injected from env (`RELAY_INSTANCE_ID`); never shared | `ADR-06.md` |
| ADR-07 | Caffeine lane map + `ArrayBlockingQueue(200)` + bounded-blocking submit (parks submitter; `CallerRunsPolicy` superseded — inline run broke single-writer) | `ADR-07.md` |
| ADR-08 | Welford effective sample cap 10,000; actual count stored separately | `ADR-08.md` |
| ADR-09 | Partition-scoped lane drain on rebalance; 5s bound; overlap accepted | `ADR-09.md` |
| ADR-10 | `volatile-lru` eviction; Welford stats keys (no TTL) protected | `ADR-10.md` |
| ADR-11 | `@JsonIgnoreProperties` + opaque relay + `schema_version` field | `ADR-11.md` |
| ADR-12 | `synchronous_commit=off` over UNLOGGED; 200ms loss window; single-line prod fix | `ADR-12.md` |
| ADR-13 | Non-durable Redis dedup accepted; exactness at alert boundary via UUIDv5 + `processed_alerts` | `ADR-13.md` |
| ADR-14 | Lua gate returns raw state array; Java `Detector` implementations apply thresholds | `ADR-14.md` |
| ADR-15 | AI triage agent: Spring AI, advisory-only, downstream of `payment.alerts`; full R4j boundary; LangChain4j rejected | `ADR-15.md` |
| ADR-16 | Spring Boot 4.1.x platform migration (EOL driver); Jackson 3 catch-block audit; JSpecify; Spring Kafka 4.0 | `ADR-16.md` |
| ADR-17 | Decompose into 4 independently deployable services along correctness-safe seams; Kafka-only async; schema-only contracts; never split the detection core | `ADR-17.md` |
| ADR-18 | One DLT per consumer group on `payment.alerts`; `payment.alerts.DLT` → sole writer `rpe-alert-service`; `payment.alerts.triage.DLT` → sole writer `rpe-triage-agent`; custom `DeadLetterPublishingRecoverer` required in triage agent | `ADR-18.md` |
| ADR-19 | Zero-trust: Kafka/network = trust boundary; Actuator surface authenticated via OAuth2 Resource Server (JWT, audience+scope); liveness/readiness public+detail-free; fail-closed, no enable flag | `ADR-19.md` |
| ADR-20 | Kafka transport auth: SASL_SSL/SCRAM per-service principal (mTLS alternative); least-privilege ACL matrix = one-writer-per-topic broker-enforced; JAAS-in-code; fail-closed; PLAINTEXT lab-only | `ADR-20.md` |
| ADR-21 | Exception-boundary discipline: narrow by default (specific → `RuntimeException` → `Exception`); broad `catch (Exception)` only in `@BoundaryHandler`; per-service ArchUnit-enforced | `ADR-21.md` |
| ADR-22 | Coordinated graceful shutdown: `SmartLifecycle` phase ladder anchored to Kafka `DEFAULT_PHASE` (stop ingest → drain → flush → close pools); bounded per-service drains; readiness-flip + k8s `preStop`; `rpe.shutdown.*` observability | `ADR-22.md` |
| ADR-23 | DLT operational strategy: operator-gated loop-safe re-drive (`dlt-redrive.sh`, attempt-cap→`*.parked`, dry-run default, fail-closed allowlist); per-writer `rpe.dlt.depth` ownership; depth-SLO alert rules; re-process-vs-discard decision tree. Fixes triage DLT routing bug | `ADR-23.md` |
| ADR-24 | Distributed rate limiting: global atomic Lua **token bucket** (`rate_limit.lua`, not fixed-window INCR), shared across replicas ⇒ global = `rate/s` not `N × per_instance`; throttle-not-drop preserved (VT parks on script `wait_ms`); own `ratelimit` R4j CB+Bulkhead; **degrade to per-instance Guava** on Redis trouble (never fail closed); separate from `gate.lua`, runs before it | `ADR-24.md` |
| ADR-25 | Distributed tracing: **app-level Micrometer Observation** → OTel → OTLP (supersedes the k8s OTel Java agent — only in-code instrumentation stitches the outbox hop); W3C `traceparent`/`tracestate` **persisted as `outbox` columns** (Flyway V5) so the relay continues the trace across the durable-queue gap; Kafka `setObservationEnabled` auto-propagates headers; triage LLM span free via Spring AI; **fail-open** (collector down ⇒ drop spans, never block detection); `inject-java` annotations removed, collector/Tempo retained | `ADR-25.md` |
| ADR-26 | Detection ordering-and-recovery correctness (EADIE audit): `asyncAcks=true` on the payment container — lane VTs ack in **completion order**, not offset order, so without it the committed watermark can pass an in-flight event (silent permanent loss on crash); this is what makes "uncommitted offset ⇒ redelivery" actually true. `automaticTransitionFromOpenToHalfOpenEnabled: true` on the `redis` CB — the ADR-02 fallback stops all traffic to an open breaker, so R4j's lazy OPEN→HALF_OPEN never fires without it (permanent wedge). `CbFallbackHandler` rewritten around a single lock-owned `dispatch` API, closing two transition races. Post-gate DLT records carry `x-rpe-outcome/-rule/-reason` headers so a dedup-blocked re-drive can reconstruct a fired-but-undurable alert deterministically instead of losing it silently | `ADR-26.md` |
| ADR-27 | Stats-key lifecycle (arch-audit R5): **sliding idle TTL on `stats:{account}`** (`rpe.detection.stats.ttl-ms`, default 30d; `PEXPIRE` in gate step 2, additive `ARGV[11]`) — supersedes ADR-10's no-TTL sub-decision. No-TTL keys are invisible to `volatile-lru`, so under account churn they squeeze the dedup/vel/geo working set (silent detection decay) and terminally wedge Redis in noeviction write errors. Loss on expiry = re-enter the <30-sample z-score gate (ADR-09 failure class). Plus memory watermark: `RedisMemoryMetrics` gauges + `RpeRedisMemoryHigh` (80%) + poll-failing meta-alert | `ADR-27.md` |
| ADR-28 | Dedup-guard horizon (arch-audit R4, backfill): `processed_alerts` 30d purge (`ProcessedAlertsPurge`) MUST exceed every window that can re-present an old `alert_id` — DLT/parked retention (14d) + operator re-drive latency + offsets-expiry replay leg (7d topic retention). Contract, not a tunable default; sibling backfill to ADR-27 from the same 2026-07-12 audit cycle | `ADR-28.md` |
| ADR-29 | No global Resilience4j defaults (backfill): every CB/bulkhead/rate-limiter/retry/timelimiter instance across all 4 services configured independently — zero `configs`/`base-config` blocks anywhere, triage's Java config uses `.custom()` never `.ofDefaults()`. Formalizes a convention enforced since ADR-01 | `ADR-29.md` |

---

## Changelog

| Date | Change |
|---|---|
| 2026-07-15 | Created as the single source of truth for the ADR index — previously duplicated verbatim in both `README.md` and `Architecture Spec`, which drifted (`README.md` was missing `ADR-27` until caught during a coverage audit the same day). `Architecture Spec` was ruled out as the canonical location despite being the more natural "AI context" home: it's gitignored in this repo (private, never published — see `.gitignore`), so a link from the public `README.md` into it would 404 for anyone actually viewing the repo on GitHub. Both `README.md` and `Architecture Spec` now link here instead of embedding the table. |
