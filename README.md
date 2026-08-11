# revenue-protection-engine (RPE)

Synthetic payment-event stream → rule-based anomaly detection → Kafka alerts, with observability.

**Portfolio / proof-of-capability artifact. Lab benchmarks only — never production claims.**

---

## Architecture

Four independently deployable services (ADR-17). The diagram below shows the hot path and
the advisory triage path. Service boundaries are marked `── [SERVICE BOUNDARY] ──`.

```
payment.events (Kafka topic, 12 partitions)
         │
         ▼
┌─────────────────────────── rpe-detection-service ────────────────────────────┐
│  @KafkaListener (Virtual Thread)                                             │
│     │  ErrorHandlingDeserializer → payment.events.DLT (poison messages)      │
│     ▼                                                                        │
│  Per-account Lane Executor (Single-threaded VT lane per accountId)           │
│     │  Caffeine cache, maxSize=50,000, evictAfterAccess=10min                │
│     │  ArrayBlockingQueue(200) + BoundedBlockingSubmitPolicy (back-pressure) │
│     ├─ Rate limiter check (global Redis token bucket; degrades to            │
│     │  per-instance Guava RateLimiter on Redis trouble — ADR-24)             │
│     ▼                                                                        │
│  ReactiveRedisTemplate.execute(Lua gate)   ← non-blocking Lettuce            │
│     │  ONE atomic script: velocity → z-score → geo → dedup (invariant)       │
│     │  .publishOn(laneScheduler)           ← MANDATORY: hop off I/O thread   │
│     ├─ dedupBlocked=true → ack, done                                         │
│     ├─ VelocityDetector  (sliding window count >= threshold)                 │
│     ├─ ZScoreDetector    (Welford online stats, z > 3σ, min 30 samples)      │
│     └─ GeoDetector       (haversine speed > threshold, broker timestamps)    │
│              │ (on ALERT)                                                    │
│              ▼                                                               │
│  Mono.fromCallable(outboxWriter.submit(intent))                              │
│      .subscribeOn(jdbcScheduler)  ← platform-thread pool                     │
│      .block()                     ← ack gated on O(1) enqueue (ADR-12/26);   │
│                                      saturated → payment.events.DLT.         │
│              ▼                                                               │
│  OutboxBatchWriter → INSERT INTO outbox (batched, ON CONFLICT DO NOTHING)    │
│  [HTTP/actuator surface: WebFlux/Netty on port 8080]                         │
└──────────────────────────────────────────────────────────────────────────────┘
         │ Postgres outbox table (sole writer: detection)
         ▼
┌──────────────────────── rpe-relay-service ───────────────────────────────────┐
│  PostgresNotifyRelayTrigger (dedicated platform thread, LISTEN outbox_ready) │
│  OutboxRelay (Virtual Thread)                                                │
│     SELECT ... FOR UPDATE SKIP LOCKED LIMIT 50                               │
│     kafkaTemplate.executeInTransaction(send)  ← Kafka tx commits first       │
│     UPDATE outbox SET status='PUBLISHED'      ← then mark published          │
└──────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
payment.alerts (Kafka topic, 3 partitions)  ← at-least-once; consumers dedupe on alert_id
         │
         ├────────────────────────────────────────────────────────────────────┐
         ▼                                                                    │
┌──────────────── rpe-alert-service ─────────────────────────────┐            │
│  AlertConsumer (Virtual Thread, isolation.level=read_committed)│            │
│     INSERT INTO processed_alerts ON CONFLICT DO NOTHING        │            │
│     alert_id = UUIDv5 → same event+rule = same id always       │            │
│     Poison alerts → payment.alerts.DLT (sole writer)           │            │
└────────────────────────────────────────────────────────────────┘            │
                                                                              ▼
                                                              ┌─── rpe-triage-agent ────────────────────┐
                                                              │  Triage consumer (VT, own group)        │
                                                              │  inbox INSERT triaged_alerts ON CONFLICT│
                                                              │  ChatClient agent loop (≤3 tool rounds) │
                                                              │  R4j: TimeLimiter+CB+Bulkhead+RL+Retry  │
                                                              │  Fallback: DEGRADED_RULE_BASED verdict  │
                                                              │  → payment.alerts.triaged               │
                                                              │  Poison → payment.alerts.triage.DLT     │
                                                              │    (custom DeadLetterPublishingRecoverer│
                                                              │     required — ADR-18)                  │
                                                              └─────────────────────────────────────────┘
```

### Service Topology (ADR-17)

Four independently deployable services. Each builds via `mvn -f <svc>/pom.xml verify`. No root pom aggregates them.

| Service | Bounded context | Sole writer of | Kafka |
|---|---|---|---|
| `rpe-detection-service` | Ingest + atomic detect + persist alert-intent (+ HTTP/actuator) | all Redis keys; `outbox` rows | consumes `payment.events` → `payment.events.DLT` |
| `rpe-relay-service` | At-least-once outbox→topic delivery (exactly-once realized at the consumer via `alert_id` dedup) | `outbox` status transitions only | produces `payment.alerts` |
| `rpe-alert-service` | Idempotent alert actioning | `processed_alerts` | consumes `payment.alerts` → `payment.alerts.DLT` |
| `rpe-triage-agent` | Advisory LLM enrichment (Spring AI, ADR-15) | `triaged_alerts` | consumes `payment.alerts` → `payment.alerts.triaged` → `payment.alerts.triage.DLT` (ADR-18) |

The detection core (`rpe-detection-service`) is **never split**: the atomic Lua gate, per-account lane, and all `Detector`s are one indivisible service (ADR-17 non-goal #1). New detection rules are new `Detector`s inside that service, never new services.

Canonical container topology: `docker compose --env-file .env -f deploy/docker-compose.services.yml up -d`

### Runtime Surface Split

| Component | Service | Runtime Model | Reason |
|---|---|---|---|
| HTTP / actuator | detection | WebFlux (Netty) | Zero threads parked on HTTP I/O |
| Kafka consumer (`@KafkaListener`) | detection | Virtual threads | Blocking poll loop |
| Per-account lane executor | detection | Virtual threads | Single-threaded per account; ordering + atomicity |
| Redis Lua gate | detection | Reactive Lettuce | Truly non-blocking; no thread parks |
| Outbox batch writer | detection | Dedicated platform-thread pool | PgJDBC `synchronized` pins VTs |
| Relay loop | relay | Virtual thread | Background, not HTTP-bound |
| Relay Kafka producer | relay | Virtual thread | Serialised per instance |
| Alert consumer | alert-service | Virtual threads | Blocking consumer model |
| Triage consumer + LLM call | triage-agent | Virtual threads; R4j-wrapped | Blocking HTTP; bounded by TimeLimiter + Bulkhead |

### Data Durability Layers

| Layer | Guarantee | Loss Window |
|---|---|---|
| Event dedup | Best-effort | 1s AOF window on hard crash; replay safe via deterministic alert_id |
| Alert intent (outbox) | Best-effort | Up to 50ms batch flush window on app crash |
| Alert delivery | At-least-once on topic | Kafka transactional producer; a relay crash between tx-commit and status UPDATE re-sends — every consumer dedupes on `alert_id` |
| Alert actioning | Exactly-once | UUIDv5 alert_id + processed_alerts ON CONFLICT DO NOTHING |

---

## Stack

- **Java 21** — virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`)
- **Spring Boot 4.1.x** — WebFlux (Netty) for HTTP; Spring MVC + VT for pipeline (ADR-16)
- **Redpanda** (local Kafka API) — `payment.events`, `payment.alerts`, `payment.alerts.triaged`; DLTs: `payment.events.DLT`, `payment.alerts.DLT`, `payment.alerts.triage.DLT` (ADR-18)
- **Redis 7.2.5** — ALL hot-path state (velocity, z-score, geo, dedup) via atomic Lua gate
  - AOF: `appendonly yes`, `appendfsync everysec`, `maxmemory 96mb`, `volatile-lru`
- **PostgreSQL 16.3** — outbox + processed_alerts (alert boundary only, ~1% of events)
- **Resilience4j** — CircuitBreaker + Bulkhead per outbound boundary
- **Micrometer → Prometheus → Grafana** — observability

---

## Quick Start

```bash
# 1. Copy env template and FILL IN values — secrets have no checked-in defaults.
#    docker compose and the app both fail fast if POSTGRES_PASSWORD / DB_PASSWORD /
#    GRAFANA_PASSWORD are unset (security rule: secrets via environment only).
cp .env.example .env

# 2. Export .env into the current shell ONCE
export $(grep -v '^#' .env | xargs)

# 3a. All-in-one DEV convenience (infra + detection-on-host via mvn + triage profile):
docker compose up -d

# 3b. CANONICAL four-service topology (ADR-17 Stage 5 — all services as containers):
docker compose -f deploy/docker-compose.services.yml up -d

# 4. Run services individually. Each service is its own module (ADR-17); the
#    repo root has no pom, so target a service pom with -f. Spring does not
#    read .env natively — this relies on the export from step 2 above.
mvn -f rpe-detection-service/pom.xml spring-boot:run   # port 8080
mvn -f rpe-relay-service/pom.xml spring-boot:run        # port 8082
mvn -f rpe-alert-service/pom.xml spring-boot:run        # port 8083
mvn -f rpe-triage-agent/pom.xml spring-boot:run         # port 8081

# 5. (dev mode — BlockHound + VT pinning trace, detection service)
mvn -f rpe-detection-service/pom.xml spring-boot:run \
  -Dspring-boot.run.profiles=dev \
  -Dspring-boot.run.arguments="--rpe.dev.blockhound.enabled=true --rpe.dev.reactor-debug-agent=true" \
  -Dspring-boot.run.jvmArguments="-XX:+AllowRedefinitionToAddDeleteMethods -Djdk.tracePinnedThreads=full"

# Inspect alerts on payment.alerts topic
kafka-console-consumer --bootstrap-server localhost:19092 \
  --topic payment.alerts --from-beginning

# Relay health: pending count + oldest row age
psql $DB_URL -c \
  "SELECT count(*), max(now()-created_at) AS oldest FROM outbox WHERE status='PENDING';"

# Lettuce pool metrics
curl -s -H "Authorization: Bearer $RPE_SCRAPE_JWT" \
  http://localhost:8080/actuator/prometheus | grep lettuce.pool

# LISTEN/NOTIFY health
curl -s -H "Authorization: Bearer $RPE_SCRAPE_JWT" \
  http://localhost:8080/actuator/health | python3 -m json.tool | grep -A3 relayListener

# The only endpoint reachable without a token (k8s probe):
curl -s http://localhost:8080/actuator/health/readiness

# Prometheus
open http://localhost:9090

# Grafana (admin / value from .env GRAFANA_PASSWORD)
open http://localhost:3000
```

---

## Key Metrics

| Metric | Description | Alert Threshold |
|---|---|---|
| `rpe.detection.timer{rule_type,outcome}` (p99) | Per-rule detector evaluation latency | — |
| `kafka.consumer.fetch.manager.records.lag` | Per-partition consumer lag (Micrometer Kafka binding) | SLO-dependent |
| `rpe.outbox.pending.count` | PENDING rows in outbox | > 100 (WARN: relay stuck) |
| `rpe.outbox.pending.age_seconds` | Age of oldest PENDING row | > 30s (WARN: relay stuck) |
| `rpe.outbox.failed.count` | Rows that exhausted relay attempts (dead-lettered, never purged) | > 0 |
| `rpe.outbox.queue.size` | Alert intents awaiting batch flush | sustained growth |
| `rpe.outbox.dropped{reason}` | Intents dropped on bounded-queue overflow (Postgres outage) | > 0 |
| `rpe.dlt.depth{topic}` | Dead-letter topic depth | > 0 |
| `rpe.gate.invalid_state{reason}` | Lua gate contract violations (ADR-14) | > 0 |
| `rpe.cb.buffer.size` | Events in CB fallback buffer (uncommitted offsets) | near buffer-size |
| `rpe.geo.exempt.skip` | Geo rule bypasses for exempt accounts (ADR-04) | informational |
| `rpe.alerts.actioned{rule_type,outcome}` | processed_alerts inserts (actioned vs duplicate) | — |
| `lettuce.pool.pending` | Lettuce pool saturation | > 0 sustained |
| `rpe.relay.listener.healthy` | LISTEN/NOTIFY connection (0/1) | = 0 |
| `rpe.redrive.reconstructed{outcome}` | Re-driven post-gate DLT records reconstructed straight to the outbox (ADR-26) | informational |
| `rpe.redrive.unrecoverable` | Re-driven records with no reconstructable outcome — parked for an operator (ADR-26) | > 0 |

---

## Detection Rules

### Velocity (sliding window)
`ZCOUNT vel:{accountId} (now-windowMs) +inf` before adding this event.
Alert when prior count ≥ `rpe.detection.velocity.max-events`.
Default: 100 events / 60-second window.

### Amount Z-Score (Welford online stats)
Welford mean + M2 maintained in Redis hash. Updated atomically in the Lua gate.
`effective_count = min(count, 10000)` — numerical stability cap (ADR-08).
Gate: count ≥ 30 (post-update). Never divide by stddev=0.
Alert when `|amount - mean| / stddev > threshold`. Default threshold: 3.0.

### Geo Velocity
`haversine(prevLat, prevLon, curLat, curLon) / (brokerIngestDelta)`.
Uses **broker ingest timestamps only** — payload clocks drift.
Alert when computed speed > `rpe.detection.geo.max-speed-kmh`. Default: 900 km/h.

### Lua Gate Execution Order (immutable)
```
1. Velocity  (ZADD + prune + PEXPIRE)
2. Z-score   (Welford HSET — no TTL; volatile-lru protects stats keys)
3. Geo       (read prev HMGET → write new HSET + PEXPIRE)
4. Dedup     (SET dedup:{eventId} NX EX 300)  ← MUST BE LAST
```
Dedup-last is a correctness invariant: if any step before 4 errors, the event is not
marked seen and Kafka redelivers safely. Dedup-first = silent permanent event loss.

---

## Circuit Breaker Fallback (Redis unavailability)

**C+D hybrid — DLT is never used for Redis unavailability (ADR-02).**

Payment events are unique and unrepeatable. Routing to DLT on a Redis outage permanently
removes events from the detection pipeline.

```
NORMAL    → [CB opens]      → BUFFERING  (in-memory ArrayBlockingQueue)
BUFFERING → [buffer full]   → PAUSED     (Kafka consumer container paused)
PAUSED    → [CB half-open]  → DRAINING   (buffer drained to lane executors)
DRAINING  → [buffer empty]  → NORMAL     (consumer resumed)
```

Buffer sizing: `event_rate_per_sec × cb_wait_duration_sec × 1.5`.
Default: `rpe.resilience.redis.buffer-size=500`.

Buffer is heap-resident; not persisted across restarts. Buffer loss on crash =
uncommitted offsets redelivered by Kafka on restart. Not data loss — but only because
`asyncAcks=true` holds the committed watermark at the un-acked gap (ADR-26); without it, an
out-of-order lane ack could advance the watermark past a buffered event and lose it silently.
The `redis` breaker also runs with `automaticTransitionFromOpenToHalfOpenEnabled: true` — without
it, the traffic-stopping fallback and R4j's call-triggered OPEN→HALF_OPEN transition wait on each
other forever, wedging detection permanently even after Redis recovers (ADR-26).

---

## Lab topology & disclosure

> **Lab artifact. No production claims — and no reproducible throughput figures quoted.** The value
> here is the design and its correctness proofs, not a benchmark number.

- **Single broker, RF=1.** `acks=all` confirms only the sole leader — not a durability guarantee.
  Production minimum: RF≥3, `min.insync.replicas=2`.
- **Single-instance Redis.** Rate limiting is globally consistent via the Lua token bucket, but a
  Redis failure degrades it to a per-instance limiter (never fail-closed — ADR-24).
- **Synthetic event stream.** No labeled data, so there is deliberately no precision/recall or
  severity-calibration claim on the detectors or the triage verdicts (ADR-08 / ADR-15).

---

## Redis Memory Sizing

96mb supports approximately 50,000 concurrently active accounts.

Per active account:
- `vel:{account}` ZSET: ~500b
- `stats:{account}` hash: ~200b (Welford; no TTL → protected from eviction)
- `geo:{account}` hash: ~100b
- `dedup:{eventId}` keys: ~50b × events-in-5min-window

`maxmemory-policy volatile-lru`: stats keys (no TTL) are protected from eviction. Under
extreme memory pressure, dedup/velocity/geo keys (TTL-bearing) may be evicted — bounded
missed detections; stats accuracy preserved.

---

## Known Limitations

| Limitation | Impact | Compensating control |
|---|---|---|
| Redis best-effort event dedup (1s AOF crash window) | Replay within window may re-process | Deterministic UUIDv5 alert_id + processed_alerts ON CONFLICT (ADR-13) |
| 50ms alert-intent loss window on app crash | Alert intents in last batch not flushed | Table survives intact; relay re-picks on restart (ADR-12) |
| Welford/geo bounded replay drift | Non-idempotent on replay past 300s TTL | Bounded, self-healing on subsequent events (ADR-09) |
| Welford M2 precision cap at 10,000 effective samples | History older than ~10k events phased out | Desirable for fraud detection accuracy (ADR-08) |
| Hot-account partition saturation | Single high-frequency account owns one partition/lane | Rate limiting caps throughput; composite key + geo-exempt is the production scale-out path (ADR-04) |
| Rate limit degrades to per-instance under Redis trouble | Global Redis token bucket (ADR-24) is primary; while its circuit breaker is open, falls back to per-instance Guava (`N × rate/s`) until Redis recovers | Bounded, self-healing; `rpe.ratelimit.degraded` signals the fallback window (ADR-24) |
| Lab topology: single broker, RF=1 | No replication durability | Production: RF≥3, min.insync.replicas=2 |
| 200ms synchronous_commit=off WAL window | Hard crash may lose last 200ms of outbox writes | Remove `SET LOCAL synchronous_commit=off` for full durability (ADR-12) |
| Rebalance overlap after 5s drain timeout | Brief dual-processing on partition revoke | Dedup + deterministic IDs handle concurrency (ADR-09) |
| Triage non-determinism | Same alert can yield a different narrative/severity on reprocess | Acceptable — advisory only; inbox dedup prevents reprocess in the normal path (ADR-15) |
| Triage threshold/severity calibration unvalidated | Synthetic data; no precision/recall claim | Calibration requires labeled production data (ADR-15) |
| Degraded mode loses enrichment, not alerts | CB open ⇒ `DEGRADED_RULE_BASED` static severity; narrative quality drops | Delivery is never gated on triage (ADR-15) |
| LLM spend bounded per-instance | Effective global cap = N × per_instance (R4j RateLimiter) | Distributed counter is the upgrade path (ADR-15) |
| Shared Postgres, not DB-per-service | detection+relay share `outbox`; triage reads `processed_alerts` | Single-writer ownership + contract-grade access; production path: per-service schema + scoped GRANTs (ADR-17 §5.2) |
| Decomposition Stages 1–6 complete | Per-service Postgres schema + scoped GRANTs, k8s topology, and cross-service tracing are built (ADR-17 §7, ADR-25) | Production hardening (mTLS, HA IdP, RF≥3) remains the documented upgrade path |
| `payment.alerts.triage.DLT` requires custom `DeadLetterPublishingRecoverer` | Spring Kafka default suffix resolves to wrong owner topic; silence failure at runtime | Custom bean must be explicitly configured in `rpe-triage-agent`; integration test asserts zero records land on `payment.alerts.DLT` (ADR-18) |
| Actuator auth is fail-closed | Every service refuses to start without `RPE_OAUTH_ISSUER`/`RPE_OAUTH_JWKS_URI` wired to an IdP/JWKS | OAuth2 Resource Server (JWT, audience+`metrics:scrape` scope); liveness/readiness stay public + detail-free (ADR-19) |
| Kafka transport auth is opt-in by env | `RPE_KAFKA_SECURITY_PROTOCOL=PLAINTEXT` is the lab default; prod/k8s must set `SASL_SSL` + run `provision-acls.sh` | Per-service SCRAM principal + least-privilege ACLs; fail-closed once SASL_* is set (ADR-20) |
| Graceful shutdown narrows the loss window, doesn't replace durability | Coordinated drain only runs on an orderly SIGTERM; SIGKILL/OOM falls back to the pre-existing safety net | Uncommitted offsets + Redis dedup + deterministic alert_id + `processed_alerts ON CONFLICT` (ADR-22) |
| DLT re-drive is a manual operator loop, not self-healing | A DLT fills, alerts, and is drained by a human via `deploy/kafka/dlt-redrive.sh` after the decision tree | Loop-safe attempt-cap → `<dlt>.parked` quarantine; RPE never auto-replays (ADR-23) |
| Tracing is best-effort and lab-sampled | Fail-open — a down/absent collector drops spans, never blocks detection; sampling is `1.0` in lab only | Persisted `traceparent` self-corrects on NULL (fresh trace); production must tie sampling to volume (ADR-25) |
| `asyncAcks` widens duplicate delivery after a failure | Lane-completion-order acks mean a crash replays the whole un-acked offset gap, not just one event | Absorbed by the gate's dedup pre-check + deterministic `alert_id`; this is what makes "uncommitted offset ⇒ redelivery" structurally true rather than aspirational (ADR-26) |
| Re-driven `payment.events.DLT` records are dedup-blocked by construction | Gate dedup key (step 4) is already set before Java sees the result, so detectors never re-run on re-drive | `ALERT_UNDURABLE` outcomes reconstruct deterministically to the outbox; any other post-gate failure is fail-visible and parks for an operator, not silently dropped (ADR-26) |

---

## Architectural Decisions

We use Architectural Decision Records (ADRs) to track key technical and design decisions.
For the complete, single-source-of-truth list, see the [ADR Index](docs/adrs/README.md).
# CI bootstrap check
