# ADR-01: Spring Boot MVC + WebFlux — Dual Runtime Surface

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** MEDIUM

---

## Context

RPE has two distinct runtime concerns: HTTP/actuator (I/O-bound, concurrent requests) and the processing pipeline (Kafka consumer, Redis hot path, JDBC outbox). A single MVC/VT stack leaves the most frequent operation — the Redis hot-path round-trip — unnecessarily thread-bound. Reactive Lettuce requires a non-blocking execution context to deliver its benefit; running it from a VT-backed MVC thread parks the VT on the round-trip wait, eliminating the non-blocking advantage.

## Decision

HTTP surface: WebFlux (Netty). Processing pipeline: MVC with virtual threads. Redis: reactive `ReactiveRedisTemplate` (Lettuce). JDBC: isolated dedicated bounded platform-thread pool. The two surfaces share no thread pools and no schedulers.

## Alternatives

**Single MVC + VTs everywhere:** VTs are cheap but they still park. The hot path fires once per event; parking a VT per Redis call at high throughput adds up. Reactive Lettuce on a VT-per-request model loses the pipeline benefit. Rejected.

**Full WebFlux everywhere:** Kafka consumer is inherently blocking (poll loop). Running a blocking consumer on WebFlux requires `subscribeOn(boundedElastic())`, which re-introduces thread pools with none of the ordering guarantees the VT lane model provides. Rejected.

## Consequences

- `publishOn(laneScheduler)` is mandatory after every reactive Redis op — result handler must not execute on the Lettuce I/O thread.
- `@KafkaListener` methods must return `void` — reactive return types are silently ignored by Spring Kafka.
- `@Transactional` is only safe on explicit platform-thread-pool paths (`jdbcScheduler`).
- Dev-only flags (BlockHound, pinning trace, Reactor debug agent) carry genuine production overhead — they are the cost of operating a hybrid model.

## Failure Modes

- **Missing `publishOn`:** Result handler executes on Lettuce I/O thread; any downstream blocking call silently deadlocks the I/O thread under load. Detection: `lettuce.pool.pending` sustained > 0; latency spike on `rpe.detection.timer`.
- **Blocking JDBC on VT scheduler:** PgJDBC `synchronized` pins carrier threads; VT concurrency degrades silently. Detection: `-Djdk.tracePinnedThreads=full` in dev; JFR `jdk.VirtualThreadPinned` in production.
