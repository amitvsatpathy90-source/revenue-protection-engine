---
adr_id: ADR-16
adr_status: ACCEPTED
adr_decided_on: 2026-06-13
adr_reversal_cost: HIGH
adr_supersedes: null
adr_superseded_by: null
---

# ADR-16 — Migrate RPE to Spring Boot 4.1.x Platform Baseline

## Status
ACCEPTED

## Context

Spring Boot 3.5.x OSS support ends 2026-06-30 — 17 days from this decision. RPE currently
targets `Spring Boot 3.3+`. Remaining on 3.x means operating on an unpatched platform from
EOL forward.

Spring Boot 4.0.0 GA shipped 2025-11-20. 4.1.0 GA shipped 2026-06. The platform upgrade
carries independent breaking surfaces across each layer:

| Layer | 3.x | 4.x |
|---|---|---|
| Spring Framework | 6.1.x | 7.0.x |
| Spring Security | 6.4.x | 7.0.x |
| Spring Kafka | 3.x | 4.0.x |
| Jackson | 2.x | 3.x (default) |
| Hibernate | 6.5.x | 7.1.x |
| HikariCP | 5.x | 7.0 |
| Micrometer | 1.13.x | 1.16.x |
| Null safety | `org.springframework.lang` | JSpecify (`org.jspecify`) |
| Embedded server options | Tomcat / Jetty / Undertow | Tomcat / Jetty (Undertow removed) |

This is a platform migration, not a version bump.

RPE is on JDK 21, which Boot 4 supports. No JDK upgrade is required for Boot 4
compatibility (minimum is 17; 21 is the recommended LTS).

## Decision

Migrate RPE to **Spring Boot 4.1.x** (minimum 4.0.x). Treat as a multi-surface platform
migration across the following breaking areas:

**1. Jackson 2 → Jackson 3**
Update package imports where `tools.jackson.*` applies; audit all `catch (IOException)` blocks
adjacent to Jackson serialization — `JacksonException` (Jackson 3) extends `RuntimeException`,
not `IOException`, so these catch blocks will silently drop Jackson errors in production.
Run JSON output comparison tests across all REST contract boundaries before promotion.

**2. Spring Kafka 3 → 4.0**
Audit `@KafkaListener` error handler and `@RetryableTopic` API surface for breaking changes.
Consumer group naming, `isolation.level`, and `enable.auto.commit` config are unchanged.

**3. JSpecify null safety**
Replace `org.springframework.lang.@NonNull/@Nullable` with `org.jspecify.annotations.*`.
Add `@NullMarked` at package level. Use `@NullUnmarked` as a bounded escape hatch for legacy
paths only — always mark with a TODO as tech debt, not permanent state.

**4. Undertow removal**
Not used in RPE (Netty for WebFlux HTTP surface; virtual threads for pipeline). No action
required. Confirm no transitive dependency pulled it in.

**5. Liveness/readiness probes**
Both probes are now enabled by default in Boot 4. Explicitly configure
`management.endpoint.health.probes.enabled` — do not rely on absence = disabled.

**6. Modular autoconfigure**
Audit any internal Boot autoconfigure imports (`org.springframework.boot.autoconfigure.*`
internals) for compile breaks. The JAR is now split into 70+ focused modules.

**7. Deprecated API removal**
All `@Deprecated` callsites from 3.x are removed in 4.0. Compile errors are the gate.

**Migration path (mandatory sequence):**
```
3.3.x
  └──► 3.5.x (latest patch) — resolve ALL @Deprecated warnings
         └──► 4.0.x — fix compile errors; run full integration suite
                └──► 4.1.x — final target
```
Do not jump from 3.3.x directly to 4.0.x.

## Alternatives Considered

| Option | Disposition | Reason |
|---|---|---|
| Stay on Spring Boot 3.5.x | **Rejected** | EOL 2026-06-30; unpatched CVEs from that date; compliance risk |
| Stay on Spring Boot 3.3.x | **Rejected** | Already EOL 2025-06-30; in breach of platform policy now |
| Migrate to Quarkus or Micronaut | **Rejected** | Full rewrite; no portfolio continuity; out of RPE scope |
| Skip 3.5.x intermediate step | **Rejected** | `@Deprecated` removals in 4.0 produce compile errors without the intermediate pass; 3.5.x surfaces them as warnings first |
| Use `spring-boot-starter-classic` (Jackson 2 bridge) permanently | **Rejected** | Transitional bridge only; defers, does not resolve the breaking surface |

## Consequences

### Positive
- Unpatched CVE risk eliminated.
- Spring Kafka 4.0 aligns with Kafka 4.0 client (KRaft-native; ZooKeeper removal complete).
- HikariCP 7.0 has virtual-thread calibrated defaults — less manual pool sizing.
- JSpecify null safety enforced by IntelliJ 2025.3+ tooling.
- Modular autoconfigure reduces startup time and native-image size.

### Negative / Risks
- **Jackson 3 catch-block risk is the highest incident surface.** `catch (IOException)` around
  Jackson call sites silently swallows Jackson errors in production — no compiler warning.
- **JSON output shape may change silently.** Jackson 3 serialization defaults differ from 2.x
  for null fields and date/time formatting. Integration tests validating REST contract JSON
  shapes will fail — this is the detection mechanism, not a failure mode. Must pass before
  promotion.
- Spring Kafka 4.0 error handler API changes require audit of `DefaultErrorHandler` and
  `@RetryableTopic` configurations. 3.x patterns may compile but behave differently.
- Hibernate 7.1 may alter behaviour on named native queries — regression suite required.
- Migration requires an intermediate 3.5.x step; adds elapsed time before 4.x lands.

## Failure Modes

| Mode | Detection | Mitigation |
|---|---|---|
| `JacksonException` swallowed by stale `catch (IOException)` | Integration tests on all REST endpoints; grep `catch.*IOException` near Jackson call sites | Replace with `catch (JacksonException e)` at every Jackson call site |
| JSON output shape regression at REST boundary | JSON body comparison tests on all `@RestController` endpoints before promote | Add contract tests asserting full response body, not just status |
| Spring Kafka 4.0 `DefaultErrorHandler` misconfiguration | Integration test with poison message injection → assert DLT routing works | Audit `DefaultErrorHandler` construction against Spring Kafka 4.0 changelog |
| Compile failure from removed 3.x deprecated APIs | Build breaks immediately on 4.0 bump | 3.5.x deprecation pass eliminates this — do not skip it |
| Probe misconfiguration (probes now default-on in Boot 4) | `/actuator/health/liveness` returns unexpected 404/503 in staging | Explicitly set `management.endpoint.health.probes.enabled` |
| Modular autoconfigure breaks internal import | Compile failure on `org.springframework.boot.autoconfigure.*` internals | Audit internal Boot imports before the 4.0 bump |

## Reversal Cost

**HIGH.** Downgrading from Boot 4 to 3.x requires reverting Jackson 3 → 2 API changes,
JSpecify → `org.springframework.lang` annotation rollback, and Kafka/Hibernate version
downgrades. Feasible but multi-day effort with regression risk on every reverted dependency.
No data migration required — all changes are behavioural and library-level.

## Validating Assumptions

| # | Assumption | Status | Invalidator |
|---|---|---|---|
| 1 | Spring Boot 4.1.x is stable on JDK 21 for production-grade workloads | INFERRED from GA status | Regression failures in integration test suite post-migration |
| 2 | RPE has no internal Boot autoconfigure imports that break at compile | UNVERIFIED — requires audit | Compile failure on first 4.0 bump |
| 3 | Netty (WebFlux HTTP surface) is unaffected by Boot 4 server-layer changes | INFERRED — Undertow removal is the only embedded server change; Netty is retained | WebFlux endpoint failures in smoke test |
| 4 | Jackson 3 serialization produces identical output for `PaymentEvent` and alert DTOs | UNVERIFIED — requires JSON output tests | JSON contract test failures |
| 5 | Spring Kafka 4.0 `@KafkaListener` + `isolation.level=read_committed` config is backward-compatible | INFERRED from changelog review | Consumer test failures with transactional producer source |

## Implementation Notes

**Migration sequence (owner: amit) — COMPLETED 2026-06-14:**
- [x] Bump `spring-boot-starter-parent` to `3.5.x` — run `mvn verify` — fix all `@Deprecated` callsites
- [x] Grep for `catch.*IOException` adjacent to Jackson call sites; document all hits before fixing
- [ ] ~~Replace `org.springframework.lang.@NonNull/@Nullable` with JSpecify annotations project-wide~~
      _**CORRECTED 2026-07-04 (doc-drift audit):** this was marked done but never actually happened.
      A repo-wide grep finds zero `org.jspecify` imports, zero `@NullMarked`/`@NullUnmarked`, and
      zero `package-info.java` files — and zero of the old `org.springframework.lang` annotations
      either. RPE carries no null-safety annotations of any generation. Re-opened as unstarted work,
      not closed out. See Spring Boot 4 Migration Policy for the paired correction._
- [x] Bump to `spring-boot-starter-parent 4.0.x` — fix compile errors
      _Boot 4.0 autoconfigure split: added `spring-boot-kafka`, `spring-boot-jackson2`, `spring-boot-flyway`;_
      _`KafkaProperties` import → `org.springframework.boot.kafka.autoconfigure`; `buildConsumerProperties(null)` → no-arg;_
      _`Health`/`HealthIndicator` → `org.springframework.boot.health.contributor`; `DataSourceProperties` → `org.springframework.boot.jdbc.autoconfigure`_
- [x] Replace `catch (IOException)` at Jackson call sites with `catch (JacksonException)` _(N/A — RPE's
      own Jackson call sites stay on Jackson 2 (`com.fasterxml.jackson`, checked `JsonProcessingException`)
      via the `spring-boot-jackson2` compatibility module, **permanently** — not a "Spring Kafka 2.x
      compat layer" as originally (incorrectly) written here, but because Spring Kafka 4.x's own
      `JsonDeserializer`/`JsonSerializer` still operate on Jackson 2 types internally. No Jackson-3
      `JacksonException` migration applies to this call surface; see Spring Boot 4 Jackson Protocol
      §Jackson, corrected 2026-07-04._
- [x] Run JSON output comparison tests across all `@RestController` endpoints; fix failures _(no `@RestController` endpoints; all 53 integration tests pass)_
- [x] Audit the Kafka consumer error-handler config against Spring Kafka 4.0 `DefaultErrorHandler` changelog _(API unchanged; `buildConsumerProperties(null)` → no-arg was the only breaking change)_
- [x] Verify `/actuator/health/liveness` and `/actuator/health/readiness` are intentionally configured
      _Added `management.endpoint.health.probes.enabled: true` to `application.yml`_
- [x] Bump to `spring-boot-starter-parent 4.1.x` — final target (core: 53/53 ✓, triage: 20/20 ✓)
- [x] Update `Architecture Spec` Stack block: `Spring Boot 3.3+` → `Spring Boot 4.1.x`

**Code/config anchors:**
- `pom.xml` `<parent>` version block
- Spring Boot 4 Developer Guidelines (behavioral rules for development sessions)

**Observability deltas:** none — Micrometer 1.16 is backward-compatible with existing metric names and label sets.

## References

- `PLATFORM_CONTEXT_SPRING_BOOT_4.md` (external project knowledge used during this decision, 2026-06-13 — not a file tracked in this repo)
- [Spring Boot 4.0 Release Notes](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Release-Notes) (accessed 2026-06-13)
- [Spring Boot 4.0 Migration Guide](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide) (accessed 2026-06-13)
- [Spring Boot EOL table — endoflife.date](https://endoflife.date/spring-boot) (accessed 2026-06-13)

## Open Questions

- Whether OpenRewrite Boot 4 recipe covers RPE's annotation surface adequately — evaluate before
  mechanical rename pass; do not treat as a substitute for manual Jackson catch-block audit.
  Track: pre-sprint-start.
- Spring Kafka 4.0 `@RetryableTopic` API compatibility with current DLT routing — track as
  part of Kafka audit task above.

## Changelog

| Date | Version | Edit | Author | Change |
|---|---|---|---|---|
| 2026-07-04 | 1.0.1 | correction | amit | Doc-drift audit: the JSpecify migration checklist item was marked `[x]` complete but has zero evidence anywhere in the repo (no `org.jspecify`, no `@NullMarked`/`@NullUnmarked`, no `package-info.java`, and no old `org.springframework.lang` annotations either) — reopened as unstarted. Also corrected the Jackson catch-block item's wording ("Spring Kafka 2.x compat layer" → the actual reason: Spring Kafka 4.x's `JsonDeserializer`/`JsonSerializer` require Jackson 2 internally, so `spring-boot-jackson2` is a permanent dependency, not transitional). No change to migration status (still ACCEPTED/complete for the parts that were actually done: Boot 4.1.x, Spring Kafka 4.0, probes, autoconfigure split). |
| 2026-06-13 | 1.0.0 | — | amit | Initial — status ACCEPTED; EOL forcing function; no viable alternative to migration |
