<!-- edit-log (newest first): v1.0 | 2026-06-28 | Initial. ACCEPTED. -->

---
asset_id: adr-19-zero-trust-actuator-boundary
asset_path: docs/adrs/ADR-19.md
asset_type: adr
version: 1.0.0
created: 2026-06-28
last_updated: 2026-06-28
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [security, zero-trust, actuator, oauth2, resource-server, jwt, prometheus, adr-17-follow-on]
---

# ADR-19 — Zero-trust posture: Kafka/network is the trust boundary; the Actuator surface is authenticated via OAuth2 Resource Server

## Status

`ACCEPTED`

Supersedes: nothing.
Related: ADR-17 (Kafka-only async; per-service ownership), ADR-01 (WebFlux/Netty HTTP surface),
ADR-13 (deterministic `alert_id` as cross-service join key), `security.md`,
`ADR-19.md`.

---

## Context

RPE exposes **no external business HTTP API** and makes **no synchronous service-to-service
calls** (ADR-17 — communication is Kafka-only, async). Ingestion is via `payment.events`, not
HTTP. The only HTTP surface any service exposes is its **Actuator** (`health`, `info`,
`prometheus`).

A codebase audit found that surface was:

1. **Unauthenticated** — no `spring-security` on any service classpath.
2. **Over-sharing** — `management.endpoint.health.show-details: always` leaked component and
   dependency health (Redis/Postgres/Kafka up-down, pool detail) to any caller able to reach
   the port.
3. Protected only by the Stage-6 k8s default-deny `NetworkPolicy` — a single network layer.

A literal reading of "secure boundaries with JWT/OAuth2" tempts a user-token-on-a-REST-endpoint
design, but there is **no business endpoint to attach it to**. The genuine unprotected surface
is the management/scrape endpoints, and the zero-trust principle (*never trust the network*)
requires that **reachability ≠ authorization** even for an internal Prometheus scrape.

---

## Decision

1. **Ratify the trust boundary.** Kafka topics + the k8s default-deny NetworkPolicy are the
   primary inter-service trust boundary. There is intentionally **no application-layer token
   between services** — there are no synchronous calls to authenticate. Cross-service
   correlation continues to ride the masked `event_id`/`account_id` and the deterministic
   `alert_id` (ADR-13), not a new per-service identity.

2. **Authenticate the Actuator surface** in every service as an **OAuth2 Resource Server (JWT)**:
   - `/actuator/prometheus` requires authority `SCOPE_metrics:scrape` (least privilege).
   - aggregate `/actuator/health` and `/actuator/info` require `authenticated`.
   - everything else is `denyAll` (no business surface exists).

3. **Liveness/readiness public + detail-free.** `/actuator/health/liveness` and
   `/actuator/health/readiness` are `permitAll` (the kubelet cannot present a token);
   `show-details`/`show-components` drop to `when-authorized` so no component detail leaks
   unauthenticated.

4. **Fail-closed.** `rpe.security.oauth2.issuer` and `jwk-set-uri` have **no defaults** — a
   missing env var aborts startup (same discipline as `DB_PASSWORD`). There is deliberately
   **no `security.enabled` flag**: a toggle is a silent-insecure bypass and violates the
   no-feature-flags rule (`reactive-pipeline.md`).

5. **Audience binding is mandatory** (`AudienceValidator`) — closes the OAuth2 confused-deputy
   hole (a signature-valid token minted for another relying party must not be accepted).

6. **Per-service config class**, gated `@ConditionalOnWebApplication` — reactive
   (`SecurityWebFilterChain`) for `rpe-detection-service`, servlet (`SecurityFilterChain`) for
   the three MVC services. No shared `rpe-common` jar (ADR-17 non-goal #2); the class is
   duplicated like `Pii`. The conditional keeps the fail-closed decoder out of the
   `webEnvironment = NONE` integration contexts so every suite still loads.

7. **JWKS fetch is timeout-bounded** (connect + read) on a bounded client — the only outbound
   network call in the auth path.

8. **Prometheus authenticates its scrapes** — lab via a static bearer token file; prod via
   `oauth2` client-credentials.

---

## Alternatives Considered

| Option | Chosen / Rejected | Reason |
|---|---|---|
| Leave Actuator open; rely on NetworkPolicy only | Rejected | Single network layer; violates "never trust the network". |
| `spring.security.user` basic auth | Rejected | Shared static credential; no audience/scope/rotation; not token-based. |
| User JWT on a business REST endpoint | N/A | No business HTTP endpoint exists (Kafka-only ingestion). |
| Separate `management.server.port` + secured child context | Deferred | Reactive child-context security is fragile/version-sensitive; chose same-port + NetworkPolicy. Separate port noted as future hardening. |
| `security.enabled` dev toggle | Rejected | Silent-insecure bypass; violates no-feature-flags rule. Dev points at a local issuer instead. |
| Skip audience validation (issuer + signature only) | Rejected | Leaves the confused-deputy hole open — any valid-issuer token would pass. |

---

## Consequences

**Positive:**
- The management surface is authenticated and scope-scoped; component health no longer leaks unauthenticated.
- Posture is identical in monolith and microservice form — each service already self-authenticates; extraction changes nothing.
- Ready to extend to a real external API later (`hasAuthority`/method security) with zero boundary rework.

**Negative:**
- Boot now depends on JWKS reachability (fail-closed). An IdP outage blocks fresh starts.
- +1 config class + 1 validator per service to keep behaviorally in sync (pinned by `actuator-security.md`).
- The canonical compose / k8s topologies now require `RPE_OAUTH_ISSUER` + `RPE_OAUTH_JWKS_URI` to be wired (a local Keycloak or static JWKS) — services will not boot otherwise. Tracked as a follow-up infra task.

---

## Failure Modes

| Failure | Detection / Mitigation |
|---|---|
| IdP/JWKS down at boot | Fail-closed (service refuses to start). Mitigate with HA IdP, cached JWKS, bounded 2s timeout (fails fast). Lab may pin a static JWKS. |
| JWKS key rotation mid-run | Nimbus refetches on unknown `kid`; bounded timeout prevents a hung refresh. Overlap-publish old+new keys during rotation. |
| Confused-deputy (valid-issuer token, wrong audience) | Rejected by `AudienceValidator` (unit-tested). |
| Bearer token theft | Blast radius bounded to read-only `metrics:scrape` + this audience; short TTL. Recommend mesh mTLS as a second factor (residual R2). |
| `show-details: always` regression | `actuator-security.md` rule + follow-up integration assertion that unauthenticated `/actuator/health` is detail-free. |
| Prometheus token expired/missing | Scrape returns 401 → target flips `down` → existing target-up alert fires (observable, not silent). |

---

## Residual Risks (explicit)

- **R1 — Liveness/readiness unauthenticated.** Mitigation: detail-free (UP/DOWN), NetworkPolicy-scoped. Residual: existence/up-down observable to anyone on the pod network.
- **R2 — Bearer token theft / no mTLS yet.** Mitigation: short TTL, audience+scope binding, read-only scope. Residual: recommend mesh/ingress mTLS as a second factor.
- **R3 — Lab static scrape token is long-lived.** Residual: prod must switch to client-credentials; the lab token must stay out of git (`.env`).
- **R4 — Kafka transport not authenticated here.** Broker SASL/mTLS + topic ACLs are the broker's concern (next ADR). Residual until broker auth is configured — this ADR secures the HTTP surface only.
- **R5 — IdP single point of failure at boot** (fail-closed). Mitigation: HA IdP / cached JWKS.

---

## Reversal Cost

`LOW` — remove the two starters + the config class/validator per service and revert the
`application.yml` blocks. No data migration; tokens are not persisted.

---

## References

- ADR-17 §1/§3.4 — Kafka-only async; per-service ownership; no shared code module
- ADR-01 — WebFlux/Netty HTTP surface (detection); MVC elsewhere
- ADR-13 — deterministic `alert_id` as the cross-service join key
- `ADR-19.md` — enforcement detail
- `security.md` — PII/actuator-exposure rules (now asserted *and* authenticated)
- Spring Security 7.0 reactive/servlet resource-server DSL; Nimbus JWKS decoder
