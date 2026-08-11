<!-- edit-log (newest first): v1.0 | 2026-06-28 | Initial. ACCEPTED. -->

---
asset_id: adr-20-kafka-transport-auth-and-acls
asset_path: docs/adrs/ADR-20.md
asset_type: adr
version: 1.0.0
created: 2026-06-28
last_updated: 2026-06-28
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [security, kafka, sasl, scram, mtls, acl, zero-trust, adr-19-follow-on, adr-17, adr-18]
---

# ADR-20 — Kafka transport authentication (SASL_SSL/SCRAM) + per-service principal ACLs

## Status

`ACCEPTED`

Supersedes: nothing.
Related: ADR-19 (zero-trust; Actuator auth — this is the bus-side counterpart, residual R4),
ADR-17 (one-writer-per-topic ownership), ADR-18 (one DLT per consumer group; foresaw ACLs),
ADR-06 (relay `transactional.id` from `RELAY_INSTANCE_ID`).

---

## Context

ADR-19 authenticated the HTTP/Actuator surface but explicitly left Kafka transport auth out of
scope (R4). The bus the entire system runs on is still `PLAINTEXT`: anything able to reach the
broker can forge `payment.alerts`, consume PII-bearing `payment.events`, or read alert traffic.
Network reachability is not authorization — the zero-trust principle must extend to the broker.

RPE already has the two ingredients that make this cheap: **one writer per topic** (ADR-17/18)
and a **per-service identity**. That ownership matrix *is* the least-privilege ACL matrix.

---

## Decision

1. **Transport auth = SASL_SSL + SCRAM-SHA-512** (primary). One SCRAM principal per service
   (`rpe-detection`, `rpe-relay`, `rpe-alert`, `rpe-triage`). TLS encrypts the wire; SCRAM
   authenticates the client. **mTLS is the documented alternative** (no shared secret, at the
   cost of a client-cert lifecycle) for deployments already running a mesh CA.
2. **Credentials from env, assembled in code.** A per-service `KafkaSecurity` helper (copy, not
   a shared jar — microservices.md §1.4) folds username/password into the `sasl.jaas.config`
   string in memory. The secret never appears in `application.yml`, logs, or `/actuator/configprops`.
3. **Fail-closed.** When `rpe.kafka.security.protocol` is `SASL_*` and credentials are missing,
   startup aborts (same discipline as `DB_PASSWORD` / ADR-19). `PLAINTEXT` is an explicit lab
   choice and the default, so `docker compose up` is unaffected; it is a behavioural no-op vs.
   today (it only sets `security.protocol` explicitly). This is environment config, not a
   feature flag — distinct from the banned `*.enabled` toggles.
4. **Per-service least-privilege ACLs** (`deploy/kafka/provision-acls.sh`, idempotent):

   | Principal | Resource | Operations |
   |---|---|---|
   | `rpe-detection` | topic `payment.events` / group `rpe-payment-consumer` | READ |
   | | topic `payment.events.DLT` | WRITE, DESCRIBE |
   | | cluster | IDEMPOTENT_WRITE |
   | `rpe-relay` | topic `payment.alerts` | WRITE, DESCRIBE |
   | | transactional-id `${RELAY_INSTANCE_ID}-*` (prefixed) | WRITE, DESCRIBE |
   | | cluster | IDEMPOTENT_WRITE |
   | `rpe-alert` | topic `payment.alerts` / group `rpe-alert-consumer` | READ |
   | | topic `payment.alerts.DLT` | WRITE, DESCRIBE |
   | `rpe-triage` | topic `payment.alerts` / group `rpe-triage-agent` | READ |
   | | topics `payment.alerts.triaged`, `payment.alerts.triage.DLT` | WRITE, DESCRIBE |

   This makes one-writer-per-topic (ADR-17/18) **broker-enforced**: a misconfigured
   `DeadLetterPublishingRecoverer` (the ADR-18 hazard) is rejected with
   `TopicAuthorizationException` instead of silently writing the wrong owner's topic.

---

## Alternatives Considered

| Option | Chosen / Rejected | Reason |
|---|---|---|
| mTLS (client certs) | Alternative, not primary | No shared secret, but adds CA + per-service keystore + cert-expiry monitoring (the Stage-6 cert burden). SCRAM rotates with one broker command. |
| `sasl.jaas.config` in `application.yml` | Rejected | Puts the password in a property source / config-map / heap-dump-visible map. Assembled in code instead. |
| One shared Kafka user for all services | Rejected | Destroys per-topic least privilege; a single compromised service = full bus access; defeats one-writer enforcement. |
| `security.enabled` toggle | Rejected | Silent-insecure bypass; violates no-feature-flags rule. Protocol is plain env config. |
| Leave PLAINTEXT, rely on NetworkPolicy | Rejected | Single network layer; identical reasoning to ADR-19. |

---

## Consequences

**Positive:**
- The bus is authenticated + encrypted; one-writer-per-topic is enforced at the broker.
- Identity model is per-service and unchanged by monolith↔microservice form.
- Forged/misrouted produce attempts fail loudly (`TopicAuthorizationException`), not silently.

**Negative:**
- 4 SCRAM users + an ACL matrix to provision and keep in step with topic changes (pinned by `kafka-security.md`).
- Prod/k8s must mount a broker truststore and the per-service credentials; PLAINTEXT lab and SASL_SSL prod now diverge in config.
- The relay's transactional producer needs the prefixed TransactionalId ACL or exactly-once init fails (covered by the matrix).

---

## Failure Modes

| Failure | Detection / Mitigation |
|---|---|
| SASL_* protocol, missing creds | Fail-closed startup abort (`KafkaSecurity` throws). |
| Relay missing TransactionalId ACL | `TransactionalIdAuthorizationException` at producer init — fail-fast; matrix grants prefixed `${RELAY_INSTANCE_ID}-`. |
| Wrong-topic write (ADR-18 hazard) | `TopicAuthorizationException` at the broker — the runtime guard ADR-18 asked for. |
| SCRAM password rotation | Dual-credential rotate broker-side, roll services, retire old — no downtime. |
| Credential leak | JAAS assembled in `KafkaSecurity`, never logged; secrets via env only; `.env` git-ignored; configprops locked (ADR-19). |

---

## Residual Risks (explicit)

- **R1 — Lab runs PLAINTEXT.** Documented, env-gated; prod/k8s sets SASL_SSL. The lab broker is single-node RF=1 anyway (existing limitation).
- **R2 — SCRAM is a shared secret per service.** mTLS is the no-shared-secret upgrade; recommended where a mesh CA already exists.
- **R3 — Broker-CA trust bootstrapping is out of scope.** Assumes a provisioned truststore; cert distribution is an infra concern.
- **R4 — ACL drift.** A new topic without a matching single-writer ACL silently fails at runtime; `kafka-security.md` makes "new topic ⇒ ACL in the same PR" binding, but there is no automated reconciler yet.
- **R5 — Admin principal for provisioning** has cluster ALTER; must be tightly held (separate from the per-service runtime principals).

---

## Reversal Cost

`LOW` — set `RPE_KAFKA_SECURITY_PROTOCOL=PLAINTEXT` (clients revert to no-op) and drop the ACLs.
No data migration; topics and offsets are unaffected.

---

## References

- ADR-19 §residual R4 — Kafka transport auth deferred to here
- ADR-17 §3.4 / ADR-18 — one-writer-per-topic ownership = the ACL matrix
- ADR-06 — relay `transactional.id` prefix from `RELAY_INSTANCE_ID`
- `ADR-20.md` — enforcement detail
- `deploy/kafka/provision-acls.sh` — the idempotent provisioning script
