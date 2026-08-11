<!-- edit-log (newest first): v1.0 | 2026-06-17 | Initial. ACCEPTED. -->

---
asset_id: adr-18-payment-alerts-dlt-per-consumer-group
asset_path: docs/adrs/ADR-18.md
asset_type: adr
version: 1.0.0
created: 2026-06-17
last_updated: 2026-06-17
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [kafka, dlt, dead-letter, rpe-alert-service, rpe-triage-agent, adr-17-follow-on]
---

# ADR-18 — One DLT topic per consumer group on `payment.alerts`; eliminate shared producer

## Status

`ACCEPTED`

Supersedes: nothing.
Related: ADR-17 (single-writer principle), ADR-10 (DLQ policy), ADR-15 (triage agent).

---

## Context

`payment.alerts` has two independent consumers:

| Consumer | Group | Purpose |
|---|---|---|
| `rpe-alert-service` | `rpe-alert-service.alerts.v1` | Idempotent alert actioning; writes `processed_alerts` |
| `rpe-triage-agent` | `rpe-triage-agent.alerts.v1` | Advisory LLM enrichment; writes `payment.alerts.triaged` |

Post-ADR-17 (service decomposition), a topology cleanup exposed that `payment.alerts.DLT` still carried two producers — `rpe-alert-service` and `rpe-triage-agent` — both routing terminal failures to the same topic by default.

This violates ADR-17's single-writer principle in two compounding ways:

1. **Ownership ambiguity.** No single service owns the schema or retention policy of the shared DLT. Schema evolution on the failure envelope requires coordinating both services.

2. **Failure-domain conflation.** Alert-service failures (bad `processed_alerts` write, downstream actioning error) and triage-agent failures (LLM timeout cascade, schema-validation failure, tool-call budget exceeded) have entirely different:
   - Root causes
   - Retry policies (alert-service: bounded Resilience4j; triage: R4j full stack including TimeLimiter + CB)
   - Triage procedures (alert failures are correctness-critical; triage failures are advisory)
   - Escalation paths

Mixing them on one topic means DLT consumers must discriminate by inspecting `x-original-consumer-group` headers — that's a workaround for a topology that should have been correct by construction.

Spring Kafka's `DeadLetterPublishingRecoverer` default destination is `<source-topic>.DLT`. Without explicit configuration, both services would write to `payment.alerts.DLT`, making the shared-producer problem invisible until a DLT triage consumer is wired.

---

## Decision

**Split `payment.alerts`'s dead-letter surface into one topic per consumer group:**

| Topic | Sole writer | For |
|---|---|---|
| `payment.alerts.DLT` | `rpe-alert-service` | Terminal failures from alert actioning |
| `payment.alerts.triage.DLT` | `rpe-triage-agent` | Terminal failures consuming `payment.alerts` (pre-triage) |

`payment.alerts.triaged.DLT` (already exists, unchanged) remains the sole-writer DLT for downstream consumers of `payment.alerts.triaged`. It is not in scope here.

**Implementation requirement — `rpe-triage-agent`:**

`DeadLetterPublishingRecoverer` must be configured with a custom destination resolver:

```java
@Bean
public DeadLetterPublishingRecoverer triageAlertsDltRecoverer(KafkaTemplate<?, ?> template) {
    return new DeadLetterPublishingRecoverer(
        template,
        (record, ex) -> new TopicPartition("payment.alerts.triage.DLT", -1) // -1 = any partition
    );
}
```

`rpe-alert-service` requires no custom config — Spring Kafka's default suffix (`payment.alerts` + `.DLT`) is correct.

**No change to `rpe-alert-service`** beyond updating its declared DLT topic ownership in the topology map. No code change required.

---

## Alternatives Considered

| Option | Chosen / Rejected | Reason |
|---|---|---|
| Keep shared `payment.alerts.DLT` | Rejected | Violates ADR-17 single-writer. Two independent failure domains under one topic; schema evolution requires cross-service coordination; triage failures mixed with alert-actioning failures makes triage harder and ops runbooks ambiguous. |
| Route triage terminal failures → `payment.alerts.triaged.DLT` | Rejected | Semantically wrong. `payment.alerts.triaged.DLT` is the DLT for consumers of the triaged *output*, not for failures in the triage agent's *input* consumption. Mixing input-side and output-side failures on the same DLT destroys the causal chain that DLT triage depends on. |
| Route triage terminal failures → `payment.alerts.triaged` with `triage_status=FAILED` | Rejected | `payment.alerts.triaged` is a business topic, not a failure envelope. Downstream consumers would need to filter on `triage_status` to avoid acting on failure records. Conflates normal output with infrastructure failures. |
| Single catch-all `payment.dlt` | Rejected | Loses topic-level routing, per-consumer retention differentiation, and makes the ACL model incoherent. Strictly worse than the status quo. |

---

## Consequences

**Positive:**
- Single-writer invariant (ADR-17) now holds for DLTs on `payment.alerts`.
- Each DLT has its own retention, ACL, and triage consumer — scoped to the failure domain it represents.
- Alert-actioning failures and LLM-enrichment failures are independently observable and independently replayable without cross-service coordination.
- DLT ownership is explicit in topology: clear ACL grants, clear schema subject ownership.

**Negative:**
- One additional topic (`payment.alerts.triage.DLT`) to declare in IaC and register in the topology map.
- `rpe-triage-agent` requires a custom `DeadLetterPublishingRecoverer` bean — the Spring Kafka default is not sufficient. If that bean is absent or misconfigured, the agent silently falls back to writing `payment.alerts.DLT`, violating the separation. Requires a test or startup assertion to catch.

---

## Failure Modes

| Failure | Detection |
|---|---|
| `rpe-triage-agent` misconfigured `DeadLetterPublishingRecoverer` — silently writes to `payment.alerts.DLT` | ACL: grant `rpe-triage-agent` principal WRITE on `payment.alerts.triage.DLT` only, not `payment.alerts.DLT`. Broker will reject misdirected writes at runtime. Verify in integration test: send a poison record → assert no record lands on `payment.alerts.DLT`. |
| `payment.alerts.triage.DLT` not declared in IaC → auto-create hits `auto.create.topics.enable=false` → `DeadLetterPublishingRecoverer` throws `UnknownTopicOrPartitionException` → escalates to fatal consumer error | Pre-deploy IaC check: `kafka-topics --describe --topic payment.alerts.triage.DLT` must succeed before `rpe-triage-agent` deploys. |
| DLT triage consumer for `payment.alerts.triage.DLT` never wired → triage failures accumulate silently | Retention alarm: `payment.alerts.triage.DLT` offset lag > 0 after 30 min with no active consumer group → alert. |

---

## Reversal Cost

`LOW` — both topics are declaration-only changes. Recombining them requires removing the custom `DeadLetterPublishingRecoverer` bean in `rpe-triage-agent` and merging the IaC topic declarations. No data migration; existing DLT records are not replayed by default (ADR-10).

---

## References

- ADR-17 §3.4 — single-writer table ownership principle
- ADR-15 — triage agent design; R4j full stack on LLM boundary
- ADR-10 — no auto-replay from DLT
- Spring Kafka `DeadLetterPublishingRecoverer` — destination resolver API
