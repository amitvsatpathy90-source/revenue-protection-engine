# ADR-11: JSONB Schema Evolution — Opaque Relay + `@JsonIgnoreProperties`

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

`outbox.payload` is JSONB. If `PaymentEvent` DTO evolves after deployment, old outbox rows or buffered Kafka messages may fail deserialization in the relay or consumer. The relay is a delivery agent — it has no business reason to understand event content.

## Decision

(1) `@JsonIgnoreProperties(ignoreUnknown = true)` on `PaymentEvent` DTO. (2) Relay treats `outbox.payload` as opaque `JsonNode` — never deserializes to `PaymentEvent`. (3) `schema_version` field on `PaymentEvent` for future versioned branching.

## Alternatives

**Relay deserializes to `PaymentEvent`:** Every DTO change is a relay change. Event and relay deployment must be coordinated. Rejected.

**No `@JsonIgnoreProperties`:** Adding a field to `PaymentEvent` crashes consumers deserializing old outbox rows or buffered messages. Breaks zero-downtime deployments. Rejected.

## Consequences

- Additive schema changes (new fields): zero-cost, zero risk.
- Breaking changes (field removal, type change, rename): dual-read migration required during transition window. This is the documented boundary.
- Relay: permanently decoupled from event schema — event DTO evolution has zero relay impact.

## Failure Modes

- **Breaking schema change without dual-read migration:** Consumer deserialization fails; `ErrorHandlingDeserializer` routes to `payment.events.DLT`. Detection: DLT depth rising. Mitigation: dual-read migration + consumer rollforward.
