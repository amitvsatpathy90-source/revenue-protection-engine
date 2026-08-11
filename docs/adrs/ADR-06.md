# ADR-06: Relay `transactional.id` — Environment-Injected Instance Identity

**Status:** ACCEPTED | **Decided:** 2026-05-20 | **Owner:** amit | **Reversal cost:** LOW

---

## Context

Kafka transactional producers require a stable, unique `transactional.id` per producer instance. Two instances sharing the same ID trigger `ProducerFencedException` in a loop, halting relay delivery. Hardcoded IDs cause the same problem on parallel deployments.

## Decision

`transactional.id` injected from environment: `spring.kafka.producer.transaction-id-prefix=${RELAY_INSTANCE_ID:relay-default}-`. Docker: `RELAY_INSTANCE_ID=${HOSTNAME}`. Kubernetes: `RELAY_INSTANCE_ID=$(POD_NAME)` via downward API. Hard rule: never run two relay instances with identical `RELAY_INSTANCE_ID`.

## Alternatives

**UUID at startup:** Unique, but not stable across restarts of the same instance. Kafka bumps the epoch on restart with a new ID, losing fencing continuity. Rejected.

**Hardcoded per-deployment config:** Operationally fragile; breaks on parallel deployment without manual coordination. Rejected.

## Consequences

- Environment identity is unique per host/pod (no coordination) and stable across restarts (same hostname → same epoch management).
- Config discipline only — zero code complexity. Enforcement is operational, not technical.

## Failure Modes

- **Two instances share `RELAY_INSTANCE_ID`:** `ProducerFencedException` loop; relay stalls. Detection: `ProducerFencedException` in logs; `rpe.outbox.pending.age_seconds` rising. Mitigation: fix env config; restart affected instance.
