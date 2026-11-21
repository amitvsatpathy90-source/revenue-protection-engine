---
asset_id: adr-33-rpe-ingest-service
asset_path: docs/adrs/ADR-33.md
asset_type: adr
version: 1.0.0
created: 2026-11-21
last_updated: 2026-11-21
status: PROPOSED
reversal_cost: MEDIUM
owner: amit
tags: [ ingest, webhook, razorpay, idempotency, inbox, kafka, public-surface, lab-validated ]
---

# ADR-33 — rpe-ingest-service: signed gateway webhooks to payment.events

## Status

`PROPOSED` — flips to `ACCEPTED` when implemented and verified on compose (P4). AWS exposure (ALB, TLS) is P5 and
out of scope here. Related: ADR-11 (additive `PaymentEvent` evolution), ADR-22 (shutdown windows).

## Context

Detection consumes `payment.events` (`PaymentEvent`: `eventId`, `accountId`, `amount`, `lat`, `lon`, `timestamp`,
`schemaVersion`). Gateway test-mode webhooks (Razorpay) should drive it with no detection change. The gateway
documents at-least-once delivery, a unique per-event header, no ordering guarantee, a 2xx-within-5s acknowledgement
and non-2xx treated as failure. A webhook carries no `lat`/`lon`.

## Decision

1. New module `rpe-ingest-service` (own pom). It is the only public RPE surface.
2. `POST /webhooks/razorpay`: verify `X-Razorpay-Signature` (HMAC-SHA256 over the raw request bytes, constant-time
   compare) before parsing. The service refuses to start without a webhook secret.
3. Acknowledge 2xx only after a durable Postgres inbox insert. Signature failure: 4xx. Inbox failure: 5xx (the
   gateway retries). Valid but not forwarded events: 2xx.
4. No logic depends on event order. Only `payment.captured` is forwarded; every other event is recorded `IGNORED`.
5. Two uniqueness constraints: `event_id` (header `x-razorpay-event-id`, delivery dedup) and `payment_id` among
   forwarded rows (business dedup). `PaymentEvent.eventId` = payment id.
6. Mapping: `accountId` from `notes.accountId`; missing or unmappable → 2xx, stored `REJECTED` with a reason, not
   forwarded. `lat`/`lon` from a deterministic per-account home location in config, with an optional scenario
   override read from `notes`. `schemaVersion` "1".
7. Staleness gate: captured events older than `ingest.max-event-age` are stored `STALE` and not forwarded (detection
   windows use broker ingest time).
8. The inbox records the server-side `received_at`. One active poller publishes pending rows to `payment.events`
   in `received_at` order per account and sets `received_at` as the Kafka record timestamp, so poller backlog does
   not shift detection time. Requires `payment.events` to use `message.timestamp.type=CreateTime`. Publishing is
   at-least-once; detection dedups on `eventId`.

## Alternatives Considered

| Option                                      | Outcome                                                 |
|---------------------------------------------|---------------------------------------------------------|
| Publish to Kafka inside the request         | A broker stall breaks the 5s acknowledgement. Rejected. |
| Make `lat`/`lon` optional in `PaymentEvent` | Changes the detection contract. Rejected.               |
| IP geolocation                              | External and non-deterministic. Rejected.               |
| Derive `accountId` from payer contact data  | Puts PII in the pipeline. Rejected.                     |
| Forward `authorized` / `order.paid` too     | Double-counts one payment. Rejected.                    |

## Consequences

- Geo-rule results on ingested traffic use simulated geography; any claim must say "synthetic geo".
- Late deliveries beyond the bound are recorded but never reach detection.
- Missed webhooks are not reconciled in the MVP (no order/payment fetch).
- Detection time for an event is its receipt time, not its publish time; out-of-order publication would skip geo
  checks (non-positive delta), so per-account publish order is a requirement, not an optimization.

## Residual Risks

- Whether custom `notes` set at order creation reach the payment entity is not verified.
- The `amount` unit and the `payment.captured` field paths are not verified against the full payload.
- The message key convention for `payment.events` is not confirmed; match detection's.
- Broker behavior with explicit old record timestamps (topic timestamp type, timestamp-difference bounds) is not
  verified on the compose broker.
- Broker behavior with explicit old record timestamps: verified on the compose Redpanda with default topic config
  (a record stamped 10 minutes in the past kept that timestamp on read-back, 2026-11-22). Not verified: much older
  stamps, topic-level overrides on `payment.events`, other brokers.

## Reversal Cost

MEDIUM. A new service, an inbox table and an exposed surface; detection is untouched.

## References

- Razorpay docs: webhook validation and testing; webhook best practices (signature, event id, ordering, retries).

## Changelog

| Version | Date       | Author | Description         |
|---------|------------|--------|---------------------|
| 1.0.0   | 2026-11-21 | @amit  | Initial — PROPOSED. |
