# RPE Synthetic E2E v1 — Assertions

Pipeline is async end-to-end (Kafka consumer -> lane -> Redis -> outbox -> relay -> Kafka ->
alert-service -> processed_alerts, +triage). **Poll, don't assert immediately** — give each
stage a few seconds before checking. `alert_id` is deterministic UUIDv5(event_id + rule_name)
per ADR-14/the operating contract, so eventId below lets you find the corresponding alert
without needing to compute the UUID yourself (`processed_alerts.rule_name`/join on account_id
+ produced_at is enough to disambiguate).

## Expected outcomes per cohort

| Cohort | Events sent | Expected alerts | Expected DB delta |
|---|---|---|---|
| 1. Clean | 12 (4 accts x 3) | 0 | 0 — clean events never touch Postgres |
| 2. Velocity boundary | 101 | 1 (`ruleName=velocity`, `eventId=velocity-101`) | +1 outbox, +1 processed_alerts |
| 3. Z-score | 31 | 1 (`ruleName=zscore`, `eventId=zscore-outlier-01`) | +1 outbox, +1 processed_alerts |
| 4a. Geo below threshold | 2 | 0 | 0 |
| 4b. Geo above threshold | 2 | 1 (`ruleName=geo`, `eventId=geo-above-second`) | +1 outbox, +1 processed_alerts |
| 5. Precedence | 101 | 1 (`ruleName=velocity` — **not** `zscore** despite amount=5000, `eventId=precedence-trigger-01`) | +1 outbox, +1 processed_alerts |
| 6a. Duplicate event (gate dedup) | 32 (2 identical `eventId=dedup4a-outlier-01`) | 1 | +1 outbox, +1 processed_alerts (not 2) |
| 6b. Duplicate alert (alert-service dedup) | 2 identical `alertId` direct to `payment.alerts` | n/a (bypasses detection) | +1 processed_alerts row for that `alert_id` (not 2) |

**Total new rows expected**: 5 outbox / 5 processed_alerts from detection-originated alerts
(cohorts 2,3,4b,5,6a) + 1 more processed_alerts row from 6b's direct injection = **6
processed_alerts, 5 outbox**.

`rpe-triage-agent` consumes `payment.alerts` directly on its own consumer group
(`rpe-triage-agent`), independent of the producer — it has no visibility into whether a
message came from the relay or was hand-injected. **6b's injected alert IS eligible for
triage** if `rpe-triage-agent` is running: bypassing detection/outbox does not bypass triage,
since triage sits downstream of `payment.alerts` itself, not downstream of the relay
specifically. Expect **6** `triaged_alerts` rows total (one per distinct `alert_id` across all
six alerting cohorts), not 5. The second (duplicate) delivery in 6b additionally exercises
triage's own inbox dedup (`triaged_alerts` `INSERT ... ON CONFLICT DO NOTHING` on `alert_id`)
— a bonus assertion this cohort wasn't originally scoped to prove.

## DB queries

```bash
# Per-service schemas (Stage 6): detection.outbox, alert.processed_alerts, triage.triaged_alerts.
# Host psql: use $DB_URL; inside postgres container: use -U rpe -d rpe.
psql -U rpe -d rpe -c "SELECT status, count(*) FROM detection.outbox GROUP BY status;"
psql -U rpe -d rpe -c "SELECT rule_name, count(*) FROM alert.processed_alerts GROUP BY rule_name ORDER BY 1;"
psql -U rpe -d rpe -c "SELECT account_id, rule_name, alert_id FROM alert.processed_alerts ORDER BY acted_at;"
psql -U rpe -d rpe -c "SELECT count(*) FROM alert.processed_alerts WHERE alert_id = '4b7c2b8e-0000-4000-8000-000000000001';"  -- expect 1, not 2
psql -U rpe -d rpe -c "SELECT triage_status, count(*) FROM triage.triaged_alerts GROUP BY triage_status;"  -- expect 6 rows total if triage-agent is running (includes 6b)
psql -U rpe -d rpe -c "SELECT count(*) FROM triage.triaged_alerts WHERE alert_id = '4b7c2b8e-0000-4000-8000-000000000001';"  -- expect 1, not 2 — proves triage's own inbox dedup
```

## Precedence assertion (the one that actually proves evaluation order, not just config order)

```bash
psql -U rpe -d rpe -c "SELECT rule_name FROM alert.processed_alerts WHERE account_id = 'acct-precedence-01';"
# MUST be exactly one row, rule_name = 'velocity'. If it's 'zscore', the detector
# @Order/list-registration precedence is broken — this is a correctness regression,
# not a flaky test.
```

## DLT depth (expect 0 throughout — nothing here should be a poison message)

```bash
for dlt in payment.events.DLT payment.alerts.DLT payment.alerts.triage.DLT; do
  echo "$dlt:"
  rpk topic describe "$dlt" -p
done
```

## Partition / consumer-lag spot check

```bash
rpk group describe rpe-payment-consumer    # KafkaConfig.java builds factory with GROUP_ID_CONFIG="rpe-payment-consumer"
rpk group describe rpe-alert-consumer      # matches rpe-alert-service/application.yml group-id
rpk group describe rpe-triage-agent        # KafkaTriageConfig.CONSUMER_GROUP
```
Confirms events landed across multiple partitions (12 total on `payment.events`) — not a
throughput claim, just confirms the account-keyed producer calls are actually being
partitioned, not silently pinned to one.

## Triaged alert narrative spot check (if triage running)

```bash
rpk topic consume payment.alerts.triaged --offset start --num 5
```

## Note

The actual `docker-compose.yml` maps Redpanda's external listener to **19092**, not 9092. All commands here use `19092`.
Since kafka-console-consumer isn't on the Redpanda container's PATH — use rpk instead.