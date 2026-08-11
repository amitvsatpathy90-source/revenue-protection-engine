#!/usr/bin/env bash
#
# provision-acls.sh — least-privilege per-service Kafka principals + ACLs (ADR-20).
#
# Idempotent: re-runnable. The ACL matrix is derived directly from RPE's one-writer-per-topic
# ownership (ADR-17 §3.4, ADR-18) — each service authenticates as its own SCRAM principal and
# is granted ONLY the topics/groups its bounded context requires. A coding bug that writes the
# wrong topic is then rejected at the broker (TopicAuthorizationException), turning the
# single-writer convention into a runtime guarantee.
#
# Requires an admin principal with cluster ALTER on ACLs. Redpanda (rpk) shown; the kafka-acls.sh
# equivalents are 1:1 (--add --allow-principal User:<svc> --operation <op> --topic <t>).
#
set -euo pipefail

RPK=(rpk --brokers "${KAFKA_ADMIN_BROKERS:?set KAFKA_ADMIN_BROKERS}"
     -X user="${KAFKA_ADMIN_USER:?}" -X pass="${KAFKA_ADMIN_PASSWORD:?}"
     -X sasl.mechanism=SCRAM-SHA-512)

echo "==> Creating SCRAM users (idempotent)"
create_user() { # $1=service  $2=password
  "${RPK[@]}" acl user create "rpe-$1" -p "$2" 2>/dev/null \
    || echo "    user rpe-$1 already exists — skipping"
}
create_user detection "${KAFKA_PW_DETECTION:?}"
create_user relay     "${KAFKA_PW_RELAY:?}"
create_user alert     "${KAFKA_PW_ALERT:?}"
create_user triage    "${KAFKA_PW_TRIAGE:?}"

acl() { "${RPK[@]}" acl create --allow-principal "User:$1" "${@:2}" || true; }

echo "==> rpe-detection: consume payment.events; produce payment.events.DLT"
acl rpe-detection --operation read              --topic payment.events
acl rpe-detection --operation read              --group rpe-payment-consumer
acl rpe-detection --operation write --operation describe --topic payment.events.DLT
# ADR-23 depth-poll: the owner reads its parked quarantine's offsets (DltDepthMetrics) but never
# writes it — DESCRIBE-only. WRITE on *.parked stays exclusively with rpe-operator. Without this
# grant the parked-depth poll is authorization-denied, the gauge freezes at 0 (false-healthy),
# and RpeDltParked can never fire.
acl rpe-detection --operation describe          --topic payment.events.DLT.parked
acl rpe-detection --operation idempotent-write  --cluster

echo "==> rpe-relay: transactional produce payment.alerts (exactly-once)"
acl rpe-relay --operation write --operation describe --topic payment.alerts
# Prefixed: every instance's transactional.id is "${RELAY_INSTANCE_ID}-..." (ADR-06)
acl rpe-relay --operation write --operation describe \
              --transactional-id "${RELAY_INSTANCE_ID:?set RELAY_INSTANCE_ID}-" \
              --resource-pattern-type prefixed
acl rpe-relay --operation idempotent-write --cluster

echo "==> rpe-alert: consume payment.alerts; produce payment.alerts.DLT"
acl rpe-alert --operation read              --group rpe-alert-consumer
acl rpe-alert --operation read              --topic payment.alerts
acl rpe-alert --operation write --operation describe --topic payment.alerts.DLT
# ADR-23 depth-poll: DESCRIBE-only on the owned parked quarantine (see rpe-detection note above).
acl rpe-alert --operation describe          --topic payment.alerts.DLT.parked

echo "==> rpe-triage: consume payment.alerts; produce triaged + triage.DLT"
acl rpe-triage --operation read              --group rpe-triage-agent
acl rpe-triage --operation read              --topic payment.alerts
acl rpe-triage --operation write --operation describe --topic payment.alerts.triaged
acl rpe-triage --operation write --operation describe --topic payment.alerts.triage.DLT
# ADR-23 depth-poll: DESCRIBE-only on the owned parked quarantine (see rpe-detection note above).
acl rpe-triage --operation describe          --topic payment.alerts.triage.DLT.parked

echo "==> rpe-operator: DLT re-drive (ADR-23) — READ each DLT; WRITE source + parking topics"
# Re-drive is an OPERATOR action (dlt-redrive.sh), never a service. This principal is least-privilege
# for that one task: read the DLTs, write back to the sources and to the parking topics. It is NOT a
# per-service principal (it crosses topic ownership by design, under human control). Provision it
# only on hosts/role accounts that run dlt-redrive.sh. Omit to disable re-drive entirely (fail-safe).
if [[ -n "${KAFKA_PW_OPERATOR:-}" ]]; then
  create_user operator "${KAFKA_PW_OPERATOR}"
  acl rpe-operator --operation read  --group rpe-redrive --resource-pattern-type prefixed
  for dlt in payment.events.DLT payment.alerts.DLT payment.alerts.triage.DLT; do
    acl rpe-operator --operation read  --operation describe --topic "$dlt"
    acl rpe-operator --operation write --operation describe --topic "${dlt}.parked"
  done
  acl rpe-operator --operation write --operation describe --topic payment.events
  acl rpe-operator --operation write --operation describe --topic payment.alerts
  acl rpe-operator --operation idempotent-write --cluster
else
  echo "    KAFKA_PW_OPERATOR unset — re-drive principal not provisioned (re-drive disabled)."
fi

echo "==> ACLs provisioned. Verify: ${RPK[*]} acl list"
