#!/usr/bin/env bash
# RPE Synthetic E2E v1 — run order matches the agreed matrix:
#   0 preflight -> 1 clean -> 2 velocity -> 3 zscore -> 4 geo -> 5 precedence -> 6a/6b idempotency
#
# BROKER PORT: docker-compose.yml maps redpanda external listener to 19092, not 9092.
set -euo pipefail
cd "$(dirname "$0")"

BROKER="${BROKER:-localhost:19092}"
DET_URL="${DET_URL:-http://localhost:8080}"
RELAY_URL="${RELAY_URL:-http://localhost:8082}"
ALERT_URL="${ALERT_URL:-http://localhost:8083}"
TRIAGE_URL="${TRIAGE_URL:-http://localhost:8081}"

produce() {   # $1 = topic, $2 = file
  docker compose exec -T redpanda rpk topic produce "$1" \
    -f '%k:%v\n' < "$2"
}

section() { echo; echo "=== $1 ==="; }

# ---------------------------------------------------------------------------
# 0. PRE-FLIGHT
# ---------------------------------------------------------------------------
section "0. Pre-flight: liveness/readiness (public, no token)"
for url in "$DET_URL" "$RELAY_URL" "$ALERT_URL" "$TRIAGE_URL"; do
  curl -sf "$url/actuator/health/liveness"  >/dev/null && echo "OK  $url liveness"
  curl -sf "$url/actuator/health/readiness" >/dev/null && echo "OK  $url readiness"
done

section "0. Pre-flight: mint scrape token + protected health (per postman/README.md)"
bash ../deploy/oauth/generate-jwks.sh >/dev/null 2>&1 || true   # no-op if keys already exist
export RPE_SCRAPE_JWT
RPE_SCRAPE_JWT="$(bash ../deploy/oauth/mint-jwt.sh --ttl 3600)"
curl -sf -H "Authorization: Bearer $RPE_SCRAPE_JWT" "$DET_URL/actuator/health" >/dev/null \
  && echo "OK  detection protected health reachable"

section "0. Pre-flight: DLT depth baseline (expect 0 on all three)"
for dlt in payment.events.DLT payment.alerts.DLT payment.alerts.triage.DLT; do
  n=$(docker compose exec -T redpanda rpk topic describe "$dlt" -p 2>/dev/null \
      | awk 'NR > 1 {sum += $6} END {print sum+0}')
  echo "  $dlt high-watermark sum: $n   (expect 0 before this run; non-zero = pre-existing poison, investigate before proceeding)"
done

section "0. Pre-flight: outbox/processed_alerts baseline counts"
echo "Run manually against your DB (see assertions.md for full queries):"
read -r -p "Baseline reviewed — press enter to continue, or Ctrl-C to abort... " _

# ---------------------------------------------------------------------------
# 1. CLEAN BASELINE
# ---------------------------------------------------------------------------
section "1. Clean baseline (4 accounts x 3 events) — expect ZERO alerts, ZERO new DB rows"
produce payment.events events/01_clean.jsonl

# ---------------------------------------------------------------------------
# 2. VELOCITY BOUNDARY
# ---------------------------------------------------------------------------
section "2. Velocity boundary (101 events, 1 account) — expect exactly 1 alert, eventId=velocity-101"
produce payment.events events/02_velocity_boundary.jsonl

# ---------------------------------------------------------------------------
# 3. Z-SCORE
# ---------------------------------------------------------------------------
section "3. Z-score (30 baseline + 1 outlier) — expect exactly 1 alert, eventId=zscore-outlier-01"
produce payment.events events/03_zscore.jsonl

# ---------------------------------------------------------------------------
# 4. GEO — staged produce, real wall-clock delay controls the speed calculation.
#    brokerIngestMs uses ConsumerRecord.timestamp(), not the payload timestamp field,
#    so the delay must be a genuine sleep between producer invocations, not a JSON field edit.
# ---------------------------------------------------------------------------
section "4a. Geo — BELOW threshold (small hop, 5s gap) — expect NO alert"
GEO_BELOW_ACCT="acct-geo-below-01"
echo "${GEO_BELOW_ACCT}:{\"eventId\":\"geo-below-seed\",\"accountId\":\"${GEO_BELOW_ACCT}\",\"amount\":50.00,\"lat\":37.7749,\"lon\":-122.4194,\"timestamp\":\"2026-09-13T12:00:00Z\",\"schemaVersion\":\"1\"}" \
  | docker compose exec -T redpanda rpk topic produce payment.events \
      -f '%k:%v\n'
sleep 5 # ~0.3km from the seed point — 0.3km / (5s/3600) = ~216 km/h, well under the 900 km/h threshold.
echo "${GEO_BELOW_ACCT}:{\"eventId\":\"geo-below-second\",\"accountId\":\"${GEO_BELOW_ACCT}\",\"amount\":50.00,\"lat\":37.7776,\"lon\":-122.4194,\"timestamp\":\"2026-09-13T12:00:05Z\",\"schemaVersion\":\"1\"}" \
  | docker compose exec -T redpanda rpk topic produce payment.events \
      -f '%k:%v\n'

section "4b. Geo — ABOVE threshold (SF -> NYC, 5s gap) — expect exactly 1 alert"
GEO_ABOVE_ACCT="acct-geo-above-01"
echo "${GEO_ABOVE_ACCT}:{\"eventId\":\"geo-above-seed\",\"accountId\":\"${GEO_ABOVE_ACCT}\",\"amount\":50.00,\"lat\":37.7749,\"lon\":-122.4194,\"timestamp\":\"2026-09-13T12:00:00Z\",\"schemaVersion\":\"1\"}" \
  | docker compose exec -T redpanda rpk topic produce payment.events \
      -f '%k:%v\n'
sleep 5 # ~4130km (SF -> NYC) / (5s/3600) ~= 2,973,600 km/h — orders of magnitude over 900 km/h.
echo "${GEO_ABOVE_ACCT}:{\"eventId\":\"geo-above-second\",\"accountId\":\"${GEO_ABOVE_ACCT}\",\"amount\":50.00,\"lat\":40.7128,\"lon\":-74.0060,\"timestamp\":\"2026-09-13T12:00:05Z\",\"schemaVersion\":\"1\"}" \
  | docker compose exec -T redpanda rpk topic produce payment.events \
      -f '%k:%v\n'

# ---------------------------------------------------------------------------
# 5. PRECEDENCE
# ---------------------------------------------------------------------------
section "5. Precedence (100 filler + 1 dual-trigger event) — expect 1 alert, ruleName=velocity (NOT zscore)"
produce payment.events events/05_precedence.jsonl

# ---------------------------------------------------------------------------
# 6a. IDEMPOTENCY — gate-level dedup (Redis SET NX EX 300)
# ---------------------------------------------------------------------------
section "6a. Duplicate payment event (same eventId sent twice, <300s apart) — expect exactly 1 alert"
produce payment.events events/06a_dedup_event.jsonl

# ---------------------------------------------------------------------------
# 6b. IDEMPOTENCY — alert-service-level dedup (processed_alerts ON CONFLICT DO NOTHING).
#     Bypasses detection entirely: publishes the same AlertMessage twice directly to
#     payment.alerts. Proves alert-service's own guard, independent of the gate (this is
#     the guard a real relay crash/redelivery would exercise — see rpe-relay-service/AlertPublisher).
# ---------------------------------------------------------------------------
section "6b. Duplicate alert identity (same alertId sent twice directly to payment.alerts) — expect exactly 1 processed_alerts row"
DUP_ALERT_ID="4b7c2b8e-0000-4000-8000-000000000001"   # fixed UUID for reproducibility
ALERT_JSON="{\"alertId\":\"${DUP_ALERT_ID}\",\"eventId\":\"synthetic-dup-4b-001\",\"accountId\":\"acct-dedup-4b-01\",\"ruleName\":\"velocity\",\"reason\":\"synthetic duplicate-identity test\",\"producedAt\":\"2026-09-13T12:00:00Z\"}"
echo "${DUP_ALERT_ID}:${ALERT_JSON}" | docker compose exec -T redpanda rpk topic produce payment.alerts \
  -f '%k:%v\n'
echo "${DUP_ALERT_ID}:${ALERT_JSON}" | docker compose exec -T redpanda rpk topic produce payment.alerts \
  -f '%k:%v\n'

section "Done producing. Pipeline is async — poll before asserting. See assertions.md."
