#!/usr/bin/env bash
# RPE synthetic E2E v1 - T0 assertions. Run after run_e2e.sh (NON_INTERACTIVE=1).
set -uo pipefail

TIMEOUT="${TIMEOUT:-60}"   # seconds to poll each positive assertion
SETTLE="${SETTLE:-15}"     # grace before negative / stability checks
TRIAGE="${TRIAGE:-0}"      # 1 = rpe-triage-agent is running
FRESH="${FRESH:-0}"        # 1 = DB and Redis were reset; also assert totals
DUP_ALERT_ID="4b7c2b8e-0000-4000-8000-000000000001"
A="alert.processed_alerts"; O="detection.outbox"; T="triage.triaged_alerts"
PASS=0; FAIL=0

psql_q() { docker compose exec -T postgres psql -U rpe -d rpe -Atc "$1" | tr -d '[:space:]'; }
dlt_sum() {
  local output

  output="$(docker compose exec -T redpanda \
    rpk topic describe "$1" -p)" || return 1

  awk '
    $1 == "PARTITION" { header=1; next }

    header && $1 ~ /^[0-9]+$/ {
      if ($5 !~ /^[0-9]+$/ || $6 !~ /^[0-9]+$/ || $6 < $5) {
        invalid=1
        next
      }

      found=1
      total += ($6 - $5)
    }

    END {
      if (!header || !found || invalid) exit 2
      print total + 0
    }
  ' <<< "$output"
}

record() {  # id desc rc detail
  if [[ "$3" == "0" ]]; then PASS=$((PASS+1)); printf 'PASS %-4s %s\n' "$1" "$2"
  else FAIL=$((FAIL+1)); printf 'FAIL %-4s %s (%s)\n' "$1" "$2" "$4"; fi
}

poll_eq() {  # sql expected -> echoes last value, rc 0 on match
  local deadline=$((SECONDS + TIMEOUT)) got=""
  while :; do
    got="$(psql_q "$1" || true)"
    if [[ "$got" == "$2" ]]; then echo "$got"; return 0; fi
    if (( SECONDS >= deadline )); then echo "$got"; return 1; fi
    sleep 2
  done
}

expect() {  # id desc sql expected
  local got rc
  got="$(poll_eq "$3" "$4")"; rc=$?
  record "$1" "$2" "$rc" "expected $4, got ${got:-<empty>}"
}

once() {  # id desc sql expected - single query, no polling
  local got rc=1
  got="$(psql_q "$3" || true)"
  [[ "$got" == "$4" ]] && rc=0
  record "$1" "$2" "$rc" "expected $4, got ${got:-<empty>}"
}

rule_of() { echo "SELECT count(*)||':'||coalesce(max(rule_name),'') FROM $A WHERE account_id='$1'"; }

main() {
  command -v docker >/dev/null || { echo "docker not found"; exit 2; }
  echo "== T0 (TIMEOUT=${TIMEOUT}s SETTLE=${SETTLE}s TRIAGE=${TRIAGE} FRESH=${FRESH})"

  expect A2  "velocity: 1 alert, rule=velocity"   "$(rule_of acct-velocity-01)"   "1:velocity"
  expect A3  "zscore: 1 alert, rule=zscore"       "$(rule_of acct-zscore-01)"      "1:zscore"
  expect A4b "geo above: 1 alert, rule=geo"       "$(rule_of acct-geo-above-01)"   "1:geo"
  expect A5  "precedence: 1 alert, rule=velocity" "$(rule_of acct-precedence-01)"  "1:velocity"
  expect A6a "gate dedup: 1 alert for dup event"  "SELECT count(*) FROM $A WHERE account_id='acct-dedup-4a-01'" "1"
  expect A6b "alert dedup: 1 row for dup alertId" "SELECT count(*) FROM $A WHERE alert_id='$DUP_ALERT_ID'" "1"
  expect A7  "outbox: 5 detection-originated rows" \
    "SELECT count(*) FROM $O WHERE account_id IN ('acct-velocity-01','acct-zscore-01','acct-geo-above-01','acct-precedence-01','acct-dedup-4a-01')" "5"

  if [[ "$TRIAGE" == "1" ]]; then
    expect A8a "triage inbox dedup: 1 row for dup alertId" "SELECT count(*) FROM $T WHERE alert_id='$DUP_ALERT_ID'" "1"
    expect A8b "triage: 6 distinct alerts"  "SELECT count(*) FROM $T" "6"
  fi
  if [[ "$FRESH" == "1" ]]; then
    expect A9 "totals: 6 processed_alerts" "SELECT count(*) FROM $A" "6"
  fi

  sleep "$SETTLE"
  once A1  "clean: no alerts"          "SELECT count(*) FROM $A WHERE account_id LIKE 'acct-clean-%'" "0"
  once A1o "clean: no outbox rows"     "SELECT count(*) FROM $O WHERE account_id LIKE 'acct-clean-%'" "0"
  once A4a "geo below: no alert"       "SELECT count(*) FROM $A WHERE account_id='acct-geo-below-01'" "0"
  once A4o "geo below: no outbox row"  "SELECT count(*) FROM $O WHERE account_id='acct-geo-below-01'" "0"
  [[ "$FRESH" == "1" ]] && once A9s "totals stable after settle: 6" "SELECT count(*) FROM $A" "6"

  local d s rc
  for d in payment.events.DLT payment.alerts.DLT payment.alerts.triage.DLT; do
    s="$(dlt_sum "$d")"; rc=1; [[ "$s" == "0" ]] && rc=0
    record "DLT" "$d empty" "$rc" "expected 0, got ${s:-<empty>}"
  done

  echo "== T0 RESULT: PASS=$PASS FAIL=$FAIL"
  (( FAIL == 0 ))
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main; fi
