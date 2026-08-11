#!/usr/bin/env bash
#
# dlt-redrive.sh — operator-gated, loop-safe re-drive of dead-letter records (ADR-23).
#
# Re-drive is ALWAYS a human decision (extends ADR-10's no-auto-replay): this script is the only
# sanctioned path from a DLT back to its source topic. Nothing in the running services ever does
# this automatically — there is no DLT consumer, no re-drive HTTP endpoint (ADR-19 keeps Actuator
# See operational documentation
#
# Safety properties (why re-drive cannot corrupt state):
#   * Dry-run by DEFAULT. Without --execute it only prints what it WOULD do.
#   * Fail-closed DLT→source allowlist. You can only re-drive a known DLT to its known source.
#   * Loop-safe. Each record carries x-redrive-attempts; at the cap it is PARKED (-> <dlt>.parked),
#     never sent back to source. A genuinely-poison record can never ping-pong DLT<->source.
#   * Idempotency-backed. Re-driving is safe because every source path dedups on replay:
#       payment.events        -> detection Lua dedup window + deterministic UUIDv5 alert_id
#       payment.alerts        -> alert-service processed_alerts ON CONFLICT DO NOTHING
#       payment.alerts(triage)-> triage inbox triaged_alerts ON CONFLICT DO NOTHING (pre-LLM)
#     so a re-driven duplicate is absorbed, not double-actioned.
#   * Forensic trail. The original failure headers Spring Kafka stamped (kafka_dlt-*) are preserved
#     byte-for-byte; a re-drive stamp (x-redrive-attempts, x-redrive-at) is added, never removed.
#
# Requires: rpk (v24.1.x — matches the compose/k8s images) and jq. PLAINTEXT lab broker by default;
# for a SASL stack export the same KAFKA_ADMIN_* vars provision-acls.sh uses. The re-drive operator
# needs READ on the DLT and WRITE on {source, <dlt>.parked} — see the rpe-operator block in
# provision-acls.sh (NOT a per-service principal; re-drive is an operator action).
#
set -euo pipefail

# ── DLT → source allowlist (fail-closed). A DLT not listed here cannot be re-driven. ────────────
declare -A SOURCE_OF=(
  ["payment.events.DLT"]="payment.events"
  ["payment.alerts.DLT"]="payment.alerts"
  ["payment.alerts.triage.DLT"]="payment.alerts"
)

DLT=""
EXECUTE=0
MAX_ATTEMPTS="${REDRIVE_MAX_ATTEMPTS:-3}"
BROKERS="${KAFKA_REDRIVE_BROKERS:-localhost:9092}"
GROUP=""

usage() {
  cat >&2 <<EOF
Usage: $0 --dlt <dlt-topic> [--execute] [--max-attempts N] [--brokers host:port] [--group g]

  --dlt          DLT to drain. One of: ${!SOURCE_OF[*]}
  --execute      Actually move records. WITHOUT this flag the script is a DRY RUN (default).
  --max-attempts Re-drive cap before a record is PARKED (default ${MAX_ATTEMPTS}, env REDRIVE_MAX_ATTEMPTS).
  --brokers      Bootstrap (default ${BROKERS}, env KAFKA_REDRIVE_BROKERS).
  --group        Re-drive consumer group (default rpe-redrive.<dlt> — resumable across runs).

Examples:
  $0 --dlt payment.alerts.triage.DLT                 # dry run: show what would re-drive / park
  $0 --dlt payment.alerts.triage.DLT --execute       # actually re-drive (parking past the cap)
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dlt)          DLT="${2:?}"; shift 2 ;;
    --execute)      EXECUTE=1; shift ;;
    --max-attempts) MAX_ATTEMPTS="${2:?}"; shift 2 ;;
    --brokers)      BROKERS="${2:?}"; shift 2 ;;
    --group)        GROUP="${2:?}"; shift 2 ;;
    -h|--help)      usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

[[ -n "$DLT" ]] || usage
command -v rpk >/dev/null || { echo "FATAL: rpk not found" >&2; exit 3; }
command -v jq  >/dev/null || { echo "FATAL: jq not found"  >&2; exit 3; }

SOURCE="${SOURCE_OF[$DLT]:-}"
if [[ -z "$SOURCE" ]]; then
  echo "FATAL: '$DLT' is not a re-drivable DLT (fail-closed allowlist). Known: ${!SOURCE_OF[*]}" >&2
  exit 4
fi
PARKED="${DLT}.parked"
GROUP="${GROUP:-rpe-redrive.${DLT}}"

RPK=(rpk --brokers "$BROKERS")
# SASL passthrough when an admin principal is exported (same vars as provision-acls.sh).
if [[ -n "${KAFKA_ADMIN_USER:-}" ]]; then
  RPK+=(-X user="$KAFKA_ADMIN_USER" -X pass="${KAFKA_ADMIN_PASSWORD:?}" -X sasl.mechanism=SCRAM-SHA-512)
fi

mode="DRY RUN (no records moved — pass --execute to act)"; [[ $EXECUTE -eq 1 ]] && mode="EXECUTE"
echo "==> dlt-redrive [$mode]"
echo "    dlt=$DLT  source=$SOURCE  parked=$PARKED  max-attempts=$MAX_ATTEMPTS  group=$GROUP"

redriven=0; parked=0; seen=0

# Drain the CURRENT backlog only (-o :end stops at the high-watermark at start; a record that
# re-poisons re-appears on a later run, where its now-higher attempt count routes it to parking).
# --group makes the drain resumable: a re-run resumes from the committed offset, never re-reads.
while IFS= read -r rec; do
  [[ -z "$rec" ]] && continue
  seen=$((seen+1))

  key=$(jq -r '.key // ""'   <<<"$rec")
  val=$(jq -r '.value // ""' <<<"$rec")
  # x-redrive-attempts header (default 0). rpk emits header values as plain strings here.
  attempts=$(jq -r '[.headers[]? | select(.key=="x-redrive-attempts") | .value] | last // "0"' <<<"$rec")
  [[ "$attempts" =~ ^[0-9]+$ ]] || attempts=0
  next=$((attempts+1))

  if (( attempts >= MAX_ATTEMPTS )); then
    dest="$PARKED"; action="PARK"; parked=$((parked+1))
  else
    dest="$SOURCE"; action="REDRIVE"; redriven=$((redriven+1))
  fi
  echo "    [$action] key='${key}' attempts=${attempts} -> ${dest}"

  if (( EXECUTE == 1 )); then
    # Re-emit the ORIGINAL headers explicitly — rpk produce carries nothing over from the
    # consume side, so without this every kafka_dlt-* forensic header AND the x-rpe-* detection
    # outcome headers (which the consumer needs to reconstruct a dedup-blocked alert on
    # re-drive) were silently dropped. Only the x-redrive-* stamps are replaced with fresh
    # values. Limitation: rpk -H splits at the first '=', so a header VALUE containing '='
    # survives, but multi-line/binary header values do not round-trip — acceptable for the
    # string headers Spring Kafka and RPE stamp.
    hdr_args=()
    while IFS=$'\t' read -r hk hv; do
      [[ -z "$hk" ]] && continue
      case "$hk" in x-redrive-attempts|x-redrive-at|x-redrive-origin) continue ;; esac
      hdr_args+=(-H "${hk}=${hv}")
    done < <(jq -r '.headers[]? | [.key, (.value // "")] | @tsv' <<<"$rec")

    "${RPK[@]}" topic produce "$dest" \
      -k "$key" \
      ${hdr_args[@]+"${hdr_args[@]}"} \
      -H "x-redrive-attempts=${next}" \
      -H "x-redrive-at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      -H "x-redrive-origin=${DLT}" \
      <<<"$val" >/dev/null
  fi
done < <("${RPK[@]}" topic consume "$DLT" --group "$GROUP" -o :end --format json 2>/dev/null \
         | jq -c '.' 2>/dev/null || true)

# rpk v24.1.x emits one JSON object per record with --format json; `jq -c '.'` normalises each to a
# single line so the read loop sees exactly one record per iteration. `-o :end` stops at the
# high-watermark captured at start, so a record that re-poisons after re-drive only re-appears on a
# LATER run — where its incremented attempt count routes it to parking, not back to source.

echo "==> done: seen=${seen} redriven=${redriven} parked=${parked} (${mode})"
if (( parked > 0 )); then
  echo "    WARNING: ${parked} record(s) parked on ${PARKED} — these exceeded the re-drive cap."
  echo "    A non-empty parked topic pages (rpe.dlt.depth{topic=${PARKED}} > 0). Decide: fix-forward or discard."
fi
