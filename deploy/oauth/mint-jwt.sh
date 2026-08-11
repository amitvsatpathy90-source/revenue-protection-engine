#!/usr/bin/env bash
# Mint an RS256 lab JWT signed by keys/private.pem (run generate-jwks.sh first).
#
# What RPE actually validates (per-service ManagementSecurityConfig, ADR-19): signature against
# the JWKS + issuer EXACT-STRING match + audience + exp/nbf. Authorization additionally needs
# SCOPE_metrics:scrape on /actuator/prometheus — Spring's default converter derives that from the
# space-delimited `scope` claim. So iss/aud here MUST match RPE_OAUTH_ISSUER / RPE_OAUTH_AUDIENCE
# in the service environment, or the token is rejected.
#
# usage:
#   mint-jwt.sh [--scope <s>] [--aud <s>] [--iss <s>] [--sub <name>] [--ttl <seconds>]
# examples:
#   RPE_PROM_SCRAPE_TOKEN=$(deploy/oauth/mint-jwt.sh --ttl 2592000)   # 30d Prometheus scrape token
#   TOKEN=$(deploy/oauth/mint-jwt.sh)                                  # 1h operator/curl token
set -euo pipefail
cd "$(dirname "$0")"

SCOPE="metrics:scrape"
AUD="rpe-actuator"                # RPE_OAUTH_AUDIENCE default (application.yml, all four services)
ISS="http://rpe-jwks-stub"        # opaque constant — must equal RPE_OAUTH_ISSUER (.env.example)
SUB="rpe-lab-operator"
TTL=3600

usage() { sed -n '3,15p' "$0"; exit 0; }

while [ $# -gt 0 ]; do
  case "$1" in
    --scope) SCOPE="$2"; shift 2 ;;
    --aud)   AUD="$2";   shift 2 ;;
    --iss)   ISS="$2";   shift 2 ;;
    --sub)   SUB="$2";   shift 2 ;;
    --ttl)   TTL="$2";   shift 2 ;;
    --help|-h) usage ;;
    *) echo "unknown arg: $1 (try --help)" >&2; exit 2 ;;
  esac
done

[ -f keys/private.pem ] || { echo "keys/private.pem missing — run generate-jwks.sh first" >&2; exit 1; }
[ -f public/jwks.json ] || { echo "public/jwks.json missing — run generate-jwks.sh first" >&2; exit 1; }

KID=$(sed -E 's/.*"kid":"([^"]+)".*/\1/' public/jwks.json)

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

NOW=$(date +%s)
HEADER=$(printf '{"alg":"RS256","typ":"JWT","kid":"%s"}' "$KID" | b64url)
PAYLOAD=$(printf '{"iss":"%s","sub":"%s","aud":"%s","scope":"%s","iat":%d,"exp":%d}' \
  "$ISS" "$SUB" "$AUD" "$SCOPE" "$NOW" $((NOW + TTL)) | b64url)
SIG=$(printf '%s.%s' "$HEADER" "$PAYLOAD" | openssl dgst -sha256 -sign keys/private.pem -binary | b64url)

printf '%s.%s.%s\n' "$HEADER" "$PAYLOAD" "$SIG"
