#!/usr/bin/env bash
# Lab IdP stub — key material + JWKS for the `jwks-stub` compose service (ADR-19 fail-closed
# Actuator auth). This is the "static JWKS" option ADR-19 names for lab use; prod points
# RPE_OAUTH_* at a real IdP and this directory is never used.
#
# The script is versioned; everything it writes is git-ignored and regenerable on demand.
# NEVER commit the private key.
#
#   keys/private.pem   RS256 signing key — read only by mint-jwt.sh; never mounted, never served
#   keys/public.pem    verification convenience (openssl dgst -verify)
#   public/jwks.json   the JWK Set nginx serves at /.well-known/jwks.json (public by definition —
#                      public/ is the ONLY path the container mounts; keys/ must stay unmounted)
#
# Every service's decoder (NimbusReactiveJwtDecoder / NimbusJwtDecoder, per-service
# ManagementSecurityConfig) matches tokens by kid; mint-jwt.sh reads the kid back from
# public/jwks.json rather than duplicating the constant.
set -euo pipefail
cd "$(dirname "$0")"

KID="${JWKS_KID:-rpe-lab-1}"

mkdir -p keys public
umask 077   # private key must land 0600; public/jwks.json is chmod'd back to world-readable below
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out keys/private.pem 2>/dev/null
openssl pkey -in keys/private.pem -pubout -out keys/public.pem

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

# JWK n = unsigned big-endian modulus, base64url (openssl prints it hex with no leading 00);
# e = 65537 = "AQAB", openssl's default public exponent.
N=$(openssl rsa -pubin -in keys/public.pem -modulus -noout | cut -d= -f2 | xxd -r -p | b64url)

cat > public/jwks.json <<EOF
{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":"${KID}","n":"${N}","e":"AQAB"}]}
EOF
chmod 644 public/jwks.json   # nginx worker (non-root) must be able to read it despite umask 077

echo "wrote keys/private.pem keys/public.pem public/jwks.json (kid=${KID})"
echo "serve it:      docker compose up -d jwks-stub   (root compose, host :9001)"
echo "               docker compose --env-file .env -f deploy/docker-compose.services.yml up -d jwks-stub"
echo "scrape token:  ./mint-jwt.sh --ttl 2592000   # 30d; put it in .env as RPE_PROM_SCRAPE_TOKEN"
