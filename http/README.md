# RPE Actuator Endpoints — Complete Testing & Authorization Guide

All four RPE services protect the actuator surface (metrics, health, env) with OAuth2 Resource Server validation. This guide covers everything: setup, bearer token generation, testing with Postman/IntelliJ, and troubleshooting.

---

## Quick Start (5 minutes)

Get a token and test an endpoint **right now**:

```bash
# 1. Generate RSA key material (one-time)
bash deploy/oauth/generate-jwks.sh

# 2. Mint a 30-day test token
TOKEN=$(bash deploy/oauth/mint-jwt.sh --ttl 2592000)
echo "$TOKEN"

# 3. Test it works (paste your token)
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/actuator/prometheus | head -5
```

**Done.** Token is valid for 30 days. Copy it to Postman or IntelliJ below.

---

## Table of Contents

1. [Files in This Folder](#files-in-this-folder)
2. [Service Topology](#service-topology)
3. [HTTP Surface: Public vs. Protected](#http-surface-public-vs-protected)
4. [Bearer Token: Generation & Setup](#bearer-token-generation--setup)
   - [Step 1: Generate OAuth Keys](#step-1-generate-oauth-key-material)
   - [Step 2: Mint a Token](#step-2-mint-a-test-token)
   - [Step 3: Configure Postman](#step-3-set-token-in-postman)
   - [Step 4: Verify Token Works](#step-5-verify-token-works)
5. [Testing Endpoints](#testing-endpoints)
   - [Startup Checklist](#startup-checklist)
6. [Token Management](#token-management)
   - [Expiry & Refresh](#token-expiry--refresh)
   - [Token Claims Reference](#token-claims-reference)
   - [Generation Options](#reference-full-token-generation-options)
7. [Troubleshooting](#troubleshooting)
8. [Notes](#notes)

---

## Files in This Folder

| File | Purpose |
|------|---------|
| `rpe-actuator.http` | Runnable REST Client requests for supported IDEs (IntelliJ, VS Code) |
| `rpe-actuator.postman_collection.json` | Postman collection for import (same endpoints) |
| `README.md` | This file — everything you need |

---

## Service Topology

Four independently deployable services, each with an OAuth2-protected actuator surface:

| Service | Port | Startup Command |
|---|---|---|
| **detection** | 8080 | `mvn -f rpe-detection-service/pom.xml spring-boot:run -Dspring-boot.run.profiles=dev` |
| **relay** | 8081 | `mvn -f rpe-relay-service/pom.xml spring-boot:run` |
| **alert** | 8082 | `mvn -f rpe-alert-service/pom.xml spring-boot:run` |
| **triage** | 8083 | `mvn -f rpe-triage-agent/pom.xml spring-boot:run` |

---

## HTTP Surface: Public vs. Protected

### Public Endpoints (No Token Required)

These are for k8s probes and are always accessible:

```bash
GET /actuator/health/liveness  # → 200 OK (service is alive)
GET /actuator/health/readiness # → 200 OK (service is ready)
```

### Protected Endpoints (OAuth2 Bearer Token Required)

All other actuator endpoints require `Authorization: Bearer <JWT>` header with `metrics:scrape` scope:

```bash
GET /actuator/health       # → component health detail (requires token)
GET /actuator/info         # → build/app info (requires token)
GET /actuator/prometheus   # → Prometheus metrics (requires token)
```

**Missing or invalid token** → `401 Unauthorized`

---

## Bearer Token: Generation & Setup

### Step 1: Generate OAuth Key Material

**What**: Create the RSA private key and public JWK Set.
**When**: One-time at the start of your dev session.
**Where**: `deploy/oauth/` directory.

```bash
bash deploy/oauth/generate-jwks.sh
```

Expected output:
```
Generating RSA key pair...
  keys/private.pem ← private key (for signing)
  public/jwks.json ← public JWK Set (for verification)
Generated 1 key pair.
```

Verify:
```bash
ls -la deploy/oauth/keys/private.pem deploy/oauth/public/jwks.json
```

---

### Step 2: Mint a Test Token

**What**: Generate a time-limited JWT signed by your private key.
**Scope**: Automatically includes `metrics:scrape` (required for `/actuator/prometheus`).
**TTL**: Default 1 hour; for local testing use 30 days.

```bash
TOKEN=$(bash deploy/oauth/mint-jwt.sh --ttl 2592000)
echo "$TOKEN"
```

**Example output** (your token will differ):
```
eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImZlN2I4ZDNjLWJhZmItNGVlNy1hMGI5LWU0Mjg5YTgzMzQ2NyJ9.eyJpc3MiOiJodHRwOi8vcnBlLWp3a3Mtc3R1YiIsInN1YiI6InJwZS1sYWItb3BlcmF0b3IiLCJhdWQiOiJycGUtYWN0dWF0b3IiLCJzY29wZSI6Im1ldHJpY3M6c2NyYXBlIiwiaWF0IjoxNzIyNzQ4NzI5LCJleHAiOjE3NTQyODQ3Mjl9.vJbhR_iNn2t5WxYPLfPqvJc4...truncated
```

**Decode to verify** (optional):
```bash
# Decode payload (second base64 part)
echo "$TOKEN" | cut -d. -f2 | base64 -d | python3 -m json.tool
```

You should see:
```json
{
  "iss": "http://rpe-jwks-stub",
  "aud": "rpe-actuator",
  "scope": "metrics:scrape",
  "iat": 1722748729,
  "exp": 1754284729
}
```

**Troubleshooting**:
- `keys/private.pem missing` → Run `bash deploy/oauth/generate-jwks.sh`
- `public/jwks.json missing` → Run `bash deploy/oauth/generate-jwks.sh`

---

### Step 3: Set Token in Postman

#### Option A: Collection Variable (Recommended)

1. **Open the collection** in Postman:
   - File → Import → Select `http/rpe-actuator.postman_collection.json`
   - Or drag-and-drop into Postman

2. **Locate collection variables**:
   - In the collection tree (left), click the collection name
   - Go to "Variables" tab

3. **Set the token**:
   - Find variable `RPE_SCRAPE_JWT`
   - Paste your token into "CURRENT VALUE"
   - Click "Save"

4. **Verify**:
   - Open any request → "Auth" tab
   - Should show: `Bearer {{RPE_SCRAPE_JWT}}`

#### Option B: Environment Variable

1. **Create environment**:
   - Settings → Environments → Create
   - Name: `RPE Local`

2. **Add variable**:
   - Variable: `RPE_SCRAPE_JWT`
   - Value: Paste your token

3. **Activate**:
   - Top-right dropdown → Select `RPE Local`

### Option C: Compatible IDEs (`.http` file)

For IDEs supporting `.http` files (IntelliJ IDEA, VS Code with REST Client extension, etc.):

1. Open `http/rpe-actuator.http`.
2. Ensure `RPE_SCRAPE_JWT` is populated.
3. Click the run icon next to any request in the file.

---

### Step 4: Verify Token Works

#### Test 1: Public Endpoint (No Token)

```bash
curl -v http://localhost:8080/actuator/health/liveness
```

Expected: `200 OK`

#### Test 2: Protected Endpoint (Token Required)

```bash
curl -v \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/actuator/prometheus | head -20
```

Expected: `200 OK` + Prometheus metrics starting with `# HELP`

#### Test 3: Invalid/Missing Token

```bash
# No token
curl -v http://localhost:8080/actuator/prometheus

# Wrong token
curl -v \
  -H "Authorization: Bearer invalid_token" \
  http://localhost:8080/actuator/prometheus
```

Expected: `401 Unauthorized`

---

## Testing Endpoints

### Startup Checklist

Before running endpoints, start everything in order:

1. **Infra** (Kafka, Redis, Postgres, JWKS stub):
   ```bash
   docker compose up -d
   ```

2. **Detection service** (with dev mode):
   ```bash
   env $(grep -v '^#' .env | xargs) mvn -f rpe-detection-service/pom.xml spring-boot:run \
     -Dspring-boot.run.profiles=dev \
     -Dspring-boot.run.arguments="--rpe.dev.blockhound.enabled=true --rpe.dev.reactor-debug-agent=true" \
     -Dspring-boot.run.jvmArguments="-XX:+AllowRedefinitionToAddDeleteMethods -Djdk.tracePinnedThreads=full"
   ```

3. **Relay service** (new terminal):
   ```bash
   env $(grep -v '^#' .env | xargs) mvn -f rpe-relay-service/pom.xml spring-boot:run
   ```

4. **Alert service** (new terminal):
   ```bash
   env $(grep -v '^#' .env | xargs) mvn -f rpe-alert-service/pom.xml spring-boot:run
   ```

5. **Triage service** (new terminal, optional):
   ```bash
   env $(grep -v '^#' .env | xargs) mvn -f rpe-triage-agent/pom.xml spring-boot:run
   ```

6. **Generate token** (new terminal):
   ```bash
   bash deploy/oauth/generate-jwks.sh
   export RPE_SCRAPE_JWT=$(bash deploy/oauth/mint-jwt.sh --ttl 2592000)
   echo "$RPE_SCRAPE_JWT"
   ```

7. **Test endpoints**:
   - **Postman**: Import collection, set variable, send requests
   - **IntelliJ**: Open `.http` file, set `@bearerToken`, run requests
   - **cURL**: Use sample commands above

---

## Token Management

### Token Expiry & Refresh

**TTL**: 30 days (when minted with `--ttl 2592000`)

**When expired**: Requests return `401 Unauthorized` + "Jwt expired" message

**Refresh**:
```bash
TOKEN=$(bash deploy/oauth/mint-jwt.sh --ttl 2592000)
echo "$TOKEN"

# Update your test tool:
#   Postman → collection variable RPE_SCRAPE_JWT
#   IntelliJ → @bearerToken in .http file or .env
```

---

### Token Claims Reference

Every JWT minted by `deploy/oauth/mint-jwt.sh` contains:

```json
{
  "iss": "http://rpe-jwks-stub",        ← Issuer (must match RPE_OAUTH_ISSUER in .env)
  "aud": "rpe-actuator",                ← Audience (must match RPE_OAUTH_AUDIENCE in .env)
  "sub": "rpe-lab-operator",            ← Subject
  "scope": "metrics:scrape",            ← Required for /actuator/prometheus
  "iat": 1722748729,                    ← Issued At (seconds since epoch)
  "exp": 1754284729                     ← Expiration (issued + TTL)
}
```

**Signature**: RS256 using `deploy/oauth/keys/private.pem` (private key)
**Verification**: Each service validates against `deploy/oauth/public/jwks.json` (public key set)

---

### Reference: Full Token Generation Options

```bash
# Defaults (1 hour Prometheus scrape token)
bash deploy/oauth/mint-jwt.sh

# 30-day token (recommended for local testing)
bash deploy/oauth/mint-jwt.sh --ttl 2592000

# Custom issuer/audience (rare; ensure env vars match)
bash deploy/oauth/mint-jwt.sh --iss "http://my-idp" --aud "my-audience"

# Custom scope (other RPE scopes not yet defined)
bash deploy/oauth/mint-jwt.sh --scope "admin:read"

# Combined options
bash deploy/oauth/mint-jwt.sh --ttl 2592000 --sub "test-operator"

# Help
bash deploy/oauth/mint-jwt.sh --help
```

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `keys/private.pem missing` | Key material not generated | Run `bash deploy/oauth/generate-jwks.sh` |
| `public/jwks.json missing` | Key material not generated | Run `bash deploy/oauth/generate-jwks.sh` |
| `401 Unauthorized` on protected endpoint | Missing or invalid bearer token | Check `Authorization: Bearer <token>` header in request; mint new token if expired |
| `401` + "Jwt expired" | Token TTL exceeded | Mint new token: `TOKEN=$(bash deploy/oauth/mint-jwt.sh --ttl 2592000)` |
| `401` + "Invalid audience" | Audience mismatch | Check `.env` `RPE_OAUTH_AUDIENCE` matches "rpe-actuator" (mint-jwt.sh default) |
| `401` + "Invalid issuer" | Issuer mismatch | Check `.env` `RPE_OAUTH_ISSUER` matches "http://rpe-jwks-stub" (mint-jwt.sh default) |
| `401` + "Invalid signature" | JWKS endpoint unreachable or keys don't match | Ensure `docker compose up -d` has `jwks-stub` running; regenerate keys with `generate-jwks.sh` |
| `Connection refused` on port 808X | Service not running | Start service: `mvn -f <svc>/pom.xml spring-boot:run` |
| `502 Bad Gateway` | Service crashed or slow startup | Check service logs; restart if needed |
| `503 Service Unavailable` | Health check reports degraded state | Use `GET /actuator/health` (with token) to see which components are down |
| Postman variable `{{RPE_SCRAPE_JWT}}` empty | Variable not saved | Click "Save" after pasting token; restart Postman if needed |
| IntelliJ shows `{{RPE_SCRAPE_JWT}}` as literal | Variable not resolved | Ensure `.env` file exists and is loaded; or replace literal in `.http` file |

---

## Notes

- **No business REST API**: This repo is Kafka-driven; HTTP surface is actuator-only
- **Lab only**: JWKS stub and test tokens are for dev/lab; production uses a real IdP
- **Composable**: Services can scale horizontally; actuator surface stays the same
- **OAuth2 Resource Server**: All actuator endpoints (except public probes) validate signature + issuer + audience + scope
- **Token reuse**: Once minted, a token is valid for its TTL duration (30 days recommended); no need to regenerate on each request

