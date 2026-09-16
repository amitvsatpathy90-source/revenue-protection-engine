# RPE Synthetic E2E v1

Matrix agreed: pre-flight -> detector contract (incl. precedence) -> bounded pipeline volume
-> idempotency (split 4a gate-level / 4b alert-service-level) -> assertions. CF/CF-Infra
failure-injection scenarios are explicitly out of scope here — this proves RPE sound on its
own before CF injects failure into it.

## Prerequisites

```bash
docker compose up -d          # infra: Redpanda, Redis, Postgres, jwks-stub
# each service, separate terminals (ports are per configuration — not sequential):
mvn -f rpe-detection-service/pom.xml spring-boot:run -Dspring-boot.run.profiles=dev   # :8080
mvn -f rpe-relay-service/pom.xml spring-boot:run                                       # :8082
mvn -f rpe-alert-service/pom.xml spring-boot:run                                       # :8083
mvn -f rpe-triage-agent/pom.xml spring-boot:run                                         # :8081 (optional — RAG stays CB-gated off, no OpenAI key needed)
```

## Files

| File | Purpose |
|---|---|
| `generate_fixtures.py` | Deterministic (seeded) generator for all JSONL cohorts — re-run if you change account counts/amounts/coordinates |
| `events/*.jsonl` | Generated fixtures, `{accountId}:{PaymentEvent JSON}` per line |
| `run_e2e.sh` | Orchestrates the full run in order: pre-flight -> cohorts 1-6 |
| `assertions.md` | Expected counts, DB queries, DLT/lag checks, polling guidance |

## Run

```bash
cd synthetic-e2e
python3 generate_fixtures.py   # re-run only if you edit the generator
./run_e2e.sh
# then work through assertions.md
```

## What's deliberately NOT covered here

- **RAG retrieval quality** — still blocked on a real `SPRING_AI_OPENAI_API_KEY`. This run
  proves the core pipeline is sound with RAG cleanly no-op'd (`rag_context_used=false`), not
  that retrieval itself works.
- **Relay crash / broker disruption / Redis or Postgres failure** — CF/CF-Infra layer, run
  against this now-proven-sound baseline, not folded in here.
- **Throughput/latency numbers** — this is a correctness run on an unspecified local rig; no
  benchmark figures should be quoted from it.
