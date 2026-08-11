# rpe-triage-agent

Advisory AI triage downstream of `payment.alerts` — **ADR-15**. Consumes alerts on its
own `read_committed` group, runs a Resilience4j-bounded Spring AI tool-calling loop
(max 3 rounds, enforced in code), and produces `{severity, narrative, evidence[],
confidence, triage_status}` to `payment.alerts.triaged`.

**The LLM never gates detection, alert identity, or delivery.** Kill the LLM endpoint
and verdicts keep flowing as `DEGRADED_RULE_BASED` with static rule→severity mapping.

Binding docs: `../docs/adrs/ADR-15.md` (rationale) + `../docs/adrs/ADR-29.md`
(enforcement detail). Conflicts: root System Invariants win.

## Build & run

Standalone module by design — **not** aggregated by the core pom (module deletion =
full rollback; the core build never sees Spring AI):

```bash
mvn -f rpe-triage-agent/pom.xml verify          # all provider interaction stubbed
mvn -f rpe-triage-agent/pom.xml spring-boot:run # needs DB_PASSWORD (+ optional SPRING_AI_OPENAI_API_KEY)
docker compose --profile triage up -d            # containerised, from repo root
```

Without `SPRING_AI_OPENAI_API_KEY` the module starts normally and every verdict is
`DEGRADED_RULE_BASED` — that is the designed degraded mode, not an error.

## Key signals (port 8081)

| Metric | Meaning |
|---|---|
| `triage_llm_latency_seconds{provider,outcome}` | Per-call LLM latency; outcome ∈ success/timeout/circuit_open/bulkhead_full/rate_limited/error |
| `triage_tool_rounds` | Agent loop rounds; saturation at 3 with low confidence = assumption #3 failing (ADR-15 §8) |
| `triage_verdicts_total{triage_status}` | LLM_TRIAGED vs DEGRADED_RULE_BASED vs PUBLISH_FAILED — alert on fallback rate-of-change |
| `resilience4j_circuitbreaker_state{name="llm"}` | The §6.1 demo signal: slow provider ⇒ open ⇒ degraded verdicts still flow |
| `triage_consumer_lag{group}` | Rising lag + open CB = provider degradation pair |
| `gen_ai_client_token_usage` | Cost signal (Spring AI observation) |
| `triage_tool_account_mismatch{tool}` | Tool asked for a non-alert account — live injection-attempt signal |

```bash
curl -s http://localhost:8081/actuator/prometheus | grep -E "resilience4j_circuitbreaker_state|gen_ai"
psql "$DB_URL" -c "SELECT triage_status, count(*) FROM triaged_alerts GROUP BY triage_status;"
kafka-console-consumer --bootstrap-server localhost:19092 --topic payment.alerts.triaged --from-beginning
```

## Non-negotiables enforced here

- Inbox `INSERT … ON CONFLICT DO NOTHING` on UUIDv5 `alert_id` runs **before** the LLM
  call — redelivery is never duplicate spend (replay test pins this).
- Round budget is a `for` loop, not a prompt instruction.
- Tools are read-only and account-scoped via `ToolContext` (model-supplied account ids
  are verified; mismatch = bounded error + counter).
- Evidence items must reference tool-call IDs from this run; fabricated evidence is
  stripped, confidence downgraded, `evidence_stripped=true`.
- `DegradedTriageFallback` has zero Spring AI imports — it must survive the dependency
  being deleted.
- Stale `PENDING_TRIAGE` rows (crash window) get degraded verdicts from the sweep;
  the LLM is never re-called for them.
- Prompts/completions never appear in logs or span attributes. Token counts only.
