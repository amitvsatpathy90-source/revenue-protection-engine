---
asset_id: adr-31-ollama-local-dev-provider
asset_path: docs/adrs/ADR-31.md
asset_type: adr
version: 1.0.0
created: 2026-11-21
last_updated: 2026-11-21
status: ACCEPTED
reversal_cost: LOW
owner: amit
tags: [ triage, ollama, local-provider, tool-calling, spring-ai, dev-only, lab-measured ]
---

# ADR-31 — Ollama as a dev-only chat provider for triage; verdict call without tools

## Status

`ACCEPTED` — scope: local development and lab measurement only. No production traffic.

Partially supersedes: ADR-15 §3.10 (local-model fallback "out of scope"), for a dev-only profile.
Amends: ADR-30's Ollama scope note (Ollama is now also a dev-only chat provider; embedding scope unchanged).
Related: ADR-15 §3.3 (code-enforced tool-round loop), ADR-29.

## Context

Local correctness testing of `rpe-triage-agent` needs a provider with no API spend. Ollama serves an
OpenAI-compatible endpoint at `/v1`. ADR-15 §3.10 rules out a local-model *fallback*; this ADR adds a dev profile, not
a fallback. On `llama3.2:3b`, a replay of one captured conversation showed the model emitting another tool call instead
of the verdict after a tool result.

## Decision

1. Add an `ollama-local` profile that points the OpenAI client at Ollama `/v1`. It does not use the native Ollama
   starter. `triage.llm.provider-tag` is `ollama`; OTLP metrics export is disabled in this profile.
2. After the last permitted tool round, the verdict call carries no tool definitions (`TriageAgent`).
3. `triage.llm.max-tool-rounds` (default 3, valid 1..3, startup-validated, ceiling `MAX_TOOL_ROUNDS`) sets the budget;
   `ollama-local` sets 1.
4. OpenAI remains the intended provider. Ollama is not a production fallback.
5. `TriageVerdict.validated()` is unchanged. On failure, the violated rule (field name only) is logged at WARN.

## Alternatives Considered

| Option                                             | Outcome                                                                                                                                                                                                                            |
|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Native Ollama starter                              | Not tested. Compare on a separate branch before any support claim.                                                                                                                                                                 |
| `response_format: json_schema` on the verdict call | Accepted by Ollama 0.40.16 without an HTTP error (probe C), but indistinguishable from tools-off (probe B). Whether it is enforced is not established; the official Ollama docs snippet does not confirm it on `/v1`. Not adopted. |
| Flatten tool history into plain text (probe F)     | 0/20. The model imitated the text format. Not adopted.                                                                                                                                                                             |
| Relax `validated()` to raise the pass rate         | Rejected.                                                                                                                                                                                                                          |
| Prompt wording for a non-empty narrative           | Untested. Candidate follow-up; it changes the prompt for every provider.                                                                                                                                                           |

## Consequences

- The integration works end to end. `llama3.2:3b` does not reliably meet the output contract; the degraded fallback
  absorbs the failures by design.
- `ollama-local` allows one tool round, so the model cannot chain a second tool after the first.
- On the default profile the fourth call, if reached, also loses tools. Not verified on OpenAI.
- `LLM_TRIAGED` on this model means contract-valid, not grounded: every observed `LLM_TRIAGED` alert had its evidence
  stripped by `EvidenceValidator`.

## Evaluation Findings (lab-only: llama3.2:3b, Ollama 0.40.16, M1 8GB)

| Evidence                                                                            | Result                                                                                                                                                                  |
|-------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Replay (`scripts/triage-ollama-replay`), one captured `velocity` conversation, n=20 | A-R1 20/20; A-R2 0/20 (verdict emitted as a tool call); B 20/20; C 20/20; E-R1 20/20; F 0/20.                                                                           |
| Application smoke, 1 alert                                                          | `LLM_TRIAGED`, `rounds=1`, EVIDENCE_STRIPPED.                                                                                                                           |
| Application batch, 20 alerts, mixed rules                                           | 8/20 `LLM_TRIAGED`; all 12 degraded rows `schema-validation`. Pre-agreed threshold 18/20: not met.                                                                      |
| Application re-run, 6 alerts, rule logged                                           | 2/6 `LLM_TRIAGED`; 4 degraded, all `verdict missing narrative` (null, absent or blank; the message cannot tell which). Both `LLM_TRIAGED` alerts had EVIDENCE_STRIPPED. |

Replay "valid verdict" (counter `b`) checks the JSON Schema only. The schema's `narrative` has no minimum length, so a
blank narrative passes replay and fails `validated()`. Replay B and C are therefore not comparable to application
validity; the application results govern.

## Residual Risks (explicit)

- Why the model omits the narrative is not established; completion bodies are not logged.
- Behavior on OpenAI is not measured (no-spend constraint). Nothing here speaks to the P2 8B tool-call-validity metric
  or to production behavior.
- Replay covers one empty-snapshot conversation; results do not generalize to non-empty account histories.
- Per-rule differences in the batch are not established at these sample sizes.

The violated rule appears in the service console as `Verdict validation failed: ...`.

## Reversal Cost

LOW. Delete `application-ollama-local.yml` and revert the `max-tool-rounds` property and the verdict-options branch in
`TriageAgent`. No schema or contract change.

## References

- `scripts/triage-ollama-replay/` (diagnostic harness; frozen decision rule in its README)
- ADR-15 §3.3, §3.10; ADR-30 (Ollama scope note); ADR-29
- `rpe-triage-agent/README.md` — local Ollama application smoke procedure.

## Changelog

| Version | Date       | Author | Description                                                                                                                           |
|---------|------------|--------|---------------------------------------------------------------------------------------------------------------------------------------|
| 1.0.0   | 2026-11-21 | @amit  | Initial — ACCEPTED (dev-only scope). Records the Ollama dev profile, the tools-free verdict call and the measured llama3.2:3b limits. |
