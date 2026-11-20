# RPE Ollama Triage Replay

Diagnostic harness. It replays two captured Ollama `/v1/chat/completions` requests (R1 and R2 of one synthetic alert)
against a local Ollama with controlled changes, to see whether `llama3.2:3b` returns a valid tool call (R1) and a valid
final verdict (R2).

**Not an end-to-end test.** No Kafka, Spring Boot, Redis, PostgreSQL, tool execution or account guard is involved.
Returned tool calls are inspected, never executed. The triage service does not need to run.

```text
scripts/triage-ollama-replay/
├── replay.py
├── test_replay.py
├── README.md
├── fixtures/        # r1.json, r2.json
└── results/         # reference-*.txt are committed; other files are git-ignored
```

## Setup (once)

Needs Python 3, `jsonschema>=4`, `pytest`, and Ollama with `llama3.2:3b` installed.

```bash
cd scripts/triage-ollama-replay
python3 -m venv .venv
source .venv/bin/activate
python -m pip install 'jsonschema>=4' pytest
```

## Run

From `scripts/triage-ollama-replay` with the venv active (`source .venv/bin/activate` in every new terminal).

1. Start Ollama and confirm the model is installed:

   ```bash
   brew services start ollama
   curl -sS http://127.0.0.1:11434/api/version
   ollama list | grep llama3.2:3b
   ```

2. Run the offline tests. Expect `9 passed`:

   ```bash
   python -m pytest -q
   ```

3. Record the environment:

   ```bash
   { curl -sS http://127.0.0.1:11434/api/version; echo; ollama list; shasum -a 256 fixtures/*.json; } \
     2>&1 | tee results/reference-metadata.txt
   ```

4. Run the probes (about 5 minutes). `C` runs separately because an HTTP error aborts the process:

   ```bash
   python replay.py A-R1 A-R2 B E-R1 E-R2 F -n 20 2>&1 | tee results/reference-main.txt
   python replay.py C -n 20 2>&1 | tee results/reference-schema.txt
   ```

5. Apply the decision rule below and fill the **Reference results** table.

## Probes

| Probe  | Request | Change                                                                          |
|--------|---------|---------------------------------------------------------------------------------|
| `A-R1` | R1      | None (baseline first-round tool call).                                          |
| `A-R2` | R2      | None (baseline final answer after the tool result).                             |
| `B`    | R2      | Tools removed.                                                                  |
| `C`    | R2      | Tools removed, `response_format: json_schema` added.                            |
| `E-R1` | R1      | Verdict schema removed from the system message; tools kept.                     |
| `E-R2` | R2      | Same schema removal; tools kept. Exploratory.                                   |
| `F`    | R2      | Tools removed; the tool call and result converted to plain assistant/user text. |

## Reading the output

Each probe prints one line: `[primary counter] a b c_bad toolcall distinct prompt_tokens`, plus the first response that
fails the primary counter.

| Counter         | Meaning                                                                                    |
|-----------------|--------------------------------------------------------------------------------------------|
| `a`             | Exactly one tool call, a registered tool, arguments exactly `{"accountId":"acc-smoke-1"}`. |
| `b`             | No tool call, and the content is strict JSON that validates against the verdict schema.    |
| `c_bad`         | Malformed or unregistered tool call (proxy for a guard denial; the real guard is not run). |
| `toolcall`      | Responses containing any tool call.                                                        |
| `distinct`      | Distinct responses. `1` across 20 trials is an anomaly to investigate.                     |
| `prompt_tokens` | Min-max prompt size reported by Ollama.                                                    |

Primary counters: `a` for `A-R1`, `E-R1`; `b` for `A-R2`, `B`, `C`, `F`; `E-R2` has none (exploratory).

### Decision rule (fixed before running; do not change after seeing results)

| Rule                   | Threshold                      | Meaning                                                                                |
|------------------------|--------------------------------|----------------------------------------------------------------------------------------|
| Fidelity gate (`A-R2`) | `b >= 18/20`                   | The failure does not reproduce in replay. Stop and investigate before reading B, C, F. |
| Candidate support      | `20/20` on the primary counter | No failures in 20 trials.                                                              |
| Inconclusive           | `17-19/20`                     | Do not claim a fix.                                                                    |
| Below threshold        | `16/20` or lower               | Does not meet the threshold.                                                           |

`20/20` is a one-sided 95% failure bound of about 13.9%: a small-sample signal, not a reliability claim.

## Reference results

Ollama version: 0.40.16. Model digest: a80c4f17acd5. Date: 2026-11-20.

| Probe  | Primary counter | Result (n=20)                                    |
|--------|-----------------|--------------------------------------------------|
| `A-R1` | `a`             | 20/20                                            |
| `A-R2` | `b`             | 0/20 (c_bad 20/20: verdict emitted as tool call) |
| `B`    | `b`             | 20/20                                            |
| `C`    | `b`             | 20/20                                            |
| `E-R1` | `a`             | 20/20                                            |
| `E-R2` | exploratory     | toolcall 0/20, distinct 2                        |
| `F`    | `b`             | 0/20 (model emitted text "I called: ...")        |

## Limits

- The fixtures have `temperature=0.2` and no seed. Outputs can vary, but this is not guaranteed.
- R2 uses a fixed, empty tool result (`eventCount=0`, no location, no recent events). Results apply to this
  conversation only.
- Probe `C` needs the local Ollama to accept `response_format`. An HTTP error is a result: record it, do not alter the
  request.
- Results apply only to this model, Ollama version and fixture set. They say nothing about other model sizes,
  providers, production reliability, or the P2 8B tool-call-validity metric.
- Out of scope: per-trial output files, latency, `finish_reason`, a `--model` override.

## Refreshing fixtures

Only if the prompt, tools or verdict schema change. Capture with
`OLLAMA_DEBUG=1 OLLAMA_DEBUG_LOG_REQUESTS=true ollama serve` (the log line "stored in <dir>" names the capture
directory), trigger one synthetic `velocity` alert, then copy the two bodies of that alert to `fixtures/r1.json` (roles
`system,user`) and `fixtures/r2.json` (roles `system,user,assistant,tool`). Run `python -m pytest -q`.

## Data handling

- Only the two fixtures are committed. Inspect them first: they hold the full prompt and synthetic identifiers.
- Never commit the raw capture directory, its `_request.sh` files, credentials or `.venv/`.
