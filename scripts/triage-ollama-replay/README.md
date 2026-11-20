# RPE Ollama Triage Replay

## Purpose

This is a developer diagnostic harness for investigating malformed tool calls and final-verdict generation in the RPE
triage agent.

It replays captured OpenAI-compatible Ollama `/v1/chat/completions` request bodies directly against Ollama. It compares
the model's response to controlled changes in tool definitions, schema instructions, and tool-history representation.

**This is not an end-to-end test.** It does not start Kafka, Spring Boot, Redis, PostgreSQL, the RPE tool executor, or
the account guard. A returned tool call is inspected; it is not executed by the harness.

## Repository location

```text
scripts/triage-ollama-replay/
├── replay.py
└── README.md
```

The current script reads the captured bodies from `~/ollama-capture-1119`, not from a repository `fixtures/` directory.
Keep raw captures outside the repository. The hard-coded input path is documented under **Input files**; making it
configurable is a follow-up change.

## Requirements

- Python 3 and `pip`.
- `jsonschema>=4` (`Draft202012Validator` is used by the script).
- `jq` for the capture role/identity checks in this README (`brew install jq` if unavailable).
- Ollama running locally and reachable at `http://127.0.0.1:11434`.
- The model named in the captured request must be installed in Ollama.
- Captured request bodies and replay scripts from the same capture session.
- Ollama is the system under test, not an optional component. Replay cannot run without it, and substituting another
  backend (vLLM, llama.cpp) is a different experiment.

Create an isolated environment and install the dependency:

```bash
cd scripts/triage-ollama-replay
python3 -m venv .venv
source .venv/bin/activate
python -m pip install 'jsonschema>=4'
```

Do not commit `.venv/`.

## Quick path (captures already exist)

If `~/ollama-capture-1119` already holds bodies `000004`-`000006` from the same alert, skip **Capturing requests**
entirely:

1. Create the venv and install `jsonschema` (see **Requirements**).
2. Confirm Ollama is listening: `curl -sS http://127.0.0.1:11434/api/version`.
3. Optional: run the role/identity check under **Input files**.
4. Run the three probe commands under **Running the probes**.

## Capturing requests

Request capture is performed by the Ollama server, not by `replay.py`. The capture contains complete prompts, message
history, tool definitions and tool results. Treat it as sensitive data until reviewed.

Skip this section if usable captures already exist (see **Quick path**). A re-capture produces new sequence numbers;
see the warning at the end of this section.

This project uses Homebrew to manage Ollama. Stop the Homebrew service before starting a temporary foreground server
with request logging; do not run two Ollama servers on the same port.

1. Check whether anything holds the Ollama port:

   ```bash
   lsof -nP -iTCP:11434 -sTCP:LISTEN
   ```

   - No output: skip to step 2.
   - Listener present (Homebrew service, desktop app, other process): stop it first. For the Homebrew service:

     ```bash
     brew services stop ollama
     ```

   Re-run `lsof`; it must show no listener before step 2. Two servers on one port are not supported.

2. Start Ollama with request-body capture enabled and leave this terminal running:

   ```bash
   OLLAMA_DEBUG=1 OLLAMA_DEBUG_LOG_REQUESTS=true ollama serve 2>&1 | tee /tmp/ollama_debug.log
   ```

3. With the RPE triage application running on its local Ollama profile, publish one synthetic `velocity` alert from the
   repository root. Use a fresh alert ID and event ID for each capture:

   ```bash
   N=6
   ID=$(uuidgen | tr 'A-Z' 'a-z')
   printf '{"alertId":"%s","eventId":"evt-smoke-%s","accountId":"acc-smoke-1","ruleName":"velocity","reason":"smoke test","producedAt":"%s","schemaVersion":"1"}' \
     "$ID" "$N" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
     | docker compose exec -T redpanda rpk topic produce payment.alerts -k acc-smoke-1
   ```

   The rule name must be lowercase `velocity`, matching the detector and `TOOLS_BY_RULE` key. Wait for the triage
   attempt to finish.

4. Find the request-capture directory reported by Ollama:

   ```bash
   grep -ni -E 'stored in|ollama-request-logs|replay|curl|request.*(dir|path)' /tmp/ollama_debug.log | tail -30
   ```

   Use the actual directory printed by the local Ollama process. Do not assume a previous `/var/folders/...` path will
   be reused.

5. Copy the capture directory outside the temporary directory before stopping or restarting Ollama. Replace
   `<ollama-dump-directory>` with the path found in the previous step:

   ```bash
   CAPTURE_SOURCE="<ollama-dump-directory>"
   CAPTURE_DIR="$HOME/ollama-capture-1119"

   # Keep one capture session in CAP; archive an older copy before replacing it.
   if [ -d "$CAPTURE_DIR" ]; then
     mv "$CAPTURE_DIR" "${CAPTURE_DIR}-previous-$(date +%Y%m%d-%H%M%S)"
   fi
   mkdir -p "$CAPTURE_DIR"
   cp -R "$CAPTURE_SOURCE"/. "$CAPTURE_DIR"/
   find "$CAPTURE_DIR" -maxdepth 1 -type f | sort
   ```

6. Stop the temporary foreground server with `Ctrl-C`.

   - Replay requires a running Ollama on `127.0.0.1:11434`. Start one before running probes.
   - Only if you stopped a Homebrew-managed service in step 1, restore it:

     ```bash
     brew services start ollama
     ```

The captured files include full request bodies and generated replay shell scripts. They may contain account identifiers,
tool results, prompt content, and authorization headers. **Do not commit the raw capture directory or replay scripts.**
Inspect and sanitize any fixture before considering it for the repository.

> **Re-capture warning:** sequence numbers are per capture session. After a new capture, re-run the role/identity
> check under **Input files**, then update the sequence mapping in `replay.py` (`load(seq)` call sites and run
> definitions) and the probe table in this README. Replay against stale numbers silently mixes alerts.

## Input files: current script behavior

The current script defines:

```python
URL = "http://127.0.0.1:11434/v1/chat/completions"
CAP = os.path.expanduser("~/ollama-capture-1119")
```

`load(seq)` searches that directory for a file matching:

```text
~/ollama-capture-1119/*-<six-digit-sequence>_*_body.json
```

The current run definitions use these captures:

| Probe  | Capture sequence | Purpose                                                                                                    |
|--------|-----------------:|------------------------------------------------------------------------------------------------------------|
| `A-R1` |         `000004` | Captured first-round request, unchanged.                                                                   |
| `A-R2` |         `000005` | Captured second-round request, unchanged.                                                                  |
| `B`    |         `000005` | R2 with the `tools` field removed.                                                                         |
| `C`    |         `000005` | R2 without tools, with a JSON Schema `response_format` added.                                              |
| `E-R1` |         `000004` | R1 with the schema instructions removed from the system message.                                           |
| `E-R2` |         `000005` | R2 with the schema instructions removed from the system message.                                           |
| `F`    |         `000005` | R2 with tools removed and the prior assistant tool call/tool result converted into ordinary text messages. |

For the current capture set, bodies `000004`, `000005`, and `000006` are the three sequential requests for the **same
alert**: R1, R2, and R3. Do not combine an R1 body from one alert with an R2 body from another alert. Sequence numbers
are specific to one capture session and will change in later sessions.

Before replaying a new capture, verify the selected request roles and the alert identity (recommended; optional if the
mapping was already verified for this capture set). With `jq` installed:

```bash
CAPTURE_DIR="$HOME/ollama-capture-1119"

for seq in 000004 000005 000006; do
  f=$(find "$CAPTURE_DIR" -maxdepth 1 -name "*-${seq}_*_body.json" -print -quit)
  test -n "$f" || { echo "Missing body for sequence $seq"; exit 1; }
  echo "== $f"
  jq -c '[.messages[].role]' "$f"
  jq -r '.messages[] | select(.role == "user") | .content' "$f" \
    | grep -Eo '"(alertId|eventId|ruleName)"[[:space:]]*:[[:space:]]*"[^"]+"' | head -3
done
```

Expected message-role shapes for this capture are:

- `000004` (R1): `["system", "user"]`.
- `000005` (R2): `["system", "user", "assistant", "tool"]`.
- `000006` (R3): `["system", "user", "assistant", "tool", "assistant", "tool"]`.

Confirm that the alert ID and event ID are identical in all three bodies and that `ruleName` is `velocity`. If the roles
or identifiers differ, stop and correct the selected sequence mapping before replay.

Before sharing or committing anything, check all generated replay scripts for authorization headers (skip if captures
stay local). No output is expected; if a match appears, inspect and redact it:

```bash
grep -il authorization "$CAPTURE_DIR"/*_request.sh
```

## Pre-flight: record the test environment

Run this before the first probe when results must be reproducible or reported. Skip for ad-hoc local runs. Save the
Ollama version, model digest, Python version, and fixture hashes with the experiment results:

```bash
CAPTURE_DIR="$HOME/ollama-capture-1119"
{
  date -u
  python3 --version
  curl -sS http://127.0.0.1:11434/api/version
  ollama list
  shasum -a 256 \
    "$CAPTURE_DIR"/*-000004_*_body.json \
    "$CAPTURE_DIR"/*-000005_*_body.json \
    "$CAPTURE_DIR"/*-000006_*_body.json
} 2>&1 | tee "$HOME/replay-1120-metadata.txt"
```

The `ollama list` output records the installed model name and digest. Verify that it matches the `model` field in the
captured request bodies. Record the exact Ollama version and model digest alongside every result; a model tag alone is
not an immutable model identity.

## Running the probes

If the Spring Boot triage application is running, stop it so it does not compete with replay requests for the local
Ollama model. Keep Ollama running. Kafka, Redis, and PostgreSQL are not used by the replay harness.

Run the baseline and no-tools comparison:

```bash
python replay.py A-R1 A-R2 B -n 20 2>&1 | tee "$HOME/replay-1120-baseline.txt"
```

Run the schema-enforcement probe separately:

```bash
python replay.py C -n 20 2>&1 | tee "$HOME/replay-1120-schema.txt"
```

Run the schema-removed/tool-phase and flattened-history probes:

```bash
python replay.py E-R1 E-R2 F -n 20 2>&1 | tee "$HOME/replay-1120-history.txt"
```

The `2>&1` redirect is intentional: Python's `sys.exit("HTTP ...")` writes an HTTP error to stderr, and this ensures
`tee` captures it. Probe `C` is isolated because the current `post()` implementation exits the whole process on an HTTP
error; an HTTP 400 would otherwise prevent later probes in the same invocation from running.

The optional `-n` argument defaults to 20. For each named probe, the script sends one warm-up request whose response is
discarded, followed by `N` measured requests. Thus `-n 20` sends 21 requests per probe.

## What each probe changes

| Probe  | Captured stage | Change from capture                                                                                                                                                                                                                               |
|--------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `A-R1` | R1             | None; baseline first-round tool-call behavior.                                                                                                                                                                                                    |
| `A-R2` | R2             | None; baseline final-answer behavior after the captured tool result.                                                                                                                                                                              |
| `B`    | R2             | Removes tool definitions; retains the captured system prompt and message history.                                                                                                                                                                 |
| `C`    | R2             | Removes tool definitions and adds `response_format: json_schema`. Support for this exact request shape must be verified against the local Ollama version; an HTTP error is recorded as unsupported/unverified, not counted as a successful trial. |
| `E-R1` | R1             | Removes the schema portion of the system message while retaining tools. Measures tool-call behavior without the verdict schema in the prompt.                                                                                                     |
| `E-R2` | R2             | Same schema removal while retaining tools. Measures whether the tool-enabled request emits malformed calls or any tool call.                                                                                                                      |
| `F`    | R2             | Removes tools and converts the prior assistant tool call and tool result into plain assistant/user text. Tests a different representation of the captured history.                                                                                |

These are isolated model-request probes, not complete executions of a two-phase agent. For example, `E-R1` does not
execute its returned tool call or build a new R2 request from its result.

## Output: current script behavior

The Python script writes its results to **standard output**. It does not create a results file or save every raw Ollama
response.

For each probe it prints:

- `a`: count of responses with exactly one registered tool call whose arguments contain only `accountId` with the
  expected synthetic account value.
- `b`: count of responses with no tool calls and content that parses as JSON and validates against the captured
  TriageVerdict schema.
- `c_bad`: count of responses containing at least one malformed or unregistered tool call. This is a proxy for a
  possible guard-denial condition; the application guard is not invoked during replay.
- `toolcall`: number of responses containing one or more tool calls.
- `distinct`: number of distinct response hashes, based on response content and tool calls.
- `prompt_tokens`: minimum-to-maximum prompt token count reported by Ollama.
- `first non-pass`: a shortened example response when the script's generic pass check considers a response unsuccessful.
  Interpret this field cautiously: the generic check is not aligned with each probe's primary metric.

For example:

```bash
python replay.py A-R1 A-R2 B -n 20 2>&1 | tee "$HOME/replay-1120-baseline.txt"
```

saves the console summary and any stderr error to `~/replay-1120-baseline.txt`. This is **not** a raw-response capture.
The harness does not currently persist each complete request/response, per-trial latency, or `finish_reason`.

Ollama's own request-debug logging, if enabled, may separately capture requests under its temporary directory. That
behavior is controlled by the Ollama server, not by `replay.py`.

## Interpreting the counters

Use the probe-specific primary counters; do not treat every printed counter as a pass rate.

| Probe          | Primary counter(s)       | Interpretation                                                                                                                                               |
|----------------|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `A-R1`, `E-R1` | `a`                      | Correct first-round tool-call shape.                                                                                                                         |
| `A-R2`         | `b`, `c_bad`, `toolcall` | Valid final verdict, malformed tool-call proxy, and any tool-call frequency.                                                                                 |
| `B`, `C`, `F`  | `b`                      | Final response is strict JSON satisfying the captured schema.                                                                                                |
| `E-R2`         | `c_bad`, `toolcall`      | Exploratory tool-phase observation. Inspect malformed-call and tool-call counts; do not claim a pass/fix until a separate expected tool-call rate is agreed. |

A `c_bad` response is only a **proxy** for a guard denial. The harness does not invoke
`TriageTools.requireAlertAccount()` and cannot measure the application's actual guard-denial counter.

The JSON Schema check is also a proxy for the application's Jackson deserialization and `TriageVerdict` validation. It
does not prove semantic correctness of severity, evidence, narrative, or confidence.

### Frozen decision rule for this diagnostic run

Use these thresholds for the captured `llama3.2:3b` conversation. Record this rule in the experiment report before
running the probes; do not adjust it after seeing results.

| Rule                                   |                               Threshold | Interpretation                                                                                                                                                               |
|----------------------------------------|----------------------------------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Baseline fidelity gate (`A-R2`)        |                            `b >= 18/20` | The malformed final-answer failure does not reproduce often enough in direct replay. Stop the comparison and investigate replay fidelity before interpreting B/C/F as fixes. |
| Candidate support                      | `20/20` on that probe's primary counter | No failures were observed in this 20-trial sample. This supports further application-level verification; it is not a production reliability claim.                           |
| Inconclusive                           |                              `17–19/20` | Do not claim the variant fixes the failure. Increase or redesign the evaluation only if the result matters to the decision.                                                  |
| Does not meet the diagnostic threshold |                        `16/20` or lower | The variant does not meet the pre-agreed threshold.                                                                                                                          |

Assuming independent, identically distributed trials, zero observed failures in 20 trials gives a one-sided 95% exact
upper confidence bound on the underlying failure probability of approximately 13.9%. Therefore, even `20/20` is only a
small-sample diagnostic signal, not proof of production reliability.

The fidelity gate is evaluated first. If `A-R2` reaches `18/20` or better on `b`, stop; do not use the candidate
thresholds to claim a fix from a replay that no longer reproduces the baseline failure. Apply the `20/20` candidate rule
only to probes with a defined positive success metric (`a` for R1 tool calls or `b` for final verdicts). `E-R2` is
exploratory until its expected tool-call behavior is separately defined.

## Experimental controls and limitations

- The captured bodies selected for this run have `temperature=0.2` and no `seed` field. This allows sampling variation,
  but does not guarantee different outputs. `distinct=1` across 20 trials is an anomaly worth investigating; it is not,
  by itself, proof that the endpoint is deterministic.
- R1 (`000004`), R2 (`000005`), and R3 (`000006`) belong to the same alert. R2 uses a fixed captured tool result. The
  Redis snapshot is empty (`eventCount=0`, `meanAmount=0`, null last-location timestamp, and an empty recent-event
  window). Conclusions apply to this specific conversation and should not be generalized to non-empty account histories.
- The request body is copied from the original capture and transformed in memory for each probe. The captured file
  itself is not edited.
- Model, temperature, sampling parameters, prompt and captured tool result are inherited from the selected request
  unless a probe explicitly changes a field.
- The script uses fixed constants for the endpoint, capture directory, allowed tool names and account ID. It does not
  currently provide a `--model` override; the model in the captured request is used.
- A cross-model replay would cause each model's own template to render the same message history. It would be a separate
  model-comparison experiment, not the RPE P2 8B tool-call-validity metric: this harness replays a fixed R2 history and
  does not run the agent loop or execute tools.
- The script does not currently record latency, `finish_reason`, complete raw responses, or a per-trial JSONL/CSV
  record.
- The schema-enforcement probe (`C`) depends on the local Ollama version accepting the supplied `response_format`
  structure. Treat HTTP rejection as unsupported/unverified behavior; do not silently change the request and call it the
  same experiment.
- Results apply only to the captured model artifact, Ollama version, prompt and empty-snapshot conversation. They are
  not evidence for other model sizes, providers, production reliability, or fraud-detection accuracy.

## Data handling

Captured request bodies can contain prompts, account identifiers, tool results, and potentially sensitive data. The
current example uses synthetic test identifiers, but inspect every fixture before sharing or committing it.

- Do not commit Authorization headers, API keys, access tokens, or environment files.
- Do not commit the raw capture directory or generated replay shell scripts.
- Keep raw captures and local replay output outside the repository unless explicitly reviewed and approved.
- If sanitized fixtures are later added to the repository, retain only the minimum required request bodies and document
  their synthetic nature and hashes.
- Keep `.venv/` and local raw-result files out of Git.

## Known changes deferred

1. Replace the hard-coded `CAP` path with a `--capture-dir` option or repository-relative fixture loading, then update
   the README and tests together.
2. Resolve fixture files explicitly and fail with a useful message if a file is missing or ambiguous; the current
   `glob(...)[0]` can raise `IndexError`.
3. Make the per-probe pass/fail reporting explicit. The current generic `first_fail` condition can be misleading for R2
   probes whose target is a final verdict; use the primary counters for interpretation.
4. Persist structured per-trial output (probe, trial, finish reason, tool calls, content, token usage, and elapsed time)
   to a caller-selected output file.
5. Record exact fixture hashes and environment metadata alongside each result; the pre-flight command above captures the
   current values separately.
6. Consider continuing after an HTTP error for one probe while clearly marking that probe failed; currently an HTTP
   error exits the whole process.
7. Add a `--model` override only when cross-model comparisons are deliberately in scope. Keep those results separate
   from the single-model failure investigation and from the RPE P2 metric.

Until these changes are made, describe the script as a local diagnostic harness for this specific capture set, not as a
portable or automated RPE test suite.
