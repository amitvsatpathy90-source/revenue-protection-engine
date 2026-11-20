#!/usr/bin/env python3
"""Replay committed Ollama /v1 request fixtures under controlled variants (probes A, B, C, E, F)."""
import argparse, copy, hashlib, json, os, re, sys, urllib.error, urllib.request
from jsonschema import Draft202012Validator

URL = "http://127.0.0.1:11434/v1/chat/completions"
FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
SHAPES = {"r1": ["system", "user"], "r2": ["system", "user", "assistant", "tool"]}
MARKER = "Here is the JSON Schema instance your output must adhere to:"
TOOLS_OK = {"redisAccountHistory", "recentAlertsForAccount"}
ACCOUNT = "acc-smoke-1"


def load(stage, root):
    """Load fixture r1/r2 and verify its message-role shape."""
    path = os.path.join(root, f"{stage}.json")
    if not os.path.isfile(path):
        sys.exit(f"missing fixture: {path}")
    with open(path) as f:
        body = json.load(f)
    roles = [m["role"] for m in body["messages"]]
    if roles != SHAPES[stage]:
        sys.exit(f"{path}: roles {roles}, expected {SHAPES[stage]}")
    return body


def split_system(body):
    """Return (rules_text, schema_dict); the schema sits inside a ``` fence."""
    rules, _, rest = body["messages"][0]["content"].partition(MARKER)
    rest = re.sub(r"^\s*`{3}(?:json)?\s*", "", rest)  # strip opening fence
    schema, _ = json.JSONDecoder().raw_decode(rest)  # ignores trailing fence
    return rules.rstrip(), schema


def drop_tools(body):
    """Variant B: remove the tools field."""
    b = copy.deepcopy(body)
    b.pop("tools", None)
    return b


def strip_schema(body):
    """Variant E: keep tools, system message holds rules only."""
    b = copy.deepcopy(body)
    b["messages"][0]["content"] = split_system(body)[0]
    return b


def enforce_schema(body):
    """Variant C: no tools, schema also enforced via response_format."""
    b = drop_tools(body)
    b["response_format"] = {"type": "json_schema",
                            "json_schema": {"name": "TriageVerdict", "schema": split_system(body)[1]}}
    return b


def flatten_history(body):
    """Variant F: tool call and result become plain assistant/user text."""
    b = drop_tools(body)
    out = b["messages"][:2]
    for m in b["messages"][2:]:
        if m["role"] == "assistant" and m.get("tool_calls"):
            calls = "; ".join(f'{c["function"]["name"]}({c["function"]["arguments"]})'
                              for c in m["tool_calls"])
            out.append({"role": "assistant", "content": f"I called: {calls}"})
        elif m["role"] == "tool":
            out.append({"role": "user", "content": f"Tool result: {m['content']}"})
        else:
            out.append(m)
    b["messages"] = out
    return b


def post(body):
    """POST one non-streaming request; abort the process on HTTP error."""
    req = urllib.request.Request(URL, json.dumps(body).encode(),
                                 {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code}: {e.read().decode()[:500]}")


def bad_tool_call(calls):
    """True if any call is not a well-formed accountId-only call to a registered tool."""
    for c in calls:
        fn = c["function"]
        args = fn["arguments"]
        try:
            args = json.loads(args) if isinstance(args, str) else args
        except json.JSONDecodeError:
            return True
        if fn["name"] not in TOOLS_OK or set(args) != {"accountId"} or args["accountId"] != ACCOUNT:
            return True
    return False


def verdict_valid(content, validator):
    """Strict check: raw content must be JSON that satisfies the schema."""
    try:
        return validator.is_valid(json.loads(content))
    except (TypeError, json.JSONDecodeError):
        return False


# probe -> (fixture stage, variant transform)
RUNS = {
    "A-R1": ("r1", lambda b: b), "A-R2": ("r2", lambda b: b),
    "B": ("r2", drop_tools), "C": ("r2", enforce_schema),
    "E-R1": ("r1", strip_schema), "E-R2": ("r2", strip_schema),
    "F": ("r2", flatten_history),
}
# primary counter per probe; E-R2 is exploratory (no pass criterion)
PRIMARY = {"A-R1": "a", "E-R1": "a", "A-R2": "b", "B": "b", "C": "b", "F": "b"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("runs", nargs="+", choices=RUNS)
    ap.add_argument("-n", type=int, default=20)
    args = ap.parse_args()
    validator = Draft202012Validator(split_system(load("r2", FIXTURES))[1])  # schema from unmodified R2
    for name in args.runs:
        stage, variant = RUNS[name]
        body = variant(load(stage, FIXTURES))
        post(body)  # warm-up, discarded
        a = b = c = tc = 0
        hashes, ptoks, first_fail = set(), [], None
        for _ in range(args.n):
            r = post(body)
            msg = r["choices"][0]["message"]
            calls = msg.get("tool_calls") or []
            content = msg.get("content") or ""
            ok_a = len(calls) == 1 and not bad_tool_call(calls)
            ok_b = not calls and verdict_valid(content, validator)
            bad_c = bool(calls) and bad_tool_call(calls)
            a, b, c, tc = a + ok_a, b + ok_b, c + bad_c, tc + bool(calls)
            hashes.add(hashlib.sha1(json.dumps([content, calls], sort_keys=True).encode()).hexdigest())
            ptoks.append(r.get("usage", {}).get("prompt_tokens"))
            ok = {"a": ok_a, "b": ok_b}.get(PRIMARY.get(name), not bad_c)  # probe's primary counter
            if first_fail is None and not ok:
                first_fail = json.dumps({"content": content[:300], "tool_calls": calls})[:500]
        n = args.n
        toks = [t for t in ptoks if t is not None]
        print(f"{name:5} [{PRIMARY.get(name, 'explore')}] a={a}/{n} b={b}/{n} c_bad={c}/{n} "
              f"toolcall={tc}/{n} distinct={len(hashes)} "
              f"prompt_tokens={min(toks) if toks else 'n/a'}-{max(toks) if toks else 'n/a'}")
        if first_fail:
            print(f"      first fail: {first_fail}")


if __name__ == "__main__":
    main()
