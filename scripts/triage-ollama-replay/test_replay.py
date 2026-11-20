"""Offline tests for replay.py helpers and fixtures; no Ollama needed."""
import json
import os

import pytest
from jsonschema import Draft202012Validator

import replay
from replay import (MARKER, bad_tool_call, drop_tools, flatten_history,
                    load, split_system, strip_schema, verdict_valid)

R2 = load("r2", replay.FIXTURES)


def call(name, args):
    return {"function": {"name": name, "arguments": args}}


def test_schema_split():
    rules, schema = split_system(R2)
    assert MARKER not in rules and "properties" in schema


def test_strip_schema_removes_marker():
    assert MARKER not in strip_schema(R2)["messages"][0]["content"]


def test_drop_tools_leaves_input_intact():
    assert "tools" not in drop_tools(R2) and "tools" in R2


def test_flatten_has_no_tool_roles():
    roles = [m["role"] for m in flatten_history(R2)["messages"]]
    assert "tool" not in roles and roles[-1] == "user"


def test_bad_tool_call():
    assert not bad_tool_call([call("redisAccountHistory", '{"accountId":"acc-smoke-1"}')])
    assert bad_tool_call([call("accountDetails", '{"accountId":"acc-smoke-1"}')])
    assert bad_tool_call([call("redisAccountHistory", '{"severity":"HIGH"}')])
    assert bad_tool_call([call("redisAccountHistory", "not json")])


def test_verdict_rejects_non_json():
    v = Draft202012Validator(split_system(R2)[1])
    assert not verdict_valid("not json", v) and not verdict_valid("", v)


def test_load_rejects_bad_fixture(tmp_path):
    (tmp_path / "r1.json").write_text(json.dumps({"messages": [{"role": "user"}]}))
    with pytest.raises(SystemExit):
        load("r1", str(tmp_path))  # wrong role shape
    with pytest.raises(SystemExit):
        load("r2", str(tmp_path))  # missing file


def test_fixtures_same_alert():
    r1 = load("r1", replay.FIXTURES)
    assert r1["messages"][:2] == R2["messages"][:2]  # R2 extends R1's messages


def test_fixtures_have_no_credentials():
    for stage in ("r1", "r2"):
        with open(os.path.join(replay.FIXTURES, f"{stage}.json")) as f:
            text = f.read().lower()
        assert not any(k in text for k in ("authorization:", "bearer ", "api_key", "api-key"))
