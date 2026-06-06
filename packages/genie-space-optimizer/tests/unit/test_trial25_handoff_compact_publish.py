"""Trial 25 W25.1-4 — compact task-value publish (publisher side).

The publisher helper `publish_task_outputs` MUST:
  1. When the master flag GSO_TRIAL25_HANDOFF_COMPACT is ON (default) AND the
     per-task sub-flag is ON (default), write exactly ONE
     `dbutils.jobs.taskValues.set(key="<task>_outputs", value=<json blob>)`
     containing every output key. This replaces the per-key fan-out that
     was the root cause of `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250`.
  2. When ANY flag in the chain is OFF, write the legacy per-key fan-out
     verbatim so consumers that still expect per-key reads do not break.
  3. Emit `GSO_TRIAL25_HANDOFF_COMPACT_PUBLISH_V1` on the compact path
     (and not on the rollback path).
  4. Always stringify the blob payload (primitives, dicts, lists) so the
     wire format matches the legacy per-key publish — which makes the
     consumer-side parser path completely backward-compatible.
"""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest


def _make_dbutils_recorder():
    """Returns a MagicMock dbutils plus a `calls` list that captures every
    `dbutils.jobs.taskValues.set(key=..., value=...)` invocation in order.
    """
    dbu = MagicMock()
    calls: list[tuple[str, object]] = []
    def _set(key, value):
        calls.append((key, value))
    dbu.jobs.taskValues.set.side_effect = _set
    return dbu, calls


@pytest.fixture(autouse=True)
def _clean_flags(monkeypatch):
    """Strip GSO_TRIAL25_* env vars between tests so each test sees its
    own explicit flag state and not bleed-through from earlier tests."""
    for k in list(os.environ):
        if k.startswith("GSO_TRIAL25_"):
            monkeypatch.delenv(k, raising=False)
    yield


# --- Compact path (the happy path) ---------------------------------------


def test_publish_writes_single_blob_when_master_flag_on():
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    publish_task_outputs(
        dbu,
        task="lever_loop",
        outputs={
            "scores": {"x": 90},
            "accuracy": 92.5,
            "model_id": "m-final",
            "iteration_counter": 3,
            "skipped": False,
        },
    )

    assert len(calls) == 1, (
        f"compact path must publish exactly one blob, got {len(calls)} sets"
    )
    key, raw = calls[0]
    assert key == "lever_loop_outputs"
    parsed = json.loads(raw)
    assert set(parsed) == {
        "scores", "accuracy", "model_id", "iteration_counter", "skipped",
    }


def test_blob_stringifies_primitives_and_dicts_to_match_legacy_wire_format():
    """The legacy per-key publish wrote `json.dumps(dict_value)` for
    dicts/lists and `str(primitive)` for ints/floats/bools. The blob
    payload must mirror that so the consumer parser (`json.loads`,
    `float`, `_bool`) keeps working without changes."""
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    publish_task_outputs(
        dbu,
        task="lever_loop",
        outputs={
            "scores": {"x": 90},          # dict -> json string
            "accuracy": 92.5,              # float -> str
            "iteration_counter": 3,        # int -> str
            "skipped": False,              # bool -> str
            "model_id": "m-final",         # str -> str (unchanged)
            "all_eval_mlflow_run_ids": ["r1", "r2"],  # list -> json string
        },
    )

    parsed = json.loads(calls[0][1])
    assert parsed["scores"] == json.dumps({"x": 90})
    assert parsed["accuracy"] == "92.5"
    assert parsed["iteration_counter"] == "3"
    assert parsed["skipped"] == "False"
    assert parsed["model_id"] == "m-final"
    assert parsed["all_eval_mlflow_run_ids"] == json.dumps(["r1", "r2"])


def test_compact_path_emits_publish_marker(capsys):
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, _ = _make_dbutils_recorder()
    publish_task_outputs(
        dbu, task="lever_loop", outputs={"scores": {}, "accuracy": 0.0},
    )

    out = capsys.readouterr().out
    assert "GSO_TRIAL25_HANDOFF_COMPACT_PUBLISH_V1" in out
    assert "lever_loop" in out


# --- Rollback path (any flag OFF) ---------------------------------------


def test_master_flag_off_falls_back_to_per_key_fan_out(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL25_HANDOFF_COMPACT", "0")
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    outputs = {
        "scores": {"x": 90},
        "accuracy": 92.5,
        "model_id": "m-final",
    }
    publish_task_outputs(dbu, task="lever_loop", outputs=outputs)

    assert len(calls) == 3
    by_key = dict(calls)
    assert by_key["scores"] == json.dumps({"x": 90})
    assert by_key["accuracy"] == 92.5
    assert by_key["model_id"] == "m-final"
    # No blob written when in rollback.
    assert "lever_loop_outputs" not in by_key


def test_per_task_subflag_off_falls_back_to_per_key_fan_out(monkeypatch):
    """Each task has its own sub-flag (GSO_TRIAL25_<TASK>_JSON_BLOB) so
    operators can roll back a single task without touching the others."""
    monkeypatch.setenv("GSO_TRIAL25_LEVER_LOOP_JSON_BLOB", "0")
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    publish_task_outputs(
        dbu,
        task="lever_loop",
        outputs={"scores": {"x": 90}, "accuracy": 92.5},
    )

    assert len(calls) == 2
    by_key = dict(calls)
    assert "lever_loop_outputs" not in by_key
    assert by_key["scores"] == json.dumps({"x": 90})


def test_rollback_path_emits_no_publish_marker(monkeypatch, capsys):
    monkeypatch.setenv("GSO_TRIAL25_HANDOFF_COMPACT", "0")
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, _ = _make_dbutils_recorder()
    publish_task_outputs(
        dbu, task="lever_loop", outputs={"scores": {}, "accuracy": 0.0},
    )

    out = capsys.readouterr().out
    assert "GSO_TRIAL25_HANDOFF_COMPACT_PUBLISH_V1" not in out


# --- Independence across tasks (W25.1-4 share the helper) ---------------


def test_different_tasks_get_their_own_blob_key():
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    publish_task_outputs(dbu, task="preflight", outputs={"run_id": "r1"})
    publish_task_outputs(dbu, task="baseline_eval", outputs={"scores": {"x": 0}})
    publish_task_outputs(dbu, task="lever_loop", outputs={"accuracy": 0.5})
    publish_task_outputs(dbu, task="finalize", outputs={"status": "CONVERGED"})

    keys = [k for k, _ in calls]
    assert keys == [
        "preflight_outputs",
        "baseline_eval_outputs",
        "lever_loop_outputs",
        "finalize_outputs",
    ]


def test_subflag_for_one_task_does_not_affect_other_task(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL25_LEVER_LOOP_JSON_BLOB", "0")
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    publish_task_outputs(dbu, task="preflight", outputs={"run_id": "r1"})
    publish_task_outputs(dbu, task="lever_loop", outputs={"accuracy": 0.5})

    keys = [k for k, _ in calls]
    # preflight goes compact (1 blob), lever_loop falls back per-key (1 set).
    assert keys == ["preflight_outputs", "accuracy"]


# --- Empty / edge inputs ------------------------------------------------


def test_empty_outputs_dict_writes_empty_blob_on_compact_path():
    """Edge case: a task with no outputs still writes the marker blob so
    consumers can distinguish 'task ran but had nothing to say' from
    'task never published anything'. The blob is just `{}`."""
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    publish_task_outputs(dbu, task="lever_loop", outputs={})

    assert len(calls) == 1
    assert calls[0] == ("lever_loop_outputs", "{}")


def test_publish_is_no_op_in_rollback_path_for_empty_outputs(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL25_HANDOFF_COMPACT", "0")
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    publish_task_outputs(dbu, task="lever_loop", outputs={})

    assert calls == []


# --- Non-JSON-serialisable values (default=str fallback) ----------------


def test_publish_stringifies_non_json_objects_via_default_str_on_compact():
    """Regression: ``run_lever_loop.py`` previously published
    ``debug_info`` via ``json.dumps(debug_info, default=str)`` so
    datetimes degraded to their str form. The compact path MUST
    preserve that escape valve or it will raise mid-publish on any
    real run."""
    from datetime import datetime, timezone
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    when = datetime(2026, 6, 6, 10, 30, tzinfo=timezone.utc)
    publish_task_outputs(
        dbu, task="lever_loop",
        outputs={"debug_info": {"started_at": when}},
    )

    parsed = json.loads(calls[0][1])
    # The blob value must be the str form of the inner dict — proving
    # default=str kicked in and didn't raise.
    assert "started_at" in parsed["debug_info"]
    assert str(when) in parsed["debug_info"]


def test_publish_stringifies_non_json_objects_via_default_str_on_rollback(monkeypatch):
    """Same contract on the per-key rollback path."""
    from datetime import datetime, timezone
    monkeypatch.setenv("GSO_TRIAL25_HANDOFF_COMPACT", "0")
    from genie_space_optimizer.jobs._handoff import publish_task_outputs

    dbu, calls = _make_dbutils_recorder()
    when = datetime(2026, 6, 6, 10, 30, tzinfo=timezone.utc)
    publish_task_outputs(
        dbu, task="lever_loop",
        outputs={"debug_info": {"started_at": when}},
    )

    assert calls[0][0] == "debug_info"
    assert str(when) in calls[0][1]
