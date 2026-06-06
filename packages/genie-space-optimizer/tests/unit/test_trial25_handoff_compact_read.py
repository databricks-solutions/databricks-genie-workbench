"""Trial 25 W25.1-4 — compact task-value read (consumer side).

After Trial 25, every `_tv_get` consumer call MUST:
  1. First check the compact blob at `<taskKey>_outputs` (this is what
     the Trial-25 publisher writes).
  2. Fall back to the per-key `dbutils.jobs.taskValues.get(taskKey, key)`
     when the blob is absent or doesn't contain that key — so consumers
     remain backward-compatible with parent runs that pre-date Trial 25.
  3. Emit `GSO_TRIAL25_HANDOFF_COMPACT_READ_V1` exactly once per
     (taskKey, reader) so we have observability for the migration.
  4. Cache the blob fetch per (dbutils, taskKey) so a single consumer
     call that pulls many keys does ONE blob fetch, not N.
"""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest


def _make_dbutils(values: dict[tuple[str, str], object]):
    """Returns a dbutils whose taskValues.get reads from `values`."""
    dbu = MagicMock()
    def _get(taskKey, key, default=""):
        return values.get((taskKey, key), default)
    dbu.jobs.taskValues.get.side_effect = _get
    return dbu


@pytest.fixture(autouse=True)
def _reset_compact_cache():
    """The Trial 25 compact read path caches blob fetches across calls
    within a single Python process. The cache MUST be cleared between
    tests so dbutils instances created by different tests don't see each
    other's blobs."""
    from genie_space_optimizer.jobs._handoff import _reset_compact_blob_cache
    _reset_compact_blob_cache()
    yield
    _reset_compact_blob_cache()


@pytest.fixture(autouse=True)
def _clean_flags(monkeypatch):
    for k in list(os.environ):
        if k.startswith("GSO_TRIAL25_"):
            monkeypatch.delenv(k, raising=False)
    yield


def test_tv_get_reads_from_compact_blob_when_present():
    from genie_space_optimizer.jobs._handoff import _tv_get

    blob = json.dumps({
        "scores": json.dumps({"x": 90}),
        "accuracy": "92.5",
        "model_id": "m-final",
    })
    dbu = _make_dbutils({
        ("lever_loop", "lever_loop_outputs"): blob,
    })

    assert _tv_get(dbu, "lever_loop", "scores") == json.dumps({"x": 90})
    assert _tv_get(dbu, "lever_loop", "accuracy") == "92.5"
    assert _tv_get(dbu, "lever_loop", "model_id") == "m-final"


def test_tv_get_falls_back_to_per_key_when_blob_absent():
    from genie_space_optimizer.jobs._handoff import _tv_get

    # No `lever_loop_outputs` key — simulates a legacy parent run that
    # pre-dates Trial 25 and wrote per-key.
    dbu = _make_dbutils({
        ("lever_loop", "scores"): json.dumps({"x": 90}),
        ("lever_loop", "accuracy"): "92.5",
    })

    assert _tv_get(dbu, "lever_loop", "scores") == json.dumps({"x": 90})
    assert _tv_get(dbu, "lever_loop", "accuracy") == "92.5"


def test_tv_get_falls_back_to_per_key_when_blob_missing_the_key():
    """The blob may be present but not include every key the consumer
    requests (e.g. a SKIP-path publish doesn't include
    `best_iteration`). The consumer must still try the per-key path
    before giving up — otherwise we'd lose data on a SKIP+resume."""
    from genie_space_optimizer.jobs._handoff import _tv_get

    blob = json.dumps({"scores": "{}", "accuracy": "0.0"})
    dbu = _make_dbutils({
        ("lever_loop", "lever_loop_outputs"): blob,
        # `best_iteration` was published per-key by a legacy task.
        ("lever_loop", "best_iteration"): "7",
    })

    assert _tv_get(dbu, "lever_loop", "scores") == "{}"
    assert _tv_get(dbu, "lever_loop", "best_iteration") == "7"


def test_tv_get_returns_default_when_blob_and_per_key_both_absent():
    from genie_space_optimizer.jobs._handoff import _tv_get

    dbu = _make_dbutils({})  # nothing
    assert _tv_get(dbu, "lever_loop", "scores", default="") == ""
    assert _tv_get(dbu, "lever_loop", "scores", default="sentinel") == "sentinel"


def test_tv_get_caches_blob_fetch_once_per_taskkey():
    """Reading 10 keys from the same task must NOT call
    `dbutils.jobs.taskValues.get(<taskkey>_outputs)` 10 times — exactly
    once per (dbutils, taskKey)."""
    from genie_space_optimizer.jobs._handoff import _tv_get

    blob = json.dumps({f"k{i}": f"v{i}" for i in range(10)})
    dbu = _make_dbutils({("lever_loop", "lever_loop_outputs"): blob})

    for i in range(10):
        assert _tv_get(dbu, "lever_loop", f"k{i}") == f"v{i}"

    # Count how many times we asked for the blob key specifically.
    blob_lookups = sum(
        1 for call in dbu.jobs.taskValues.get.call_args_list
        if call.kwargs.get("key") == "lever_loop_outputs"
    )
    assert blob_lookups == 1, (
        f"compact blob must be fetched exactly once per (dbutils, taskKey); "
        f"got {blob_lookups}"
    )


def test_tv_get_emits_compact_read_marker_once_per_taskkey(capsys):
    from genie_space_optimizer.jobs._handoff import _tv_get

    blob = json.dumps({"scores": "{}", "accuracy": "0.0"})
    dbu = _make_dbutils({("lever_loop", "lever_loop_outputs"): blob})

    _tv_get(dbu, "lever_loop", "scores")
    _tv_get(dbu, "lever_loop", "accuracy")

    out = capsys.readouterr().out
    occurrences = out.count("GSO_TRIAL25_HANDOFF_COMPACT_READ_V1")
    assert occurrences == 1, (
        f"compact read marker should be emitted exactly once per taskKey, "
        f"got {occurrences}"
    )


def test_tv_get_emits_no_read_marker_on_legacy_per_key_path(capsys):
    from genie_space_optimizer.jobs._handoff import _tv_get

    dbu = _make_dbutils({("lever_loop", "scores"): "{}"})
    _tv_get(dbu, "lever_loop", "scores")

    out = capsys.readouterr().out
    assert "GSO_TRIAL25_HANDOFF_COMPACT_READ_V1" not in out


def test_blob_that_is_not_json_falls_back_to_per_key_path():
    """Defensive: if the blob slot somehow holds non-JSON garbage (e.g.
    a manual override), do NOT crash — treat it as 'no blob' and use
    per-key reads."""
    from genie_space_optimizer.jobs._handoff import _tv_get

    dbu = _make_dbutils({
        ("lever_loop", "lever_loop_outputs"): "not-json-{{{",
        ("lever_loop", "scores"): "{}",
    })

    assert _tv_get(dbu, "lever_loop", "scores") == "{}"


def test_blob_that_is_json_but_not_a_dict_falls_back_to_per_key_path():
    """Defensive: if the blob slot holds a JSON value that isn't a
    dict (e.g. a bare string or list), treat as 'no blob'."""
    from genie_space_optimizer.jobs._handoff import _tv_get

    dbu = _make_dbutils({
        ("lever_loop", "lever_loop_outputs"): '"a string"',
        ("lever_loop", "scores"): "{}",
    })

    assert _tv_get(dbu, "lever_loop", "scores") == "{}"


# --- End-to-end: consumer that exercises many keys via _tv_get ---------


def test_get_lever_loop_outputs_routes_through_compact_blob():
    """The high-level consumer `get_lever_loop_outputs` MUST work
    transparently when the underlying publisher used the compact path —
    no signature change required at consumer call sites."""
    from genie_space_optimizer.jobs._handoff import (
        HandoffSource,
        get_lever_loop_outputs,
    )

    blob_payload = {
        "scores": json.dumps({"x": 90}),
        "accuracy": "92.5",
        "model_id": "m-final",
        "iteration_counter": "3",
        "best_iteration": "2",
        "skipped": "false",
        "all_eval_mlflow_run_ids": json.dumps(["r1", "r2"]),
        "all_failure_question_ids": json.dumps(["q1"]),
    }
    dbu = _make_dbutils({
        ("lever_loop", "lever_loop_outputs"): json.dumps(blob_payload),
    })
    spark = MagicMock()

    state = get_lever_loop_outputs(
        spark, run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
    )

    assert state["scores"].value == {"x": 90}
    assert state["accuracy"].value == 92.5
    assert state["model_id"].value == "m-final"
    assert state["iteration_counter"].value == 3
    assert state["best_iteration"].value == 2
    assert state["skipped"].value is False
    assert state["all_eval_mlflow_run_ids"].value == ["r1", "r2"]
    assert state["all_failure_question_ids"].value == ["q1"]
    # All sourced from taskValues — the blob path is "still taskValues",
    # not a Delta fallback.
    assert state["scores"].source is HandoffSource.TASK_VALUES
