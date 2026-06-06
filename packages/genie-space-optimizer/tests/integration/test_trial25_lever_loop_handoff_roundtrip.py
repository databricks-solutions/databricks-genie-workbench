"""Trial 25 W25.1 — end-to-end handoff round-trip.

Publish via `publish_task_outputs` then read via `get_lever_loop_outputs`
through the same fake `dbutils`. The consumer MUST see the same typed
values regardless of whether the publisher took the compact JSON-blob
path or the legacy per-key fan-out path.

This is the merge gate: if either flag state silently loses data, the
round-trip assertion fires before the change reaches a real anchor."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest


def _make_fake_dbutils():
    """A fake `dbutils` that records every `taskValues.set` into a
    backing store and serves every `taskValues.get` from the same store.
    This is the smallest mock that round-trips publish+read."""
    store: dict[tuple[str, str], object] = {}
    current_task: dict[str, str] = {"task": ""}
    dbu = MagicMock()

    def _set(key, value):
        # Real dbutils.jobs.taskValues.set scopes by the running task; we
        # tag every set with the task the test was simulating at the
        # time so consumer-side reads (which include `taskKey`) work.
        store[(current_task["task"], key)] = value
    def _get(taskKey, key, default=""):
        return store.get((taskKey, key), default)

    dbu.jobs.taskValues.set.side_effect = _set
    dbu.jobs.taskValues.get.side_effect = _get
    return dbu, store, current_task


@pytest.fixture(autouse=True)
def _reset_compact_cache():
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


_PAYLOAD = {
    "scores": {"q1": 90, "q2": 85},
    "accuracy": 92.5,
    "model_id": "m-final",
    "iteration_counter": 3,
    "best_iteration": 2,
    "skipped": False,
    "all_eval_mlflow_run_ids": ["r1", "r2"],
    "all_failure_question_ids": ["q3"],
}


def _assert_roundtrip(state):
    from genie_space_optimizer.jobs._handoff import HandoffSource
    assert state["scores"].value == {"q1": 90, "q2": 85}
    assert state["accuracy"].value == 92.5
    assert state["model_id"].value == "m-final"
    assert state["iteration_counter"].value == 3
    assert state["best_iteration"].value == 2
    assert state["skipped"].value is False
    assert state["all_eval_mlflow_run_ids"].value == ["r1", "r2"]
    assert state["all_failure_question_ids"].value == ["q3"]
    assert state["scores"].source is HandoffSource.TASK_VALUES


def test_compact_path_roundtrips_all_lever_loop_outputs():
    from genie_space_optimizer.jobs._handoff import (
        get_lever_loop_outputs,
        publish_task_outputs,
    )

    dbu, _store, ctx = _make_fake_dbutils()

    # Publish (under "lever_loop" task scope).
    ctx["task"] = "lever_loop"
    publish_task_outputs(dbu, task="lever_loop", outputs=_PAYLOAD)

    # Switch task scope — consumers run inside `finalize` / `deploy` and
    # read FROM `lever_loop` taskKey.
    ctx["task"] = "finalize"
    state = get_lever_loop_outputs(
        MagicMock(),  # spark — only used on the Delta fallback path
        run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
    )
    _assert_roundtrip(state)


def test_rollback_path_roundtrips_all_lever_loop_outputs(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL25_HANDOFF_COMPACT", "0")
    from genie_space_optimizer.jobs._handoff import (
        get_lever_loop_outputs,
        publish_task_outputs,
    )

    dbu, _store, ctx = _make_fake_dbutils()

    ctx["task"] = "lever_loop"
    publish_task_outputs(dbu, task="lever_loop", outputs=_PAYLOAD)

    ctx["task"] = "finalize"
    state = get_lever_loop_outputs(
        MagicMock(),
        run_id="run-001", catalog="cat", schema="sch", dbutils=dbu,
    )
    _assert_roundtrip(state)


def test_consumer_count_of_taskvalues_get_calls_drops_under_compact():
    """The point of Trial 25 is fewer `taskValues` calls per parent
    run. Under the compact path the consumer's blob read should
    short-circuit per-key gets — we verify by counting actual
    `dbutils.jobs.taskValues.get` invocations in both flag states."""
    from genie_space_optimizer.jobs._handoff import (
        get_lever_loop_outputs,
        publish_task_outputs,
    )

    # --- Compact path -------------------------------------------------
    dbu_c, _store_c, ctx_c = _make_fake_dbutils()
    ctx_c["task"] = "lever_loop"
    publish_task_outputs(dbu_c, task="lever_loop", outputs=_PAYLOAD)

    ctx_c["task"] = "finalize"
    get_lever_loop_outputs(
        MagicMock(),
        run_id="run-001", catalog="cat", schema="sch", dbutils=dbu_c,
    )
    compact_get_count = dbu_c.jobs.taskValues.get.call_count

    # --- Rollback path -----------------------------------------------
    os.environ["GSO_TRIAL25_HANDOFF_COMPACT"] = "0"
    try:
        from genie_space_optimizer.jobs._handoff import (
            _reset_compact_blob_cache,
        )
        _reset_compact_blob_cache()
        dbu_l, _store_l, ctx_l = _make_fake_dbutils()
        ctx_l["task"] = "lever_loop"
        publish_task_outputs(dbu_l, task="lever_loop", outputs=_PAYLOAD)

        ctx_l["task"] = "finalize"
        get_lever_loop_outputs(
            MagicMock(),
            run_id="run-001", catalog="cat", schema="sch", dbutils=dbu_l,
        )
        legacy_get_count = dbu_l.jobs.taskValues.get.call_count
    finally:
        del os.environ["GSO_TRIAL25_HANDOFF_COMPACT"]

    # The compact path must read the blob exactly once and then serve
    # everything from cache, so its get-call count is strictly less than
    # the per-key path's. This is the observable win of W25.1.
    assert compact_get_count < legacy_get_count, (
        f"expected compact_gets < legacy_gets but got "
        f"compact={compact_get_count} >= legacy={legacy_get_count}; "
        f"Trial 25 has no observable per-replay budget impact"
    )
