"""P4 C10 unit tests — Evidence-bundle anchoring honors the
requested ``task_run_id`` input. When fallback fires, a
``GSO_STALE_ANCHOR_DIAGNOSTIC_V1`` marker is emitted."""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.tools.evidence_bundle import (
    LeverLoopTaskSelection,
    _select_lever_loop_task,
    select_lever_loop_task,
    stale_anchor_diagnostic_marker,
)


def _t(
    *,
    task_run_id: str,
    state: str = "SUCCESS",
    end_time: int = 0,
    start_time: int = 0,
):
    return {
        "task_key": "lever_loop",
        "task_run_id": task_run_id,
        "run_id": f"run_for_{task_run_id}",
        "state": {"result_state": state},
        "end_time": end_time,
        "start_time": start_time,
    }


def test_empty_tasks_returns_none():
    sel = select_lever_loop_task([])
    assert sel.chosen is None
    assert sel.failed_attempts == []
    assert sel.honored_requested_id is False
    assert sel.stale_anchor_reason == ""


def test_empty_tasks_with_requested_id_marks_stale():
    sel = select_lever_loop_task([], requested_task_run_id="999")
    assert sel.chosen is None
    assert sel.honored_requested_id is False
    assert "999" in sel.stale_anchor_reason


def test_requested_id_honored_when_present_success():
    tasks = [
        _t(task_run_id="100", state="SUCCESS", end_time=10),
        _t(task_run_id="200", state="SUCCESS", end_time=20),  # latest by heuristic
        _t(task_run_id="300", state="FAILED", end_time=30),
    ]
    sel = select_lever_loop_task(tasks, requested_task_run_id="100")
    assert sel.chosen is not None
    assert sel.chosen["task_run_id"] == "100"
    assert sel.honored_requested_id is True
    assert sel.stale_anchor_reason == ""


def test_requested_id_honored_when_present_failed_attempt():
    """The requested attempt may itself be FAILED — honor it
    regardless of state."""
    tasks = [
        _t(task_run_id="100", state="FAILED", end_time=10),
        _t(task_run_id="200", state="SUCCESS", end_time=20),
    ]
    sel = select_lever_loop_task(tasks, requested_task_run_id="100")
    assert sel.chosen["task_run_id"] == "100"
    assert sel.honored_requested_id is True


def test_requested_id_not_present_falls_back_with_stale_reason():
    """The requested task_run_id does not exist in the parent job's
    lever_loop attempts → fallback selector picks the latest SUCCESS
    AND populates stale_anchor_reason."""
    tasks = [
        _t(task_run_id="100", state="SUCCESS", end_time=10),
        _t(task_run_id="200", state="SUCCESS", end_time=20),
    ]
    sel = select_lever_loop_task(tasks, requested_task_run_id="999")
    assert sel.chosen is not None
    assert sel.chosen["task_run_id"] == "200"  # latest SUCCESS
    assert sel.honored_requested_id is False
    assert "999" in sel.stale_anchor_reason
    assert "stale" in sel.stale_anchor_reason.lower()


def test_no_requested_id_legacy_latest_heuristic_intact():
    """Without ``requested_task_run_id``, the legacy latest-task
    heuristic is unchanged."""
    tasks = [
        _t(task_run_id="100", state="SUCCESS", end_time=10),
        _t(task_run_id="200", state="SUCCESS", end_time=20),
        _t(task_run_id="300", state="FAILED", end_time=30),
    ]
    sel = select_lever_loop_task(tasks)
    assert sel.chosen["task_run_id"] == "200"
    assert sel.honored_requested_id is False
    assert sel.stale_anchor_reason == ""


def test_requested_id_not_present_all_failed_fallback():
    """No SUCCESS attempt + requested id not present → fall back to
    latest FAILED + stale_anchor reason set."""
    tasks = [
        _t(task_run_id="100", state="FAILED", end_time=10),
        _t(task_run_id="200", state="FAILED", end_time=20),
    ]
    sel = select_lever_loop_task(tasks, requested_task_run_id="999")
    assert sel.chosen["task_run_id"] == "200"
    assert sel.honored_requested_id is False
    assert sel.stale_anchor_reason


def test_legacy_tuple_wrapper_signature_unchanged():
    """``_select_lever_loop_task`` returns the legacy 2-tuple shape
    for back-compat with existing callers (the harness
    ``build_bundle`` path was extended in a separate call site)."""
    tasks = [
        _t(task_run_id="100", state="SUCCESS", end_time=10),
        _t(task_run_id="200", state="FAILED", end_time=20),
    ]
    chosen, failed = _select_lever_loop_task(tasks)
    assert isinstance(chosen, dict)
    assert chosen["task_run_id"] == "100"
    assert failed[0]["task_run_id"] == "200"


def test_legacy_tuple_wrapper_accepts_requested_id():
    tasks = [
        _t(task_run_id="100", state="SUCCESS"),
        _t(task_run_id="200", state="SUCCESS", end_time=10),
    ]
    chosen, _ = _select_lever_loop_task(
        tasks, requested_task_run_id="100",
    )
    assert chosen["task_run_id"] == "100"


def test_stale_anchor_diagnostic_marker_shape_pinned():
    """Marker is a single line prefixed with the V1 sentinel and a
    JSON-sorted payload so downstream parsers can pin against it."""
    line = stale_anchor_diagnostic_marker(
        optimization_run_id="opt_abc",
        requested_task_run_id="999",
        chosen_task_run_id="200",
        reason="fallback fired",
    )
    assert line.startswith("GSO_STALE_ANCHOR_DIAGNOSTIC_V1 ")
    payload = json.loads(line[len("GSO_STALE_ANCHOR_DIAGNOSTIC_V1 "):])
    assert payload == {
        "chosen_task_run_id": "200",
        "optimization_run_id": "opt_abc",
        "reason": "fallback fired",
        "requested_task_run_id": "999",
    }


def test_stale_anchor_marker_handles_blank_inputs():
    line = stale_anchor_diagnostic_marker(
        optimization_run_id="",
        requested_task_run_id="",
        chosen_task_run_id="",
        reason="",
    )
    assert line.startswith("GSO_STALE_ANCHOR_DIAGNOSTIC_V1 ")
    payload = json.loads(line[len("GSO_STALE_ANCHOR_DIAGNOSTIC_V1 "):])
    assert payload == {
        "chosen_task_run_id": "",
        "optimization_run_id": "",
        "reason": "",
        "requested_task_run_id": "",
    }


def test_lever_loop_task_selection_is_dataclass():
    """Pin the typed selection contract."""
    sel = LeverLoopTaskSelection(
        chosen={"task_run_id": "1"},
        failed_attempts=[],
        requested_task_run_id="1",
        honored_requested_id=True,
        stale_anchor_reason="",
    )
    assert sel.chosen == {"task_run_id": "1"}
    assert sel.honored_requested_id is True
