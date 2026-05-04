"""Evidence-bundle attempt-aware lever_loop selection.

Today the bundle picks the first task whose ``task_key=='lever_loop'``,
regardless of state. Run 2423b960 had four failed attempts before a
successful retry; the bundle anchored to attempt 1 and missed the
analyzable success transcript. This suite pins the new selector that
prefers the latest SUCCESS attempt and records all attempts.
"""
from __future__ import annotations


def _task(*, run_id: str, state: str, end_time: int, start_time: int = 0) -> dict:
    return {
        "task_key": "lever_loop",
        "run_id": run_id,
        "state": {"life_cycle_state": "TERMINATED", "result_state": state},
        "end_time": end_time,
        "start_time": start_time,
    }


def test_picks_only_attempt_when_one_exists() -> None:
    from genie_space_optimizer.tools.evidence_bundle import _select_lever_loop_task

    tasks = [_task(run_id="100", state="SUCCESS", end_time=10)]
    selected, failed_attempts = _select_lever_loop_task(tasks)
    assert selected == tasks[0]
    assert failed_attempts == []


def test_picks_latest_success_among_multiple_successes() -> None:
    from genie_space_optimizer.tools.evidence_bundle import _select_lever_loop_task

    tasks = [
        _task(run_id="100", state="SUCCESS", end_time=10),
        _task(run_id="101", state="SUCCESS", end_time=20),
        _task(run_id="102", state="SUCCESS", end_time=15),
    ]
    selected, failed_attempts = _select_lever_loop_task(tasks)
    assert selected["run_id"] == "101"
    assert failed_attempts == []


def test_prefers_success_over_failed_regardless_of_order() -> None:
    """The 7Now reproducer: 4 fail, 1 success. Must pick the success."""
    from genie_space_optimizer.tools.evidence_bundle import _select_lever_loop_task

    tasks = [
        _task(run_id="200", state="FAILED", end_time=1),
        _task(run_id="201", state="FAILED", end_time=2),
        _task(run_id="202", state="FAILED", end_time=3),
        _task(run_id="203", state="FAILED", end_time=4),
        _task(run_id="204", state="SUCCESS", end_time=5),
    ]
    selected, failed_attempts = _select_lever_loop_task(tasks)
    assert selected["run_id"] == "204"
    assert sorted(t["run_id"] for t in failed_attempts) == [
        "200", "201", "202", "203",
    ]


def test_falls_back_to_latest_failed_when_no_success() -> None:
    from genie_space_optimizer.tools.evidence_bundle import _select_lever_loop_task

    tasks = [
        _task(run_id="300", state="FAILED", end_time=10),
        _task(run_id="301", state="FAILED", end_time=20),
    ]
    selected, failed_attempts = _select_lever_loop_task(tasks)
    assert selected["run_id"] == "301"
    # The chosen attempt is also recorded under failed_attempts so the
    # caller can dump the same per-attempt artifact for it.
    assert sorted(t["run_id"] for t in failed_attempts) == ["300", "301"]


def test_returns_none_when_no_lever_loop_task_exists() -> None:
    from genie_space_optimizer.tools.evidence_bundle import _select_lever_loop_task

    tasks = [
        {"task_key": "preflight", "run_id": "1", "state": {"result_state": "SUCCESS"}, "end_time": 1},
        {"task_key": "baseline_eval", "run_id": "2", "state": {"result_state": "SUCCESS"}, "end_time": 2},
    ]
    selected, failed_attempts = _select_lever_loop_task(tasks)
    assert selected is None
    assert failed_attempts == []


def test_ignores_non_lever_loop_tasks() -> None:
    from genie_space_optimizer.tools.evidence_bundle import _select_lever_loop_task

    tasks = [
        _task(run_id="400", state="SUCCESS", end_time=10),
        {"task_key": "finalize", "run_id": "401", "state": {"result_state": "FAILED"}, "end_time": 99},
    ]
    selected, failed_attempts = _select_lever_loop_task(tasks)
    assert selected["run_id"] == "400"
    assert failed_attempts == []
