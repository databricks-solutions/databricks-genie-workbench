"""GSO v2 — unified loop runs full native benchmark evals only."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from genie_space_optimizer.optimization import eval_budget
from genie_space_optimizer.optimization import unified_loop


def test_target_accuracy_normalizes_fraction_to_percent() -> None:
    assert unified_loop.target_accuracy_percent(0.9) == 90.0
    assert unified_loop.target_accuracy_percent(90.0) == 90.0


def test_full_benchmark_estimate_is_one_full_run() -> None:
    one_full = eval_budget.estimate_full_benchmark_seconds(30)
    assert one_full == eval_budget.estimate_eval_run_seconds(30)
    three_gate = eval_budget.estimate_three_gate_seconds(working_set_size=30)
    assert one_full < three_gate


def test_unified_loop_does_not_call_slice_or_p0_gates() -> None:
    src = inspect.getsource(unified_loop.run_unified_optimization_loop)
    assert "_run_gate_checks" not in src
    assert "SLICE" not in src
    assert "P0" not in src
    assert "eval_scope=FULL" in src


def _eval_result(accuracy: float, *, failed: bool = False) -> dict:
    return {
        "overall_accuracy": accuracy,
        "total_questions": 2,
        "correct_count": int(accuracy > 0),
        "rows": [],
        "failures": [],
        "remaining_failures": [],
        "eval_run_failed": failed,
    }


def test_unified_loop_stops_when_baseline_reaches_target(monkeypatch) -> None:
    writes: list[tuple[int, dict]] = []
    champions: list[int] = []
    status_updates: list[dict] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    native_eval = MagicMock(return_value=_eval_result(95.0))
    monkeypatch.setattr(unified_loop, "_native_eval", native_eval)
    propose = MagicMock()
    monkeypatch.setattr(unified_loop, "propose_patches", propose)
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, eval_result, **_kwargs: writes.append(
            (iteration, eval_result)
        ),
    )
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(
        unified_loop,
        "update_run_status",
        lambda _spark, _run_id, _catalog, _schema, **kwargs: status_updates.append(kwargs),
    )

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1],
        max_attempts=3,
        target_accuracy=0.9,
    )

    assert result["terminal_reason"] == "TARGET_REACHED"
    assert result["best_iteration"] == 0
    assert writes == [(0, _eval_result(95.0))]
    assert champions == [0]
    assert native_eval.call_count == 1
    propose.assert_not_called()
    assert status_updates[-1]["convergence_reason"] == "TARGET_REACHED"


def test_unified_loop_rolls_back_non_improving_candidate(monkeypatch) -> None:
    writes: list[int] = []
    rolled_back: list[tuple[int, str]] = []
    champions: list[int] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        MagicMock(side_effect=[_eval_result(50.0), _eval_result(40.0)]),
    )
    monkeypatch.setattr(
        unified_loop,
        "propose_patches",
        lambda *_args, **_kwargs: (
            1,
            "try table description",
            [{"type": "update_description", "lever": 1, "target": "t", "new_text": "better"}],
            "{}",
        ),
    )
    monkeypatch.setattr(
        unified_loop,
        "apply_patch_set",
        lambda *_args, **_kwargs: {
            "patch_deployed": True,
            "post_snapshot": {"title": "candidate"},
            "applied": [
                {
                    "patch": {"type": "update_description", "lever": 1, "target": "t"},
                    "action": {"risk_level": "low", "target": "t"},
                }
            ],
        },
    )
    rollback = MagicMock()
    monkeypatch.setattr(unified_loop, "rollback", rollback)
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, _eval_result, **_kwargs: writes.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "write_patch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "mark_patches_rolled_back",
        lambda _spark, _run_id, iteration, reason, *_args: rolled_back.append((iteration, reason)),
    )
    monkeypatch.setattr(unified_loop, "mark_iteration_rolled_back", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1],
        max_attempts=1,
        target_accuracy=90.0,
    )

    assert result["terminal_reason"] == "MAX_ATTEMPTS"
    assert result["best_iteration"] == 0
    assert result["levers_rolled_back"] == [1]
    assert writes == [0, 1]
    assert rolled_back and rolled_back[0][0] == 1
    assert champions == [0]
    rollback.assert_called_once()
