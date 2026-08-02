from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization import unified_loop


def _config(content: list[str]) -> dict:
    return {
        "version": 2,
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "instructions": {
            "text_instructions": [{"id": "instruction-1", "content": content}],
        },
    }


def test_settled_observation_failure_is_non_fatal(monkeypatch) -> None:
    def fail(_w, _space_id):
        raise RuntimeError("temporary GET failure")

    monkeypatch.setattr(unified_loop, "fetch_space_config", fail)

    assert unified_loop._read_observed_config_after_evaluation(
        MagicMock(),
        "space-1",
        run_id="run-1",
        iteration=0,
    ) is None


def test_baseline_observation_is_captured_after_native_evaluation(monkeypatch) -> None:
    """Regression: an immediate GET can predate Genie's normalization."""
    submitted = _config(["PURPOSE:\n- Help users"])
    normalized = _config(["PURPOSE:\n", "- Help users"])
    events: list[str] = []
    diagnostics: list[tuple[str, dict[str, object]]] = []
    writes: list[dict] = []
    fetch_count = 0

    def fetch_config(_w, _space_id):
        nonlocal fetch_count
        fetch_count += 1
        if fetch_count == 1:
            events.append("fetch_initial")
            return {"_parsed_space": copy.deepcopy(submitted)}
        events.append("fetch_after_eval")
        return {"_parsed_space": copy.deepcopy(normalized)}

    def enrich(*_args, **_kwargs):
        events.append("enrich")
        return SimpleNamespace(current_config=copy.deepcopy(submitted))

    def evaluate(*_args, **_kwargs):
        events.append("evaluate")
        return {
            "overall_accuracy": 95.0,
            "total_questions": 1,
            "correct_count": 1,
            "scores": {},
            "failures": [],
            "remaining_failures": [],
            "thresholds_met": True,
            "rows": [],
        }

    def write(*_args, **kwargs):
        events.append("write")
        writes.append(kwargs)

    monkeypatch.setattr(unified_loop, "fetch_space_config", fetch_config)
    monkeypatch.setattr(unified_loop, "run_space_quality_enrichment", enrich)
    monkeypatch.setattr(unified_loop, "_native_eval", evaluate)
    monkeypatch.setattr(unified_loop, "write_iteration", write)
    monkeypatch.setattr(
        unified_loop,
        "update_run_status",
        lambda *_args, **_kwargs: events.append("update_status"),
    )
    monkeypatch.setattr(
        unified_loop,
        "_stamp_terminal",
        lambda *_args, **_kwargs: events.append("stamp_terminal"),
    )

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run-1",
        space_id="space-1",
        benchmarks=[],
        catalog="catalog",
        schema="schema",
        levers=[1],
        max_attempts=1,
        target_accuracy=0.9,
        diagnostic_callback=lambda event, **payload: diagnostics.append(
            (event, payload)
        ),
    )

    assert result["terminal_reason"] == "TARGET_REACHED"
    assert events == [
        "fetch_initial",
        "enrich",
        "evaluate",
        "fetch_after_eval",
        "write",
        "update_status",
        "stamp_terminal",
    ]
    assert writes[0]["config_snapshot"] == submitted
    assert writes[0]["observed_config_snapshot"] == normalized
    assert [event for event, _payload in diagnostics] == [
        "Baseline evaluated",
        "Optimization stopped",
    ]
    assert diagnostics[0][1]["accuracy"] == 95.0
    assert diagnostics[1][1] == {
        "terminal_reason": "TARGET_REACHED",
        "champion_iteration": 0,
        "champion_accuracy": 95.0,
        "target_accuracy": 90.0,
        "attempts_used": 0,
        "max_attempts": 1,
    }


def test_failed_baseline_read_error_preserves_prior_champion_metadata(
    monkeypatch,
) -> None:
    """A restart must fail closed when durable champion state is unreadable."""
    baseline_config = _config(["PURPOSE:\n- Help users"])
    update_status = MagicMock(name="update_run_status")
    stamp_terminal = MagicMock(name="stamp_terminal")

    monkeypatch.setattr(
        unified_loop,
        "fetch_space_config",
        lambda *_args, **_kwargs: {"_parsed_space": copy.deepcopy(baseline_config)},
    )
    monkeypatch.setattr(
        unified_loop,
        "run_space_quality_enrichment",
        lambda *_args, **_kwargs: SimpleNamespace(
            current_config=copy.deepcopy(baseline_config)
        ),
    )
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        lambda *_args, **_kwargs: {
            "overall_accuracy": 0.0,
            "total_questions": 1,
            "correct_count": 0,
            "scores": {},
            "failures": ["q1"],
            "remaining_failures": ["q1"],
            "thresholds_met": False,
            "rows": [],
            "eval_run_failed": True,
            "eval_run_status": "FAILED",
        },
    )
    monkeypatch.setattr(
        unified_loop, "write_iteration", lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(unified_loop, "update_run_status", update_status)
    monkeypatch.setattr(unified_loop, "_stamp_terminal", stamp_terminal)
    load_persisted = MagicMock(
        side_effect=RuntimeError("transient Delta read failure")
    )
    monkeypatch.setattr(
        unified_loop, "load_all_scored_iterations", load_persisted,
    )

    with pytest.raises(RuntimeError, match="transient Delta read failure"):
        unified_loop.run_unified_optimization_loop(
            MagicMock(),
            MagicMock(),
            run_id="run-restarted",
            space_id="space-1",
            benchmarks=[],
            catalog="catalog",
            schema="schema",
            levers=[1],
            max_attempts=1,
            target_accuracy=0.9,
        )

    update_status.assert_not_called()
    stamp_terminal.assert_not_called()
    load_persisted.assert_called_once()


def test_accepted_attempt_emits_bounded_decision_diagnostics(monkeypatch) -> None:
    baseline_config = _config(["PURPOSE:\n- Help users"])
    candidate_config = _config(["PURPOSE:\n- Help users", "TERMS:\n- Revenue"])
    patch = {
        "type": "update_column",
        "lever": 1,
        "target": "main.sales.orders.revenue",
    }
    diagnostics: list[tuple[str, dict[str, object]]] = []
    evaluations = iter(
        [
            {
                "overall_accuracy": 70.0,
                "total_questions": 10,
                "correct_count": 7,
                "scores": {},
                "failures": ["q8", "q9", "q10"],
                "remaining_failures": ["q8", "q9", "q10"],
                "thresholds_met": False,
                "rows": [],
            },
            {
                "overall_accuracy": 80.0,
                "total_questions": 10,
                "correct_count": 8,
                "scores": {},
                "failures": ["q9", "q10"],
                "remaining_failures": ["q9", "q10"],
                "thresholds_met": False,
                "rows": [],
            },
        ]
    )

    monkeypatch.setattr(
        unified_loop,
        "fetch_space_config",
        lambda *_args, **_kwargs: {"_parsed_space": copy.deepcopy(baseline_config)},
    )
    monkeypatch.setattr(
        unified_loop,
        "run_space_quality_enrichment",
        lambda *_args, **_kwargs: SimpleNamespace(
            current_config=copy.deepcopy(baseline_config)
        ),
    )
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        lambda *_args, **_kwargs: next(evaluations),
    )
    monkeypatch.setattr(
        unified_loop,
        "propose_patches",
        lambda *_args, **_kwargs: (1, "Improve revenue metadata", [patch], "{}"),
    )
    monkeypatch.setattr(
        unified_loop,
        "_preapply_safety_screen",
        lambda patches, **_kwargs: (patches, []),
    )
    monkeypatch.setattr(
        unified_loop,
        "apply_patch_set",
        lambda *_args, **_kwargs: {
            "patch_deployed": True,
            "applied": [{"patch": patch, "action": {}}],
            "post_snapshot": copy.deepcopy(candidate_config),
            "dropped_patches": [],
        },
    )
    for name in (
        "write_iteration",
        "write_patch",
        "update_iteration_loop_state",
        "update_run_status",
        "_stamp_terminal",
    ):
        monkeypatch.setattr(unified_loop, name, lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run-1",
        space_id="space-1",
        benchmarks=[],
        catalog="catalog",
        schema="schema",
        levers=[1],
        max_attempts=1,
        target_accuracy=90.0,
        diagnostic_callback=lambda event, **payload: diagnostics.append(
            (event, payload)
        ),
    )

    assert result["terminal_reason"] == "MAX_ATTEMPTS"
    assert [event for event, _payload in diagnostics] == [
        "Baseline evaluated",
        "Attempt prepared",
        "Attempt accepted",
        "Optimization stopped",
    ]
    accepted = diagnostics[2][1]
    assert accepted["candidate_accuracy"] == 80.0
    assert accepted["improvement"] == 10.0
    assert accepted["champion_accuracy"] == 80.0
    assert "Improve revenue metadata" not in str(diagnostics)
