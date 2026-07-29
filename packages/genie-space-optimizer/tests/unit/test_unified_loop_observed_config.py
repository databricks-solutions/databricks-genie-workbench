from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock

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
