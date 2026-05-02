"""Pin that the eval-result carrier refresh works on rolled-back iterations.

Regression test for the Phase A burn-down bug where every iteration of two
separate real-Genie runs (airline + 7now) produced empty `eval_rows` because
`_latest_eval_result = full_result or {}` only ran on the accept branch of
the AG decision, while every iteration in those runs hit the rollback branch.
The fix moved the carrier refresh to right after `_run_gate_checks` returns,
via the `_extract_eval_result_from_gate` helper tested here.

These tests are pure-Python: they exercise the helpers directly without
standing up Spark, MLflow, Genie, or a full `_run_lever_loop` scope.
"""

from __future__ import annotations


def test_extract_eval_result_returns_full_result_on_accept_path() -> None:
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "passed": True,
        "full_result": {
            "question_ids": ["q_001", "q_002"],
            "scores": {"q_001": "yes", "q_002": "no"},
            "failure_question_ids": ["q_002"],
        },
    }
    out = _extract_eval_result_from_gate(gate_result)
    assert out["question_ids"] == ["q_001", "q_002"]
    assert out["scores"]["q_001"] == "yes"


def test_extract_eval_result_returns_failed_eval_result_on_rollback_path() -> None:
    """The bug: rolled-back iterations were silently skipping the carrier refresh.

    The fix uses `failed_eval_result` (which `_run_gate_checks` populates on
    rollback) so the carrier still tracks the most recent measurement.
    """
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "passed": False,
        "rollback_reason": "regression",
        "failed_eval_result": {
            "question_ids": ["q_001", "q_002", "q_003"],
            "scores": {"q_001": "yes", "q_002": "no", "q_003": "no"},
            "failure_question_ids": ["q_002", "q_003"],
        },
    }
    out = _extract_eval_result_from_gate(gate_result)
    assert out["question_ids"] == ["q_001", "q_002", "q_003"]
    assert set(out["failure_question_ids"]) == {"q_002", "q_003"}


def test_extract_eval_result_prefers_full_result_when_both_present() -> None:
    """Defensive: if a gate has both keys, full_result wins (the canonical accept payload)."""
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "full_result": {"question_ids": ["from_full"]},
        "failed_eval_result": {"question_ids": ["from_failed"]},
    }
    out = _extract_eval_result_from_gate(gate_result)
    assert out["question_ids"] == ["from_full"]


def test_extract_eval_result_returns_empty_when_neither_key_present() -> None:
    """Sentinel for "do not overwrite the carrier" — caller checks `if _gate_eval:`."""
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    assert _extract_eval_result_from_gate({}) == {}
    assert _extract_eval_result_from_gate({"passed": True}) == {}
    assert _extract_eval_result_from_gate({"full_result": None}) == {}
    assert _extract_eval_result_from_gate({"full_result": {}}) == {}


def test_extract_eval_result_handles_non_dict_input() -> None:
    """Defensive against accidental None / list inputs (gate_result should always
    be a dict, but the carrier-refresh path runs in a wrap-everything-defensively
    region of `_run_lever_loop`)."""
    from genie_space_optimizer.optimization.harness import (
        _extract_eval_result_from_gate,
    )

    assert _extract_eval_result_from_gate(None) == {}  # type: ignore[arg-type]
    assert _extract_eval_result_from_gate([]) == {}  # type: ignore[arg-type]
    assert _extract_eval_result_from_gate("not a dict") == {}  # type: ignore[arg-type]


def test_build_fixture_eval_rows_uses_scores_when_available() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
    )

    rows = _build_fixture_eval_rows({
        "question_ids": ["q_001", "q_002", "q_003"],
        "scores": {"q_001": "yes", "q_002": "no", "q_003": "pass"},
        "arbiter_verdicts": {"q_001": "both_correct", "q_002": "ground_truth_correct"},
    })
    by_qid = {r["question_id"]: r for r in rows}
    assert by_qid["q_001"] == {
        "question_id": "q_001",
        "result_correctness": "yes",
        "arbiter": "both_correct",
    }
    assert by_qid["q_002"]["result_correctness"] == "no"
    assert by_qid["q_002"]["arbiter"] == "ground_truth_correct"
    assert by_qid["q_003"]["result_correctness"] == "yes"
    assert "arbiter" not in by_qid["q_003"]


def test_build_fixture_eval_rows_falls_back_to_failure_set_when_scores_missing() -> None:
    """When `scores` is empty, derive correctness from `failure_question_ids`."""
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
    )

    rows = _build_fixture_eval_rows({
        "question_ids": ["q_001", "q_002"],
        "failure_question_ids": ["q_002"],
    })
    by_qid = {r["question_id"]: r["result_correctness"] for r in rows}
    assert by_qid == {"q_001": "yes", "q_002": "no"}


def test_build_fixture_eval_rows_returns_empty_when_no_qids() -> None:
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
    )

    assert _build_fixture_eval_rows({}) == []
    assert _build_fixture_eval_rows({"question_ids": []}) == []
    assert _build_fixture_eval_rows(None) == []  # type: ignore[arg-type]


def test_carrier_helpers_compose_to_recover_rolled_back_iter_data() -> None:
    """End-to-end shape test: gate rolls back → helpers produce a fixture-shape eval_rows.

    Pins the behavior the Phase A burn-down needed: a rolled-back gate must
    still feed real eval data into the iteration snapshot.
    """
    from genie_space_optimizer.optimization.harness import (
        _build_fixture_eval_rows,
        _extract_eval_result_from_gate,
    )

    gate_result = {
        "passed": False,
        "rollback_reason": "content_regression",
        "failed_eval_result": {
            "question_ids": ["airline_gs_001", "airline_gs_002"],
            "scores": {"airline_gs_001": "yes", "airline_gs_002": "no"},
            "failure_question_ids": ["airline_gs_002"],
            "arbiter_verdicts": {"airline_gs_002": "ground_truth_correct"},
        },
    }
    eval_payload = _extract_eval_result_from_gate(gate_result)
    rows = _build_fixture_eval_rows(eval_payload)
    assert len(rows) == 2
    assert {r["question_id"] for r in rows} == {"airline_gs_001", "airline_gs_002"}
    by_qid = {r["question_id"]: r for r in rows}
    assert by_qid["airline_gs_001"]["result_correctness"] == "yes"
    assert by_qid["airline_gs_002"]["result_correctness"] == "no"
    assert by_qid["airline_gs_002"]["arbiter"] == "ground_truth_correct"
