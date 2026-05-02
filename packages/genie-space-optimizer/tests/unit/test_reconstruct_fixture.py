"""Unit tests for the cycle 7 fixture reconstruction script.

These tests exercise the pure-Python transformation logic with synthetic
inputs. The MLflow + Delta data fetchers are exercised manually inside a
Databricks notebook (Phase 3 of the plan).
"""
from __future__ import annotations


def test_substitute_trace_ids_with_canonical_qids_per_iteration() -> None:
    """Per-iteration trace_id -> canonical_qid map produces clean eval_rows."""
    from genie_space_optimizer.scripts.reconstruct_airline_real_v1_fixture import (
        substitute_trace_ids_with_canonical_qids,
    )

    raw_iter = {
        "iteration": 1,
        "eval_rows": [
            {"question_id": "tr-aaa", "result_correctness": "yes", "arbiter": "both_correct"},
            {"question_id": "tr-bbb", "result_correctness": "no", "arbiter": "ground_truth_correct"},
        ],
        "clusters": [
            {"cluster_id": "H001", "question_ids": ["airline_q_002"], "root_cause": "missing_filter"},
        ],
    }
    trace_to_canonical = {"tr-aaa": "airline_q_001", "tr-bbb": "airline_q_002"}

    out = substitute_trace_ids_with_canonical_qids(raw_iter, trace_to_canonical)

    assert [r["question_id"] for r in out["eval_rows"]] == ["airline_q_001", "airline_q_002"]
    assert out["eval_rows"][0]["result_correctness"] == "yes"
    assert out["eval_rows"][0]["arbiter"] == "both_correct"
    assert out["clusters"] == raw_iter["clusters"], "non-eval-row fields must pass through unchanged"


def test_substitute_raises_when_trace_id_unmapped() -> None:
    """Missing trace_id in the map is a hard failure — silently dropping rows
    would corrupt the fixture without operator visibility."""
    import pytest
    from genie_space_optimizer.scripts.reconstruct_airline_real_v1_fixture import (
        substitute_trace_ids_with_canonical_qids,
    )

    raw_iter = {
        "iteration": 1,
        "eval_rows": [{"question_id": "tr-zzz", "result_correctness": "yes"}],
        "clusters": [],
    }
    with pytest.raises(KeyError, match="tr-zzz"):
        substitute_trace_ids_with_canonical_qids(raw_iter, {"tr-aaa": "airline_q_001"})


def test_substitute_passes_through_already_canonical_rows() -> None:
    """Forward-compatibility: if eval_rows already have canonical qids
    (e.g., after Track D ships and a future cycle runs), do not corrupt them."""
    from genie_space_optimizer.scripts.reconstruct_airline_real_v1_fixture import (
        substitute_trace_ids_with_canonical_qids,
    )

    raw_iter = {
        "iteration": 1,
        "eval_rows": [{"question_id": "airline_q_001", "result_correctness": "yes"}],
        "clusters": [],
    }
    out = substitute_trace_ids_with_canonical_qids(raw_iter, {})
    assert out["eval_rows"][0]["question_id"] == "airline_q_001"
