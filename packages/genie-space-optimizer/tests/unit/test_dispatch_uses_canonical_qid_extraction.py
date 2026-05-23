"""Phase 2 — every dispatch / SM admission callsite must use the canonical
qid extractor so production MLflow row shapes reach the authoritative paths.

This test exists because the prior 2026-05-22 surgical fix at ``optimizer.py:
_row_is_failing`` only fixed the hardness predicate. The admission callsites
that decide which qid to admit still used ``row.get("question_id")`` and
silently dropped every hard row whose qid lived under ``inputs/question_id``
(MLflow-flattened), nested ``inputs.question_id``, or
``request.kwargs.question_id``.

Each assertion fails on the pre-Phase-2 code and passes once the four
callsites delegate to ``extract_question_id``.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)


@pytest.fixture(scope="module")
def production_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["eval_rows"]]


@pytest.fixture(scope="module")
def expected_hard_qids() -> list[str]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [
        r["_expected_qid"] for r in data["eval_rows"]
        if r.get("_expected_hard") is True
    ]


def test_stamp_failing_qids_from_eval_results_admits_production_shape_rows(
    production_rows: list[dict],
    expected_hard_qids: list[str],
) -> None:
    """``_stamp_failing_qids_from_eval_results`` must use the canonical qid
    extractor so MLflow-flattened / nested / request.kwargs qids are admitted.
    Fails pre-Phase-2 because optimizer.py:8552 used ``row.get("question_id")``.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _stamp_failing_qids_from_eval_results,
    )

    eval_results = {"rows": production_rows}
    snap: dict = {}
    _stamp_failing_qids_from_eval_results(eval_results, snap)

    assert sorted(snap.get("_failing_qids") or []) == sorted(expected_hard_qids)
    assert len(snap.get("_eval_rows_failing") or []) == len(expected_hard_qids)


def test_build_initial_states_admits_production_shape_rows(
    production_rows: list[dict],
    expected_hard_qids: list[str],
) -> None:
    """``build_initial_states_from_eval_rows`` must use the canonical qid
    extractor. Fails pre-Phase-2 because dispatch_input.py:47 used
    ``row.get("question_id", "")`` and dropped every hard row.
    """
    from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
        build_initial_states_from_eval_rows,
    )

    states = build_initial_states_from_eval_rows(production_rows, iteration=1)
    qids = sorted(str(s.qid) for s in states)
    assert qids == sorted(expected_hard_qids)


def test_build_plan11_failing_qids_from_raw_admits_production_shape_rows(
    production_rows: list[dict],
    expected_hard_qids: list[str],
) -> None:
    """``_build_plan11_failing_qids_from_raw`` must use the canonical qid
    extractor in its ``by_qid`` dict. Fails pre-Phase-2 because
    optimizer.py:8576-8580 only admitted rows with top-level ``question_id``.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _build_plan11_failing_qids_from_raw,
    )

    out = _build_plan11_failing_qids_from_raw(
        failing_qids=list(expected_hard_qids),
        eval_rows=production_rows,
    )
    out_qids = sorted(str(e["qid"]) for e in out)
    assert out_qids == sorted(expected_hard_qids)


def test_diagnose_llm_find_eval_row_admits_production_shape_rows(
    production_rows: list[dict],
    expected_hard_qids: list[str],
) -> None:
    """``diagnose_llm._find_eval_row`` matches a state's qid to its eval row.
    Fails pre-Phase-2 because line 99 used ``row.get("question_id")``.
    """
    from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
        _find_eval_row,
    )

    class _Ctx:
        baseline_eval_rows = production_rows

    for qid in expected_hard_qids:
        row = _find_eval_row(_Ctx(), qid)
        assert row is not None, f"expected to find production row for {qid}"
