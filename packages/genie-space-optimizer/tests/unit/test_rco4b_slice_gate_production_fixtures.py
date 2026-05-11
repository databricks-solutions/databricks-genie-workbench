"""RCO-4b Phase B Task 7 — production-shape fixture tests.

Each fixture directory under ``fixtures/rco4b/slice_gate/`` contains
an ``input.json`` (carrying ``slice_gate_input``, ``tolerance_constants``,
``slice_min_reduction``, ``broadness_small_corpus_rows``, and optional
``post_eval_input``) and an ``expected_output.json`` (carrying the
three pure-helper outputs). The test runs all three helpers and
asserts the combined output matches.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    decide_slice_gate_post_eval,
    decide_slice_gate_should_run,
    compute_slice_gate_effective_tolerance,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    SliceGateInput,
)


FIXTURE_ROOT = (
    pathlib.Path(__file__).parent
    / "fixtures" / "rco4b" / "slice_gate"
)


def _list_fixture_dirs() -> list[pathlib.Path]:
    return sorted(p for p in FIXTURE_ROOT.iterdir() if p.is_dir())


def _outcome_to_dict(o) -> dict:
    return {
        "should_run": o.should_run,
        "skip_reason": o.skip_reason,
        "broadness_ratio": o.broadness_ratio,
        "effective_tolerance": o.effective_tolerance,
        "passed": o.passed,
        "rollback_reason": o.rollback_reason,
        "regression_judge": o.regression_judge,
    }


@pytest.mark.parametrize(
    "fixture_dir",
    _list_fixture_dirs(),
    ids=lambda p: p.name,
)
def test_slice_gate_fixtures(fixture_dir: pathlib.Path) -> None:
    inp = json.loads((fixture_dir / "input.json").read_text(encoding="utf-8"))
    expected = json.loads(
        (fixture_dir / "expected_output.json").read_text(encoding="utf-8")
    )

    sgi = SliceGateInput(
        ag_id=inp["slice_gate_input"]["ag_id"],
        run_id=inp["slice_gate_input"]["run_id"],
        iteration=inp["slice_gate_input"]["iteration"],
        all_benchmark_qids=tuple(inp["slice_gate_input"]["all_benchmark_qids"]),
        prev_failure_qids=tuple(inp["slice_gate_input"]["prev_failure_qids"]),
        affected_question_ids=tuple(inp["slice_gate_input"]["affected_question_ids"]),
        baseline_passing_qids_known=bool(
            inp["slice_gate_input"]["baseline_passing_qids_known"]
        ),
        slice_benchmark_count=int(inp["slice_gate_input"]["slice_benchmark_count"]),
        full_benchmark_count=int(inp["slice_gate_input"]["full_benchmark_count"]),
        best_accuracy=float(inp["slice_gate_input"]["best_accuracy"]),
        noise_floor=float(inp["slice_gate_input"]["noise_floor"]),
        legacy_gates_enabled=bool(inp["slice_gate_input"]["legacy_gates_enabled"]),
        slice_gate_enabled=bool(inp["slice_gate_input"]["slice_gate_enabled"]),
    )

    pre = decide_slice_gate_should_run(
        sgi,
        slice_min_reduction=float(inp["slice_min_reduction"]),
        broadness_small_corpus_rows=int(inp["broadness_small_corpus_rows"]),
    )
    assert _outcome_to_dict(pre) == expected["pre_eval_outcome"]

    if pre.should_run:
        tol = compute_slice_gate_effective_tolerance(
            sgi,
            base_tol_standard=float(inp["tolerance_constants"]["base_tol_standard"]),
            base_tol_small_corpus=float(
                inp["tolerance_constants"]["base_tol_small_corpus"]
            ),
            small_corpus_threshold_rows=int(
                inp["tolerance_constants"]["small_corpus_threshold_rows"]
            ),
        )
        assert tol == pytest.approx(expected["effective_tolerance"])

        post_inp = inp["post_eval_input"]
        post = decide_slice_gate_post_eval(
            sgi,
            slice_drops=tuple(post_inp["slice_drops"]),
            effective_tolerance=tol,
        )
        assert _outcome_to_dict(post) == expected["post_eval_outcome"]
    else:
        assert expected["effective_tolerance"] is None
        assert expected["post_eval_outcome"] is None
