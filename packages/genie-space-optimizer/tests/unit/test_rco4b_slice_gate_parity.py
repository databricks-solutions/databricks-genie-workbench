"""RCO-4b Phase B Task 8 — parity between pure helpers and a faithful
reimplementation of the legacy inline slice-gate decision logic.

We do NOT drive the harness; the harness path depends on Spark.
Instead, the test re-encodes the legacy decision tree from
``harness._run_gate_checks:13030-13222`` as a small in-test reference
and asserts the three pure helpers produce identical outcomes for a
parametrized matrix of scenarios.

If the legacy harness logic changes, this test must be updated in
the same commit — the parity assertion only proves the helpers match
the in-test reference, not the live harness.
"""
from __future__ import annotations

from typing import Any

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    decide_slice_gate_post_eval,
    decide_slice_gate_should_run,
    compute_slice_gate_effective_tolerance,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    SliceGateInput,
)


def _legacy_should_run(
    inp: SliceGateInput,
    *,
    slice_min_reduction: float,
) -> tuple[bool, str | None, float | None]:
    """Reimplements ``harness._run_gate_checks:13030-13099``."""
    if not inp.legacy_gates_enabled:
        return (False, "legacy_gates_disabled", None)
    if not inp.slice_gate_enabled:
        return (False, "slice_gate_disabled", None)
    if inp.slice_benchmark_count <= 0:
        return (False, "slice_empty", None)
    _total = int(inp.full_benchmark_count)
    _sliced = int(inp.slice_benchmark_count)
    _small_corpus = _total <= 30
    _broadness_ratio = _sliced / _total if _total else 1.0
    _slice_threshold = 0.9 if _small_corpus else (1.0 - slice_min_reduction)
    if _broadness_ratio > _slice_threshold:
        return (False, "slice_too_broad", _broadness_ratio)
    return (True, None, _broadness_ratio)


def _legacy_effective_tolerance(
    inp: SliceGateInput,
    *,
    base_tol_standard: float,
    base_tol_small_corpus: float,
    small_corpus_threshold_rows: int,
) -> float:
    """Reimplements ``harness._run_gate_checks:13136-13144``."""
    _full_corpus = int(inp.full_benchmark_count)
    _is_small_corpus = _full_corpus < small_corpus_threshold_rows
    _base_tol = base_tol_small_corpus if _is_small_corpus else base_tol_standard
    _slice_qw = 100.0 / max(_full_corpus, 1)
    return max(_base_tol, inp.noise_floor + 2.0, _slice_qw + 0.5)


def _legacy_post_eval(
    slice_drops: tuple[dict[str, Any], ...],
) -> tuple[bool, str | None, str | None]:
    """Reimplements ``harness._run_gate_checks:13169-13222``."""
    if not slice_drops:
        return (True, None, None)
    judge = str(slice_drops[0].get("judge", "") or "")
    return (False, f"slice_gate: {judge}", judge)


PARITY_MATRIX = [
    # (description, legacy_gates, slice_gate, full_count, slice_count, noise, drops)
    ("legacy_off", False, True, 100, 20, 1.0, ()),
    ("slice_off", True, False, 100, 20, 1.0, ()),
    ("empty_slice", True, True, 100, 0, 1.0, ()),
    ("too_broad_standard", True, True, 100, 60, 1.0, ()),
    ("too_broad_small_corpus", True, True, 30, 28, 1.0, ()),
    ("at_threshold_standard", True, True, 100, 50, 1.0, ()),
    ("runs_standard_pass", True, True, 100, 20, 1.0, ()),
    ("runs_standard_pass_with_drops", True, True, 100, 20, 1.0,
     ({"judge": "correctness", "drop": -3.0},)),
    ("runs_small_corpus", True, True, 22, 5, 0.5, ()),
    ("runs_small_corpus_rollback", True, True, 22, 5, 0.5,
     ({"judge": "completeness", "drop": -4.0},)),
    ("tiny_corpus", True, True, 5, 2, 0.5, ()),
    ("high_noise", True, True, 100, 20, 20.0, ()),
]


@pytest.mark.parametrize(
    "desc,legacy_on,slice_on,full_count,slice_count,noise,drops",
    PARITY_MATRIX,
    ids=[m[0] for m in PARITY_MATRIX],
)
def test_pure_helpers_match_legacy_reference(
    desc: str,
    legacy_on: bool,
    slice_on: bool,
    full_count: int,
    slice_count: int,
    noise: float,
    drops: tuple[dict[str, Any], ...],
) -> None:
    sgi = SliceGateInput(
        ag_id="ag-parity",
        run_id="run-parity",
        iteration=1,
        all_benchmark_qids=tuple(f"q-{i}" for i in range(full_count)),
        prev_failure_qids=(),
        affected_question_ids=(),
        baseline_passing_qids_known=True,
        slice_benchmark_count=slice_count,
        full_benchmark_count=full_count,
        best_accuracy=80.0,
        noise_floor=noise,
        legacy_gates_enabled=legacy_on,
        slice_gate_enabled=slice_on,
    )

    # Pre-eval parity.
    pure_pre = decide_slice_gate_should_run(sgi, slice_min_reduction=0.5)
    legacy_should, legacy_skip, legacy_ratio = _legacy_should_run(
        sgi, slice_min_reduction=0.5
    )
    assert pure_pre.should_run == legacy_should, f"{desc} should_run mismatch"
    assert pure_pre.skip_reason == legacy_skip, f"{desc} skip_reason mismatch"
    if legacy_ratio is None:
        assert pure_pre.broadness_ratio is None
    else:
        assert pure_pre.broadness_ratio == pytest.approx(legacy_ratio)

    if not pure_pre.should_run:
        return  # tolerance + post-eval don't apply

    # Tolerance parity.
    pure_tol = compute_slice_gate_effective_tolerance(
        sgi,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    legacy_tol = _legacy_effective_tolerance(
        sgi,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    assert pure_tol == pytest.approx(legacy_tol), f"{desc} tolerance mismatch"

    # Post-eval parity.
    pure_post = decide_slice_gate_post_eval(
        sgi, slice_drops=drops, effective_tolerance=pure_tol
    )
    legacy_passed, legacy_reason, legacy_judge = _legacy_post_eval(drops)
    assert pure_post.passed == legacy_passed, f"{desc} passed mismatch"
    assert pure_post.rollback_reason == legacy_reason, f"{desc} reason mismatch"
    assert pure_post.regression_judge == legacy_judge, f"{desc} judge mismatch"
