"""RCO-4b Phase B Task 3 — compute_slice_gate_effective_tolerance.

Pure math: ``effective_tolerance = max(base_tol, noise_floor + 2.0,
qw + 0.5)`` where ``qw = 100.0 / max(full_benchmark_count, 1)`` and
``base_tol`` switches between the standard and small-corpus values at
``full_benchmark_count < small_corpus_threshold_rows``.

Mirrors ``harness._run_gate_checks:13136-13144``.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.gate_types import (
    SliceGateInput,
)


def _make_input(full_corpus: int, noise_floor: float = 1.0) -> SliceGateInput:
    return SliceGateInput(
        ag_id="ag-001",
        run_id="run-001",
        iteration=1,
        all_benchmark_qids=tuple(f"q-{i}" for i in range(full_corpus)),
        prev_failure_qids=(),
        affected_question_ids=(),
        baseline_passing_qids_known=True,
        slice_benchmark_count=min(full_corpus, 10),
        full_benchmark_count=full_corpus,
        best_accuracy=85.0,
        noise_floor=noise_floor,
        legacy_gates_enabled=True,
        slice_gate_enabled=True,
    )


def test_standard_corpus_uses_standard_base_tolerance() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        compute_slice_gate_effective_tolerance,
    )
    inp = _make_input(full_corpus=100, noise_floor=1.0)
    # base_tol=5.0 wins because noise+2=3.0 and qw+0.5=1.5
    tol = compute_slice_gate_effective_tolerance(
        inp,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    assert tol == pytest.approx(5.0)


def test_small_corpus_uses_small_corpus_base_tolerance() -> None:
    """22 < 30 threshold → small-corpus base."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        compute_slice_gate_effective_tolerance,
    )
    inp = _make_input(full_corpus=22, noise_floor=1.0)
    tol = compute_slice_gate_effective_tolerance(
        inp,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    # qw = 100/22 = 4.545 → qw+0.5 = 5.045
    # noise+2 = 3.0
    # small base = 10.0 wins
    assert tol == pytest.approx(10.0)


def test_noise_floor_term_wins_when_largest() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        compute_slice_gate_effective_tolerance,
    )
    inp = _make_input(full_corpus=200, noise_floor=20.0)
    # noise+2 = 22.0
    # base_standard = 5.0
    # qw+0.5 = 100/200 + 0.5 = 1.0
    tol = compute_slice_gate_effective_tolerance(
        inp,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    assert tol == pytest.approx(22.0)


def test_question_weight_term_wins_with_tiny_corpus() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        compute_slice_gate_effective_tolerance,
    )
    # corpus = 5 rows, noise = 0.5
    # qw = 100/5 = 20.0 → qw+0.5 = 20.5
    # noise+2 = 2.5
    # small base = 10.0
    inp = _make_input(full_corpus=5, noise_floor=0.5)
    tol = compute_slice_gate_effective_tolerance(
        inp,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    assert tol == pytest.approx(20.5)


def test_zero_corpus_does_not_divide_by_zero() -> None:
    """Defensive: full_benchmark_count=0 must not raise ZeroDivisionError.
    Legacy code uses ``max(len(benchmarks), 1)`` for the divisor."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        compute_slice_gate_effective_tolerance,
    )
    inp = _make_input(full_corpus=0, noise_floor=1.0)
    tol = compute_slice_gate_effective_tolerance(
        inp,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    # qw = 100/max(0,1) = 100.0 → qw+0.5 = 100.5 wins
    assert tol == pytest.approx(100.5)


def test_threshold_boundary_full_corpus_equals_small_threshold() -> None:
    """Legacy code uses ``<`` (strict less-than), so corpus==threshold
    is the STANDARD path, not small."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        compute_slice_gate_effective_tolerance,
    )
    inp = _make_input(full_corpus=30, noise_floor=0.0)
    tol = compute_slice_gate_effective_tolerance(
        inp,
        base_tol_standard=5.0,
        base_tol_small_corpus=10.0,
        small_corpus_threshold_rows=30,
    )
    # corpus==30 → STANDARD base (5.0)
    # qw = 100/30 = 3.33 → +0.5 = 3.83
    # noise+2 = 2.0
    # max = 5.0
    assert tol == pytest.approx(5.0)
