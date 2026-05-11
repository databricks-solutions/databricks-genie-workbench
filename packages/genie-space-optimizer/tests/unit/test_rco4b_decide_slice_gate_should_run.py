"""RCO-4b Phase B Task 2 — decide_slice_gate_should_run tests.

The helper encapsulates the three pre-eval gating branches at
``harness._run_gate_checks:13030-13099``. Pure function — no
``run_evaluation`` calls, no Spark, no prints.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.gate_types import (
    SliceGateInput,
)


def _make_input(**overrides) -> SliceGateInput:
    base = dict(
        ag_id="ag-001",
        run_id="run-001",
        iteration=1,
        all_benchmark_qids=tuple(f"q-{i}" for i in range(100)),
        prev_failure_qids=(),
        affected_question_ids=("q-1", "q-2"),
        baseline_passing_qids_known=True,
        slice_benchmark_count=10,
        full_benchmark_count=100,
        best_accuracy=85.0,
        noise_floor=2.0,
        legacy_gates_enabled=True,
        slice_gate_enabled=True,
    )
    base.update(overrides)
    return SliceGateInput(**base)


def test_legacy_disabled_skips_with_legacy_gates_disabled_reason() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(legacy_gates_enabled=False)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is False
    assert out.skip_reason == "legacy_gates_disabled"


def test_legacy_disabled_does_not_check_other_branches() -> None:
    """When legacy gates are off, the helper must short-circuit before
    looking at slice_gate_enabled or broadness."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(
        legacy_gates_enabled=False,
        slice_gate_enabled=False,  # also off
        slice_benchmark_count=0,   # also empty
    )
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.skip_reason == "legacy_gates_disabled"


def test_slice_gate_disabled_skips_with_slice_gate_disabled_reason() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_gate_enabled=False)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is False
    assert out.skip_reason == "slice_gate_disabled"


def test_empty_slice_skips_with_slice_empty_reason() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_benchmark_count=0)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is False
    assert out.skip_reason == "slice_empty"


def test_broadness_too_high_skips_with_slice_too_broad_reason() -> None:
    """Standard-corpus path: 60 sliced / 100 full = 0.6 ratio.
    Threshold = 1.0 - 0.5 = 0.5. 0.6 > 0.5 → skip."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_benchmark_count=60, full_benchmark_count=100)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is False
    assert out.skip_reason == "slice_too_broad"
    assert out.broadness_ratio == pytest.approx(0.6)


def test_broadness_at_threshold_runs() -> None:
    """Ratio == threshold passes (the legacy code uses ``<=``)."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_benchmark_count=50, full_benchmark_count=100)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is True
    assert out.skip_reason is None
    assert out.broadness_ratio == pytest.approx(0.5)


def test_broadness_below_threshold_runs() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_benchmark_count=20, full_benchmark_count=100)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is True
    assert out.broadness_ratio == pytest.approx(0.2)


def test_small_corpus_uses_relaxed_0_9_threshold() -> None:
    """Corpus of 30 rows: legacy code branches on ``_total <= 30``
    and uses 0.9 as the broadness threshold instead of
    ``1.0 - slice_min_reduction``. With min_reduction=0.5 and a 0.85
    ratio, standard path would skip; small-corpus path runs."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_benchmark_count=25, full_benchmark_count=30)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is True
    assert out.broadness_ratio == pytest.approx(25 / 30, abs=1e-3)


def test_small_corpus_still_skips_at_extreme_broadness() -> None:
    """28/30 = 0.933 > 0.9 small-corpus threshold → skip."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_benchmark_count=28, full_benchmark_count=30)
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.should_run is False
    assert out.skip_reason == "slice_too_broad"


def test_outcome_does_not_populate_post_eval_fields() -> None:
    """The pre-eval helper must NOT populate passed / rollback_reason /
    regression_judge / effective_tolerance. Those are the post-eval
    helper's responsibility."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input()
    out = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    assert out.passed is None
    assert out.rollback_reason is None
    assert out.regression_judge is None
    assert out.effective_tolerance is None


def test_broadness_small_corpus_rows_keyword_is_respected() -> None:
    """The keyword arg lets tests probe at different small-corpus
    thresholds. Default is 30; tests should still pass with explicit
    same value."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_should_run,
    )
    inp = _make_input(slice_benchmark_count=18, full_benchmark_count=25)
    out_default = decide_slice_gate_should_run(inp, slice_min_reduction=0.5)
    out_explicit = decide_slice_gate_should_run(
        inp, slice_min_reduction=0.5, broadness_small_corpus_rows=30,
    )
    assert out_default == out_explicit
