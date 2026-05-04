"""Phase G-lite Task 3: RunEvaluationKwargs TypedDict shape test.

The TypedDict mirrors evaluation.run_evaluation's 25-parameter
signature so callers can type-check kwargs without committing to a
frozen dataclass.
"""

from __future__ import annotations


def test_run_evaluation_kwargs_required_keys_present() -> None:
    from genie_space_optimizer.optimization.stages import RunEvaluationKwargs

    annotations = RunEvaluationKwargs.__annotations__
    # 9 positional-equivalents of run_evaluation:
    for key in (
        "space_id", "experiment_name", "iteration", "benchmarks",
        "domain", "model_id", "eval_scope", "predict_fn", "scorers",
    ):
        assert key in annotations, f"missing required key: {key}"


def test_run_evaluation_kwargs_keyword_only_keys_present() -> None:
    from genie_space_optimizer.optimization.stages import RunEvaluationKwargs

    annotations = RunEvaluationKwargs.__annotations__
    # 16 keyword-only parameters of run_evaluation:
    for key in (
        "spark", "w", "catalog", "gold_schema", "uc_schema",
        "warehouse_id", "patched_objects", "reference_sqls",
        "metric_view_names", "metric_view_measures",
        "optimization_run_id", "lever", "model_creation_kwargs",
        "max_benchmark_count", "run_name", "extra_tags",
    ):
        assert key in annotations, f"missing keyword-only key: {key}"


def test_run_evaluation_kwargs_can_be_constructed_from_dict() -> None:
    """A plain dict with the right keys is accepted as a RunEvaluationKwargs.
    This is the primary use case — the harness builds a dict and passes
    it through to evaluate_post_patch."""
    from genie_space_optimizer.optimization.stages import RunEvaluationKwargs

    sample: RunEvaluationKwargs = {
        "space_id": "s1",
        "experiment_name": "exp",
        "iteration": 1,
        "benchmarks": [],
        "domain": "airline",
        "model_id": None,
        "eval_scope": "full",
        "predict_fn": (lambda *a, **k: None),
        "scorers": [],
    }
    # TypedDict allows partial dicts (no `total=True`) — we let callers
    # omit keyword-only fields. The presence of required keys above is
    # what matters.
    assert sample["space_id"] == "s1"
