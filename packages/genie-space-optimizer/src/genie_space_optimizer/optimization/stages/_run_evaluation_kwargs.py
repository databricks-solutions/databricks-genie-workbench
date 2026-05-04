"""RunEvaluationKwargs: typed kwargs surface for evaluation.run_evaluation.

Closes the F1 weak point (``eval_kwargs: dict[str, Any]``) by giving
mypy and IDE tools a typed shape for the 25-parameter
``evaluation.run_evaluation`` signature. We use a TypedDict (not a
frozen dataclass) because:

1. The harness already constructs a kwargs dict and unpacks via ``**``;
   TypedDict matches this idiomatically.
2. We don't want the freezing risks of a full dataclass (subclassing,
   pickling, slots) for what is fundamentally a kwargs-passing
   convenience type.
3. Optional keyword-only parameters can be omitted without sentinel
   defaults — TypedDict with ``total=False`` makes this natural.

Phase H's per-stage I/O capture serializes the StageInput, not the
RunEvaluationKwargs (kwargs are runtime resolution, not stage I/O),
so frozen + serializable shape is not required.
"""

from __future__ import annotations

from typing import Any, TypedDict


class RunEvaluationKwargs(TypedDict, total=False):
    """Typed kwargs surface for ``evaluation.run_evaluation``.

    ``total=False`` makes every key optional; callers supply only the
    fields they need. Required-in-practice keys (``space_id``,
    ``experiment_name``, ``iteration``, ``benchmarks``, ``domain``,
    ``model_id``, ``eval_scope``, ``predict_fn``, ``scorers``) are
    enforced by the function signature itself, not the TypedDict.

    The signature is verified against ``evaluation.run_evaluation``
    at ``evaluation.py:7266-7293`` as of 2026-05-04 post-F8.
    """

    # Positional-equivalents in run_evaluation's signature.
    space_id: str
    experiment_name: str
    iteration: int
    benchmarks: list[dict[str, Any]]
    domain: str
    model_id: str | None
    eval_scope: str
    predict_fn: Any
    scorers: list[Any]

    # Keyword-only parameters.
    spark: Any
    w: Any
    catalog: str
    gold_schema: str
    uc_schema: str
    warehouse_id: str
    patched_objects: list[str] | None
    reference_sqls: dict[str, str] | None
    metric_view_names: set[str] | None
    metric_view_measures: dict[str, set[str]] | None
    optimization_run_id: str
    lever: int | None
    model_creation_kwargs: dict[str, Any] | None
    max_benchmark_count: int
    run_name: str | None
    extra_tags: dict[str, str] | None
