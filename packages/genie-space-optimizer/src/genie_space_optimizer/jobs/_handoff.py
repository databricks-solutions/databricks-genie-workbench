"""Cross-task state helpers for the GSO v2 5-task DAG (Delta-only handoff).

Design note D9 (Delta-only): the v2 DAG
(``00_intake_and_snapshot`` -> ``01_benchmark_qc_and_repair`` ->
``02_baseline_eval_and_triage`` -> ``03_optimize`` -> ``publish_and_audit``)
publishes NO Databricks task values. Every cross-task read is served from the
durable Delta state persisted in ``genie_opt_runs`` and ``genie_opt_iterations``.
This is both simpler and Repair-Run safe: Databricks does not propagate task
values to repaired downstream tasks, but Delta rows survive, so reading straight
from Delta is the single source of truth.

Each read returns a ``HandoffValue`` carrying the ``HandoffSource`` it came from
so log readers can distinguish a value reconstructed from Delta
(``DELTA_FALLBACK``) from a value that was never persisted (``MISSING``).

Historical note: an earlier design probed the Databricks job task-value store
first and fell back to Delta. Under the v2 DAG those probes targeted task keys
from the retired 6-task DAG (``preflight``, ``baseline_eval``, ``enrichment``,
``lever_loop``) that no longer exist in a run, and the probe raised
``ValueError: Task key does not exist in run`` before the Delta fallback could
run. The task-value probe has been removed; Delta is now the ONLY source.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class HandoffSource(str, Enum):
    """Where a HandoffValue was sourced from.

    Under D9 (Delta-only handoff) the helpers in this module only ever emit
    ``DELTA_FALLBACK`` or ``MISSING``. ``TASK_VALUES`` is retained for direct /
    authoritative reads that never pass through Delta — the ``catalog`` /
    ``schema`` job-widget bootstrap keys in ``get_run_context`` and the
    published-scorecard check in ``resolve_finalize_skip_scores`` — and for the
    ``DEFAULT`` documented-default case.
    """

    TASK_VALUES = "task_values"        # direct/authoritative read (e.g. job widget)
    DELTA_FALLBACK = "delta_fallback"  # reconstructed from durable Delta state
    DEFAULT = "default"                # documented default applied
    MISSING = "missing"                # not found anywhere — caller decides


@dataclass(frozen=True)
class HandoffValue:
    """A typed read of a cross-task value.

    Attributes:
        key: Logical key being read (e.g. ``"run_id"``,
            ``"overall_accuracy"``).
        value: The resolved value (already typed — int / float / dict /
            list / str / None).
        source: Where ``value`` came from, for audit logs.
        delta_query: Optional SQL string of the Delta query used when
            ``source == DELTA_FALLBACK``. Captured for log audits;
            never None on the fallback path.
    """

    key: str
    value: Any
    source: HandoffSource
    delta_query: str | None = None


import json
import logging
from typing import TYPE_CHECKING, Optional

from genie_space_optimizer.optimization.state import load_run

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _resolve(
    *,
    delta_value: Any,
    key: str,
    delta_query: Optional[str] = None,
) -> HandoffValue:
    """Delta-only cross-task resolution (D9).

    A non-``None`` ``delta_value`` is the resolved value (source
    ``DELTA_FALLBACK``); ``None`` means the upstream task never persisted it
    (source ``MISSING`` — the caller decides whether that is fatal). Callers pass
    ``delta_value`` already typed (e.g. ``overall_accuracy`` as a float,
    ``scores_json`` parsed to a dict by the ``_load_*`` helpers), so this function
    does no parsing of its own.
    """
    if delta_value is not None:
        return HandoffValue(
            key=key, value=delta_value,
            source=HandoffSource.DELTA_FALLBACK,
            delta_query=delta_query,
        )
    return HandoffValue(
        key=key, value=None, source=HandoffSource.MISSING,
    )


def get_run_context(
    spark: "SparkSession",
    *,
    run_id_widget: str,
    catalog_widget: str,
    schema_widget: str,
    dbutils: Any = None,
) -> dict[str, HandoffValue]:
    """Read all run-level context from ``genie_opt_runs`` (Delta-only, D9).

    The widgets ``run_id_widget``, ``catalog_widget``, ``schema_widget`` are the
    only bootstrap inputs every task can rely on — Databricks Jobs widgets /
    parameters survive Repair Run. ``run_id_widget`` locates the durable run row;
    ``catalog`` / ``schema`` are echoed straight back as the bootstrap keys.

    ``dbutils`` is accepted for call-site compatibility but is no longer used: v2
    tasks publish NO Databricks task values (D9), so every field is read from Delta.

    Returns a dict of ``HandoffValue`` keyed by logical name.

    Raises:
        RuntimeError: if ``run_id_widget`` is empty (nothing to look up), or if
            there is no ``genie_opt_runs`` row for it (the run was never created).
    """
    del dbutils  # D9: Delta-only handoff — no task values are consulted.

    if not run_id_widget:
        raise RuntimeError(
            "get_run_context: run_id_widget is required — there is no run_id to "
            "look up in Delta."
        )

    delta_query = (
        f"SELECT * FROM {catalog_widget}.{schema_widget}.genie_opt_runs "
        f"WHERE run_id = '{run_id_widget}' LIMIT 1"
    )
    run_row = load_run(spark, run_id_widget, catalog_widget, schema_widget)

    if run_row is None:
        # No durable run row — we know nothing about this run.
        raise RuntimeError(
            f"get_run_context: no run context available for run_id="
            f"{run_id_widget!r}. No row in "
            f"{catalog_widget}.{schema_widget}.genie_opt_runs — the intake task "
            f"(00_intake_and_snapshot) must create the run row first."
        )

    # Per-key resolution. Each entry: (logical_key, genie_opt_runs column).
    spec = [
        ("run_id", "run_id"),
        ("space_id", "space_id"),
        ("domain", "domain"),
        ("experiment_name", "experiment_name"),
        ("apply_mode", "apply_mode"),
        ("triggered_by", "triggered_by"),
        ("warehouse_id", "warehouse_id"),
        ("max_iterations", "max_iterations"),
        ("levers", "levers"),
        ("max_benchmark_count", "max_benchmark_count"),
        ("human_corrections", "human_corrections_json"),
    ]

    out: dict[str, HandoffValue] = {}
    for logical, delta_key in spec:
        delta_val = run_row.get(delta_key)
        # JSON columns (levers, human_corrections_json) come back from
        # load_run as strings. Parse them here so callers always see a
        # typed value.
        if logical in ("levers", "human_corrections") and isinstance(delta_val, str):
            try:
                delta_val = json.loads(delta_val)
            except (ValueError, TypeError):
                delta_val = None
        out[logical] = _resolve(
            delta_value=delta_val,
            key=logical,
            delta_query=delta_query,
        )

    # ``catalog`` and ``schema`` come straight from the job widgets — they ARE
    # the bootstrap keys, not fields stored in the run row, so they are marked
    # as a direct/authoritative read (TASK_VALUES) rather than DELTA_FALLBACK.
    out["catalog"] = HandoffValue(
        key="catalog", value=catalog_widget,
        source=HandoffSource.TASK_VALUES,
    )
    out["schema"] = HandoffValue(
        key="schema", value=schema_widget,
        source=HandoffSource.TASK_VALUES,
    )
    return out


from genie_space_optimizer.common.delta_helpers import _fqn, run_query


def _load_baseline_iteration_row(
    spark: "SparkSession", run_id: str, catalog: str, schema: str,
) -> dict | None:
    """Latest iteration=0, eval_scope='full' row for ``run_id``.

    Distinct from ``load_latest_full_iteration`` which orders by iteration
    DESC — the baseline is uniquely the iteration=0 row.
    """
    fqn = _fqn(catalog, schema, "genie_opt_iterations")
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' "
        f"AND iteration = 0 AND eval_scope = 'full' "
        f"AND (rolled_back IS NULL OR rolled_back = false) "
        f"ORDER BY timestamp DESC LIMIT 1",
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    if row.get("scores_json") and isinstance(row["scores_json"], str):
        try:
            row["scores_json"] = json.loads(row["scores_json"])
        except (json.JSONDecodeError, TypeError):
            pass
    return row


def get_baseline_eval_state(
    spark: "SparkSession",
    *,
    run_id: str,
    catalog: str,
    schema: str,
    dbutils: Any = None,
) -> dict[str, HandoffValue]:
    """Read baseline eval state from ``genie_opt_iterations`` (Delta-only, D9).

    Reads the iteration=0 / ``eval_scope='full'`` row written by the baseline eval
    task (``02_baseline_eval_and_triage``). Returns ``HandoffValue`` for:
    ``scores``, ``overall_accuracy``, ``thresholds_met``, ``model_id``,
    ``mlflow_run_id``. ``dbutils`` is accepted for call-site compatibility but
    unused (D9 — no task values).

    Raises:
        RuntimeError: if there is no Delta iteration=0 baseline row — the baseline
            eval never completed.
    """
    del dbutils  # D9: Delta-only handoff — no task values are consulted.

    delta_query = (
        f"SELECT * FROM {catalog}.{schema}.genie_opt_iterations "
        f"WHERE run_id = '{run_id}' AND iteration = 0 "
        f"AND eval_scope = 'full' LIMIT 1"
    )

    delta_row = _load_baseline_iteration_row(spark, run_id, catalog, schema)

    if delta_row is None:
        raise RuntimeError(
            f"get_baseline_eval_state: no baseline state available for "
            f"run_id={run_id!r}. No row in "
            f"{catalog}.{schema}.genie_opt_iterations at iteration=0 "
            f"(eval_scope='full') — the baseline eval task "
            f"(02_baseline_eval_and_triage) must complete before optimize / publish."
        )

    out = {
        "scores": _resolve(
            delta_value=delta_row.get("scores_json"),
            key="scores", delta_query=delta_query,
        ),
        "overall_accuracy": _resolve(
            delta_value=delta_row.get("overall_accuracy"),
            key="overall_accuracy", delta_query=delta_query,
        ),
        "thresholds_met": _resolve(
            delta_value=delta_row.get("thresholds_met"),
            key="thresholds_met", delta_query=delta_query,
        ),
        "model_id": _resolve(
            delta_value=delta_row.get("model_id"),
            key="model_id", delta_query=delta_query,
        ),
        "mlflow_run_id": _resolve(
            delta_value=delta_row.get("mlflow_run_id"),
            key="mlflow_run_id", delta_query=delta_query,
        ),
    }
    return out


def _load_enrichment_iteration_row(
    spark: "SparkSession", run_id: str, catalog: str, schema: str,
) -> dict | None:
    """Latest eval_scope='enrichment' row for ``run_id``.

    Returns ``None`` if enrichment was skipped (no row written).
    """
    fqn = _fqn(catalog, schema, "genie_opt_iterations")
    df = run_query(
        spark,
        f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' "
        f"AND eval_scope = 'enrichment' "
        f"AND (rolled_back IS NULL OR rolled_back = false) "
        f"ORDER BY timestamp DESC LIMIT 1",
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    if row.get("scores_json") and isinstance(row["scores_json"], str):
        try:
            row["scores_json"] = json.loads(row["scores_json"])
        except (json.JSONDecodeError, TypeError):
            pass
    return row


def get_enrichment_state(
    spark: "SparkSession",
    *,
    run_id: str,
    catalog: str,
    schema: str,
    dbutils: Any = None,
) -> dict[str, HandoffValue]:
    """Read enrichment state from ``genie_opt_iterations`` (Delta-only, D9).

    Returns ``HandoffValue`` for: ``enrichment_model_id``,
    ``enrichment_skipped``, ``post_enrichment_accuracy``,
    ``post_enrichment_scores``, ``post_enrichment_model_id``,
    ``post_enrichment_thresholds_met``. ``dbutils`` is accepted for call-site
    compatibility but unused (D9 — no task values).

    Absence of a Delta enrichment row -> ``enrichment_skipped=True`` with
    source=DELTA_FALLBACK; all post_* values are MISSING. This is a valid
    state and does NOT raise.
    """
    del dbutils  # D9: Delta-only handoff — no task values are consulted.

    delta_query = (
        f"SELECT * FROM {catalog}.{schema}.genie_opt_iterations "
        f"WHERE run_id = '{run_id}' AND eval_scope = 'enrichment' LIMIT 1"
    )

    delta_row = _load_enrichment_iteration_row(spark, run_id, catalog, schema)
    skipped_val = delta_row is None
    skipped_hv = HandoffValue(
        key="enrichment_skipped", value=skipped_val,
        source=HandoffSource.DELTA_FALLBACK,
        delta_query=delta_query,
    )

    out: dict[str, HandoffValue] = {"enrichment_skipped": skipped_hv}

    out["enrichment_model_id"] = _resolve(
        delta_value=(delta_row or {}).get("model_id"),
        key="enrichment_model_id", delta_query=delta_query,
    )
    out["post_enrichment_accuracy"] = _resolve(
        delta_value=(delta_row or {}).get("overall_accuracy"),
        key="post_enrichment_accuracy", delta_query=delta_query,
    )
    out["post_enrichment_scores"] = _resolve(
        delta_value=(delta_row or {}).get("scores_json"),
        key="post_enrichment_scores", delta_query=delta_query,
    )
    out["post_enrichment_model_id"] = _resolve(
        delta_value=(delta_row or {}).get("model_id"),
        key="post_enrichment_model_id", delta_query=delta_query,
    )
    out["post_enrichment_thresholds_met"] = _resolve(
        delta_value=(delta_row or {}).get("thresholds_met"),
        key="post_enrichment_thresholds_met", delta_query=delta_query,
    )
    return out


from genie_space_optimizer.optimization.state import (
    load_iterations,
    load_latest_full_iteration,
    load_latest_state_iteration,
)


def get_lever_loop_outputs(
    spark: "SparkSession",
    *,
    run_id: str,
    catalog: str,
    schema: str,
    dbutils: Any = None,
) -> dict[str, HandoffValue]:
    """Read lever-loop / optimize outputs from Delta state (Delta-only, D9).

    Reconstructs the loop outputs from ``genie_opt_runs`` (best-* columns) and the
    latest ``eval_scope='full'`` ``genie_opt_iterations`` row. ``dbutils`` is
    accepted for call-site compatibility but unused (D9 — no task values).

    Raises:
        RuntimeError: if there is no run row AND no full-scope iteration row — the
            optimize task never completed.
    """
    del dbutils  # D9: Delta-only handoff — no task values are consulted.

    delta_query = (
        f"SELECT * FROM {catalog}.{schema}.genie_opt_iterations "
        f"WHERE run_id = '{run_id}' AND eval_scope = 'full' "
        f"ORDER BY iteration DESC LIMIT 1"
    )

    _run_row = load_run(spark, run_id, catalog, schema)
    _latest_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if _run_row is None and _latest_iter is None:
        raise RuntimeError(
            f"get_lever_loop_outputs: no state available for run_id="
            f"{run_id!r}. No rows in "
            f"{catalog}.{schema}.genie_opt_runs / genie_opt_iterations — the "
            f"optimize task (03_optimize) must complete before publish / deploy."
        )
    _iters_df = load_iterations(spark, run_id, catalog, schema)

    delta_skipped = (
        _latest_iter is not None and int(_latest_iter.get("iteration", 0)) == 0
    )

    delta_eval_ids: list[str] | None = None
    if _iters_df is not None and not _iters_df.empty:
        col = _iters_df.get("mlflow_run_id")
        if col is not None:
            delta_eval_ids = [
                x for x in col.dropna().tolist() if x
            ]

    out = {
        "scores": _resolve(
            delta_value=(_latest_iter or {}).get("scores_json"),
            key="scores", delta_query=delta_query,
        ),
        "accuracy": _resolve(
            delta_value=(_latest_iter or {}).get("overall_accuracy"),
            key="accuracy", delta_query=delta_query,
        ),
        "model_id": _resolve(
            delta_value=(_latest_iter or {}).get("model_id")
            or (_run_row or {}).get("best_model_id"),
            key="model_id", delta_query=delta_query,
        ),
        "iteration_counter": _resolve(
            delta_value=(_latest_iter or {}).get("iteration"),
            key="iteration_counter", delta_query=delta_query,
        ),
        "best_iteration": _resolve(
            delta_value=(_run_row or {}).get("best_iteration"),
            key="best_iteration", delta_query=delta_query,
        ),
        "skipped": _resolve(
            delta_value=delta_skipped if _latest_iter is not None else None,
            key="skipped", delta_query=delta_query,
        ),
        "all_eval_mlflow_run_ids": _resolve(
            delta_value=delta_eval_ids,
            key="all_eval_mlflow_run_ids", delta_query=delta_query,
        ),
        "all_failure_question_ids": _resolve(
            delta_value=(_latest_iter or {}).get("failures_json"),
            key="all_failure_question_ids", delta_query=delta_query,
        ),
    }
    return out


def select_finalize_skip_scores(
    *,
    lever_loop_scores: Any,
    baseline_scores: Any,
) -> Any:
    """Pick the scorecard finalize evaluates when the lever loop skipped.

    ``jobs/run_lever_loop.py`` publishes the *resolved* current scorecard
    (baseline OR post-enrichment) into the ``lever_loop.scores`` task
    value before exiting on its starting-point gate — see the skip path
    in that notebook. So when enrichment raised accuracy above thresholds
    and the loop skipped with ``post_enrichment_meets_thresholds``, the
    post-enrichment scorecard is what lives in ``lever_loop.scores``.

    Historically the finalize task read ``baseline_eval.scores`` whenever
    the loop was skipped. That leaked the *stale pre-enrichment*
    scorecard: ``_run_finalize`` then evaluated thresholds against numbers
    below target and finalized a genuinely CONVERGED run as STALLED
    (``no_further_improvement``). The lever_loop scores are authoritative
    whenever present; the baseline scorecard is only a degraded fallback
    for the recovery path where the skip never published scores.

    Args:
        lever_loop_scores: Scores published by ``lever_loop`` (dict) or a
            falsy value (``None`` / ``{}``) if the task value and Delta
            fallback were both empty.
        baseline_scores: Scores from ``baseline_eval`` (dict) or ``None``.

    Returns:
        The scorecard dict to feed ``_run_finalize`` (never ``None``).
    """
    if lever_loop_scores:
        return lever_loop_scores
    return baseline_scores or {}


def resolve_finalize_skip_scores(
    spark: "SparkSession",
    *,
    run_id: str,
    catalog: str,
    schema: str,
    lever_loop_scores: HandoffValue,
    baseline_scores: Any,
) -> Any:
    """Resolve the scorecard finalize scores on the lever-loop skip path.

    Resolution priority:

    1. **Authoritative published skip scorecard** — when
       ``lever_loop.scores`` came straight from task values, the lever-
       loop skip path already resolved it (baseline OR post-enrichment),
       so it is the source of truth.
    2. **Repair / Delta fallback** — when task values were lost,
       ``get_lever_loop_outputs`` resolves ``lever_loop.scores`` from
       ``load_latest_full_iteration`` (``eval_scope='full'``), which on a
       skip is the *stale baseline* row. That leaks the pre-enrichment
       scorecard for a ``post_enrichment_meets_thresholds`` skip and makes
       finalize stall. Re-resolve via ``load_latest_state_iteration``
       (``eval_scope IN ('full', 'enrichment')``, enrichment preferred so
       the newer iter-0 row wins) so the post-enrichment scorecard is
       chosen instead.
    3. **Baseline scorecard** — last resort when neither a published nor a
       Delta state scorecard exists.

    Args:
        spark: Active Spark session for the Delta read.
        run_id, catalog, schema: Locate the run's iteration state.
        lever_loop_scores: The ``lever_loop.scores`` HandoffValue (carries
            the ``HandoffSource`` so authoritative task values can be told
            apart from the stale full-row Delta fallback).
        baseline_scores: ``baseline_eval`` scores (dict) or ``None``.

    Returns:
        The scorecard dict to feed ``_run_finalize`` (never ``None``).
    """
    if (
        lever_loop_scores.source is HandoffSource.TASK_VALUES
        and lever_loop_scores.value
    ):
        return lever_loop_scores.value

    state_row = (
        load_latest_state_iteration(spark, run_id, catalog, schema) or {}
    )
    return select_finalize_skip_scores(
        lever_loop_scores=state_row.get("scores_json"),
        baseline_scores=baseline_scores,
    )


def assert_lever_loop_inputs_sane(state: dict[str, HandoffValue]) -> None:
    """Refuse to run the lever loop with degenerate baseline inputs.

    The fingerprint of a Repair Run whose baseline state never persisted is:
    ``overall_accuracy`` is 0.0/None AND ``scores`` is empty AND both
    are sourced from MISSING (Delta also empty). When this happens, the
    loop will silently terminate as ``plateau_no_open_failures`` and
    publish a misleading "Final accuracy: 0.0%" summary.

    This guard is loud-failure replaces silent-success: raise immediately
    with an actionable message instead.

    Args:
        state: dict of HandoffValue. Must contain
            ``overall_accuracy`` and ``scores``.

    Raises:
        RuntimeError: if inputs are degenerate AND not a direct/authoritative read.
    """
    acc_hv = state["overall_accuracy"]
    scores_hv = state["scores"]
    acc_val = acc_hv.value
    scores_val = scores_hv.value

    is_acc_empty = acc_val in (0.0, 0, None)
    is_scores_empty = (
        scores_val is None
        or (isinstance(scores_val, dict) and not scores_val)
    )

    if not (is_acc_empty and is_scores_empty):
        return

    both_real = (
        acc_hv.source is HandoffSource.TASK_VALUES
        and scores_hv.source is HandoffSource.TASK_VALUES
    )
    if both_real:
        return

    raise RuntimeError(
        f"assert_lever_loop_inputs_sane: degenerate baseline state "
        f"detected (overall_accuracy={acc_val!r} from {acc_hv.source.value}, "
        f"scores={scores_val!r} from {scores_hv.source.value}). "
        f"This is the Repair Run silent-success fingerprint: no baseline state "
        f"was persisted AND Delta has no row to fall back to. "
        f"Re-run the full DAG, or pass --override-baseline-from-delta "
        f"with a known-good run_id."
    )
