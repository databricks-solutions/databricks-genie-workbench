"""Cross-task state resilience helpers for the GSO 6-task DAG.

Every notebook task in the optimization DAG reads upstream state via
``dbutils.jobs.taskValues.get(...)``. On a Repair Run, taskValues from
prior runs are NOT propagated, so each call returns the empty default
and the task silently runs on degenerate inputs.

This module wraps every cross-task read with a 3-step lookup:

  1. taskValues first (the happy path).
  2. Delta fallback against the durable state already persisted in
     ``genie_opt_runs`` and ``genie_opt_iterations``.
  3. Loud failure (or documented default) if neither exists.

Each value carries the ``HandoffSource`` it came from so log readers can
distinguish "the run resumed correctly" from "the run silently fell
back to Delta state". See the cross-task state resilience plan
(``docs/2026-05-01-cross-task-state-resilience-plan.md``) for the
problem statement and design.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class HandoffSource(str, Enum):
    """Where a HandoffValue was sourced from."""

    TASK_VALUES = "task_values"       # taskValues returned a real value
    DELTA_FALLBACK = "delta_fallback"  # taskValues empty; Delta filled in
    DEFAULT = "default"               # both empty; documented default applied
    MISSING = "missing"               # both empty; no default — caller decides


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


def _tv_get(dbutils, taskKey: str, key: str, default: str = "") -> str:
    """Tiny wrapper so tests can swap in a mock and the prod call site
    does not duplicate ``dbutils.jobs.taskValues.get`` boilerplate."""
    return dbutils.jobs.taskValues.get(taskKey=taskKey, key=key, default=default)


def _resolve(
    raw_tv: str,
    *,
    delta_value,
    parser=lambda s: s,
    key: str,
    delta_query: Optional[str] = None,
) -> HandoffValue:
    """Pick taskValues if non-empty, else Delta if non-None, else MISSING.

    ``parser`` converts the raw string to its typed value; on parse error
    the string is treated as empty so Delta wins.
    """
    if raw_tv not in ("", None):
        try:
            return HandoffValue(
                key=key, value=parser(raw_tv),
                source=HandoffSource.TASK_VALUES,
            )
        except (ValueError, json.JSONDecodeError, TypeError):
            pass  # fall through to Delta
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
    dbutils,
) -> dict[str, HandoffValue]:
    """Read all preflight-published values, with Delta fallback to genie_opt_runs.

    The widgets ``run_id_widget``, ``catalog_widget``, ``schema_widget``
    are required because they are the only values we can rely on at the
    very start of any task — Databricks Jobs widgets DO survive Repair
    Run, while taskValues do not.

    Returns a dict of ``HandoffValue`` keyed by logical name.

    Raises:
        RuntimeError: if ``run_id_widget`` is empty AND no taskValue ran_id
            is available — there is nothing to look up in Delta.
    """
    if not run_id_widget:
        raise RuntimeError(
            "get_run_context: run_id_widget is required (Databricks Jobs "
            "widget). Pass dbutils.widgets.get('run_id') from the notebook."
        )

    # Try taskValues first for the bootstrap key
    tv_run_id = _tv_get(dbutils, "preflight", "run_id")
    bootstrap_run_id = tv_run_id or run_id_widget

    delta_query = (
        f"SELECT * FROM {catalog_widget}.{schema_widget}.genie_opt_runs "
        f"WHERE run_id = '{bootstrap_run_id}' LIMIT 1"
    )
    run_row = load_run(spark, bootstrap_run_id, catalog_widget, schema_widget)

    if run_row is None and not tv_run_id:
        # Both taskValues and Delta empty — we know nothing about this run.
        raise RuntimeError(
            f"get_run_context: no run context available for run_id="
            f"{bootstrap_run_id!r}. taskValues are empty AND no row in "
            f"{catalog_widget}.{schema_widget}.genie_opt_runs. Repair "
            f"Run cannot proceed."
        )

    # Per-key resolution. Each entry: (logical_key, taskValues_key,
    # delta_row_key, parser).
    spec = [
        ("run_id", "run_id", "run_id", str),
        ("space_id", "space_id", "space_id", str),
        ("domain", "domain", "domain", str),
        ("experiment_name", "experiment_name", "experiment_name", str),
        ("apply_mode", "apply_mode", "apply_mode", str),
        ("triggered_by", "triggered_by", "triggered_by", str),
        ("warehouse_id", "warehouse_id", "warehouse_id", str),
        ("max_iterations", "max_iterations", "max_iterations", int),
        ("levers", "levers", "levers", json.loads),
        (
            "max_benchmark_count",
            "max_benchmark_count",
            "max_benchmark_count",
            int,
        ),
        (
            "human_corrections",
            "human_corrections",
            "human_corrections_json",
            json.loads,
        ),
    ]

    out: dict[str, HandoffValue] = {}
    for logical, tv_key, delta_key, parser in spec:
        raw = _tv_get(dbutils, "preflight", tv_key)
        delta_val = (run_row or {}).get(delta_key)
        # JSON columns (levers, human_corrections_json) come back from
        # load_run as strings. Parse them here so callers always see a
        # typed value regardless of source.
        if logical in ("levers", "human_corrections") and isinstance(delta_val, str):
            try:
                delta_val = json.loads(delta_val)
            except (ValueError, TypeError):
                delta_val = None
        out[logical] = _resolve(
            raw,
            delta_value=delta_val,
            parser=parser,
            key=logical,
            delta_query=delta_query,
        )

    # ``catalog`` and ``schema`` are passed in as widgets, never read from
    # taskValues — they ARE the bootstrap keys.
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
    dbutils,
) -> dict[str, HandoffValue]:
    """Read baseline_eval task values, falling back to genie_opt_iterations.

    Returns ``HandoffValue`` for: ``scores``, ``overall_accuracy``,
    ``thresholds_met``, ``model_id``, ``mlflow_run_id``.

    Raises:
        RuntimeError: if neither taskValues nor a Delta iteration=0 row
            is available — the baseline never ran.
    """
    delta_query = (
        f"SELECT * FROM {catalog}.{schema}.genie_opt_iterations "
        f"WHERE run_id = '{run_id}' AND iteration = 0 "
        f"AND eval_scope = 'full' LIMIT 1"
    )

    raw_scores = _tv_get(dbutils, "baseline_eval", "scores")
    raw_acc = _tv_get(dbutils, "baseline_eval", "overall_accuracy")
    raw_thr = _tv_get(dbutils, "baseline_eval", "thresholds_met")
    raw_mid = _tv_get(dbutils, "baseline_eval", "model_id")
    raw_mlid = _tv_get(dbutils, "baseline_eval", "mlflow_run_id")

    delta_row = None
    if raw_scores in ("", None) or raw_acc in ("", None):
        delta_row = _load_baseline_iteration_row(
            spark, run_id, catalog, schema,
        )

    if (
        raw_scores in ("", None)
        and raw_acc in ("", None)
        and delta_row is None
    ):
        raise RuntimeError(
            f"get_baseline_eval_state: no baseline state available for "
            f"run_id={run_id!r}. taskValues empty AND no row in "
            f"{catalog}.{schema}.genie_opt_iterations at iteration=0. "
            f"baseline_eval task must complete before lever_loop / finalize."
        )

    def _bool(s: str) -> bool:
        return str(s).lower() in ("true", "1")

    out = {
        "scores": _resolve(
            raw_scores,
            delta_value=(delta_row or {}).get("scores_json"),
            parser=json.loads,
            key="scores", delta_query=delta_query,
        ),
        "overall_accuracy": _resolve(
            raw_acc,
            delta_value=(delta_row or {}).get("overall_accuracy"),
            parser=float,
            key="overall_accuracy", delta_query=delta_query,
        ),
        "thresholds_met": _resolve(
            raw_thr,
            delta_value=(delta_row or {}).get("thresholds_met"),
            parser=_bool,
            key="thresholds_met", delta_query=delta_query,
        ),
        "model_id": _resolve(
            raw_mid,
            delta_value=(delta_row or {}).get("model_id"),
            parser=str,
            key="model_id", delta_query=delta_query,
        ),
        "mlflow_run_id": _resolve(
            raw_mlid,
            delta_value=(delta_row or {}).get("mlflow_run_id"),
            parser=str,
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
    dbutils,
) -> dict[str, HandoffValue]:
    """Read enrichment task values, falling back to genie_opt_iterations.

    Returns ``HandoffValue`` for: ``enrichment_model_id``,
    ``enrichment_skipped``, ``post_enrichment_accuracy``,
    ``post_enrichment_scores``, ``post_enrichment_model_id``,
    ``post_enrichment_thresholds_met``.

    Absence of a Delta enrichment row -> ``enrichment_skipped=True`` with
    source=DELTA_FALLBACK; all post_* values are MISSING. This is a valid
    state and does NOT raise.
    """
    delta_query = (
        f"SELECT * FROM {catalog}.{schema}.genie_opt_iterations "
        f"WHERE run_id = '{run_id}' AND eval_scope = 'enrichment' LIMIT 1"
    )

    raw_skipped = _tv_get(dbutils, "enrichment", "enrichment_skipped")
    if raw_skipped not in ("", None):
        # Operator-supplied skip signal -- trust it.
        skipped_val = str(raw_skipped).lower() in ("true", "1")
        skipped_hv = HandoffValue(
            key="enrichment_skipped", value=skipped_val,
            source=HandoffSource.TASK_VALUES,
        )
        delta_row = None
    else:
        delta_row = _load_enrichment_iteration_row(
            spark, run_id, catalog, schema,
        )
        skipped_val = delta_row is None
        skipped_hv = HandoffValue(
            key="enrichment_skipped", value=skipped_val,
            source=HandoffSource.DELTA_FALLBACK,
            delta_query=delta_query,
        )

    def _bool(s: str) -> bool:
        return str(s).lower() in ("true", "1")

    out: dict[str, HandoffValue] = {"enrichment_skipped": skipped_hv}

    out["enrichment_model_id"] = _resolve(
        _tv_get(dbutils, "enrichment", "enrichment_model_id"),
        delta_value=(delta_row or {}).get("model_id"),
        parser=str,
        key="enrichment_model_id", delta_query=delta_query,
    )
    out["post_enrichment_accuracy"] = _resolve(
        _tv_get(dbutils, "enrichment", "post_enrichment_accuracy"),
        delta_value=(delta_row or {}).get("overall_accuracy"),
        parser=float,
        key="post_enrichment_accuracy", delta_query=delta_query,
    )
    out["post_enrichment_scores"] = _resolve(
        _tv_get(dbutils, "enrichment", "post_enrichment_scores"),
        delta_value=(delta_row or {}).get("scores_json"),
        parser=json.loads,
        key="post_enrichment_scores", delta_query=delta_query,
    )
    out["post_enrichment_model_id"] = _resolve(
        _tv_get(dbutils, "enrichment", "post_enrichment_model_id"),
        delta_value=(delta_row or {}).get("model_id"),
        parser=str,
        key="post_enrichment_model_id", delta_query=delta_query,
    )
    out["post_enrichment_thresholds_met"] = _resolve(
        _tv_get(dbutils, "enrichment", "post_enrichment_thresholds_met"),
        delta_value=(delta_row or {}).get("thresholds_met"),
        parser=_bool,
        key="post_enrichment_thresholds_met", delta_query=delta_query,
    )
    return out
