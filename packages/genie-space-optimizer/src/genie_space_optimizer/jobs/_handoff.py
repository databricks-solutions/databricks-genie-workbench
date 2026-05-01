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
