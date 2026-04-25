"""Catalog-level metric-view detection.

Runs ``DESCRIBE TABLE EXTENDED ... AS JSON`` against a list of UC table
refs and classifies each ref as a metric view (or not) based on whether
its ``view_text`` payload parses as metric-view YAML (a ``source`` plus
``dimensions`` and/or ``measures``).

Lives in ``common`` rather than ``optimization.preflight`` so the same
helper can be invoked by every stage that needs the answer (preflight,
enrichment, follow-up refreshes) without dragging the entire preflight
module into harness's import graph.

Detection is *permissive* — false negatives only, never false positives.
A failed DESCRIBE, a non-JSON envelope, or a YAML that doesn't match the
metric-view shape silently treats the ref as a non-MV so the regular
table-profile path remains correct for real tables.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def detect_metric_views_via_catalog(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
    *,
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
    exec_sql: Any = None,
) -> tuple[set[str], dict[str, dict]]:
    """Catalog-level metric-view detection.

    For each ``(catalog, schema, name)`` triple, runs ``DESCRIBE TABLE
    EXTENDED <fq> AS JSON`` and parses the JSON envelope. A ref is
    classified as a metric view when the response contains a
    ``view_text`` (or equivalent field) whose YAML payload has the
    metric-view top-level shape — a ``source`` plus at least one of
    ``dimensions`` / ``measures``.

    Returns ``(detected, yamls)`` where:

    * ``detected`` is a set of fully-qualified, lower-cased identifiers
      for refs classified as MVs.
    * ``yamls`` maps each detected identifier to its parsed YAML dict so
      downstream callers (MV-aware data profiling, prompt building, the
      MEASURE auto-wrap rewriter) can inspect dimensions and measures
      without re-running DESCRIBE.

    The optional ``exec_sql`` lets tests inject a stub; production
    callers leave it ``None`` and the helper resolves the canonical
    ``evaluation._exec_sql`` lazily.
    """
    import json as _json

    import yaml as _yaml

    if exec_sql is None:
        from genie_space_optimizer.optimization.evaluation import _exec_sql as _exec
    else:
        _exec = exec_sql

    detected: set[str] = set()
    yamls: dict[str, dict] = {}

    for cat, sch, name in refs:
        cat = (cat or "").strip()
        sch = (sch or "").strip()
        name = (name or "").strip()
        if not (cat and sch and name):
            continue
        fq_lower = f"{cat}.{sch}.{name}".lower()
        fq_quoted = ".".join(f"`{p}`" for p in (cat, sch, name))

        try:
            describe_df = _exec(
                f"DESCRIBE TABLE EXTENDED {fq_quoted} AS JSON",
                spark,
                w=w,
                warehouse_id=warehouse_id,
                catalog=catalog,
                schema=schema,
            )
        except Exception:
            logger.debug(
                "MV catalog detection: DESCRIBE failed for %s, treating as non-MV",
                fq_lower,
                exc_info=True,
            )
            continue

        if describe_df is None or describe_df.empty:
            continue

        # The JSON payload may live under different column names depending on
        # whether the warehouse path or the Spark path returned the row;
        # search the row's values for the first parseable JSON envelope.
        envelope: dict[str, Any] | None = None
        for value in describe_df.iloc[0].tolist():
            if not isinstance(value, str):
                continue
            try:
                parsed = _json.loads(value)
            except (ValueError, TypeError):
                continue
            if isinstance(parsed, dict):
                envelope = parsed
                break
        if envelope is None:
            continue

        view_text = (
            envelope.get("view_text")
            or envelope.get("View Text")
            or envelope.get("view_definition")
            or envelope.get("ViewText")
            or ""
        )
        if not isinstance(view_text, str) or not view_text.strip():
            continue

        try:
            yaml_doc = _yaml.safe_load(view_text)
        except Exception:
            logger.debug(
                "MV catalog detection: YAML parse failed for %s", fq_lower,
                exc_info=True,
            )
            continue

        if not isinstance(yaml_doc, dict):
            continue
        if not yaml_doc.get("source"):
            continue
        if not (yaml_doc.get("dimensions") or yaml_doc.get("measures")):
            continue

        detected.add(fq_lower)
        yamls[fq_lower] = yaml_doc

    return detected, yamls
