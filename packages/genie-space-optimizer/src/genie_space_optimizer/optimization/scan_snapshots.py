"""Persistence for IQ Scan snapshots linked to optimizer runs.

Snapshots are captured at two phases:

- ``phase='preflight'`` — before the lever loop starts, right after
  :func:`preflight_run_iq_scan` executes.
- ``phase='postflight'`` — after the lever loop reaches a terminal status
  (CONVERGED / STALLED / MAX_ITERATIONS).

The table is additive and idempotent: the writer uses ``MERGE`` keyed on
``(run_id, phase)`` so job retries don't duplicate rows. The primary consumer
is the run-detail delta view which diffs the two rows for a single run.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from genie_space_optimizer.common.config import TABLE_SCAN_SNAPSHOTS

logger = logging.getLogger(__name__)


def _iq_scan_postflight_enabled() -> bool:
    """Return True when the postflight IQ Scan hook is enabled via env var.

    Shares ``GSO_ENABLE_IQ_SCAN_PREFLIGHT`` with the preflight sub-step so both
    halves of the pre/post pair flip on and off together — a postflight row
    without a matching preflight row would break the delta view.
    """
    import os as _os
    return _os.getenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "false").lower() in {
        "1", "true", "yes", "on",
    }


def _ensure_scan_snapshot_table(spark: Any, catalog: str, schema: str) -> None:
    """Idempotently create the ``genie_opt_scan_snapshots`` UC Delta table."""
    fqn = f"{catalog}.{schema}.{TABLE_SCAN_SNAPSHOTS}"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fqn} (
                run_id          STRING      NOT NULL,
                space_id        STRING      NOT NULL,
                phase           STRING      NOT NULL,
                score           INT,
                total           INT,
                maturity        STRING,
                checks_json     STRING,
                findings_json   STRING,
                warnings_json   STRING,
                scanned_at      TIMESTAMP   NOT NULL
            ) USING DELTA
        """)
    except Exception:
        logger.debug("Scan snapshot table already exists or creation failed", exc_info=True)


def _jsonify(value: Any) -> str:
    """Serialize *value* to JSON; return ``'null'`` on failure so SQL stays valid."""
    try:
        return json.dumps(value, default=str)
    except Exception:
        logger.debug("Failed to serialize scan snapshot field", exc_info=True)
        return "null"


def write_scan_snapshot(
    spark: Any,
    run_id: str,
    space_id: str,
    phase: str,
    scan_result: dict,
    catalog: str,
    schema: str,
) -> bool:
    """Persist (or update) the IQ Scan snapshot for a given run and phase.

    Uses ``MERGE INTO`` keyed on ``(run_id, phase)`` so re-runs of the same
    phase — which happen when the harness retries preflight or when the
    postflight hook fires twice — produce exactly one row per
    ``(run_id, phase)`` pair.

    Returns ``True`` on successful persist, ``False`` if the write failed.
    Callers should soft-fail: persistence is a diagnostic, not a correctness
    gate.
    """
    if phase not in {"preflight", "postflight"}:
        raise ValueError(f"phase must be 'preflight' or 'postflight', got {phase!r}")
    if not run_id or not space_id:
        raise ValueError("run_id and space_id are required")

    fqn = f"{catalog}.{schema}.{TABLE_SCAN_SNAPSHOTS}"
    _ensure_scan_snapshot_table(spark, catalog, schema)

    score = scan_result.get("score")
    total = scan_result.get("total")
    maturity = scan_result.get("maturity") or ""
    checks = scan_result.get("checks") or []
    findings = scan_result.get("findings") or []
    warnings = scan_result.get("warnings") or []

    scanned_at = scan_result.get("scanned_at") or datetime.now(timezone.utc).isoformat()

    checks_json = _jsonify(checks).replace("'", "''")
    findings_json = _jsonify(findings).replace("'", "''")
    warnings_json = _jsonify(warnings).replace("'", "''")
    maturity_esc = maturity.replace("'", "''")

    score_sql = "NULL" if score is None else str(int(score))
    total_sql = "NULL" if total is None else str(int(total))

    try:
        spark.sql(f"""
            MERGE INTO {fqn} AS t
            USING (
                SELECT '{run_id}' AS run_id,
                       '{space_id}' AS space_id,
                       '{phase}' AS phase
            ) AS s
            ON t.run_id = s.run_id AND t.phase = s.phase
            WHEN MATCHED THEN UPDATE SET
                t.space_id      = s.space_id,
                t.score         = {score_sql},
                t.total         = {total_sql},
                t.maturity      = '{maturity_esc}',
                t.checks_json   = '{checks_json}',
                t.findings_json = '{findings_json}',
                t.warnings_json = '{warnings_json}',
                t.scanned_at    = '{scanned_at}'
            WHEN NOT MATCHED THEN INSERT (
                run_id, space_id, phase, score, total, maturity,
                checks_json, findings_json, warnings_json, scanned_at
            ) VALUES (
                s.run_id, s.space_id, s.phase, {score_sql}, {total_sql}, '{maturity_esc}',
                '{checks_json}', '{findings_json}', '{warnings_json}', '{scanned_at}'
            )
        """)
        logger.info(
            "Wrote IQ scan snapshot run=%s space=%s phase=%s score=%s/%s",
            run_id, space_id, phase, score, total,
        )
        return True
    except Exception:
        logger.warning(
            "Failed to persist IQ scan snapshot run=%s phase=%s",
            run_id, phase, exc_info=True,
        )
        return False


def run_postflight_scan(
    w: Any,
    spark: Any,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    *,
    best_accuracy: float | None = None,
) -> bool:
    """Post-convergence IQ Scan hook; writes the ``phase='postflight'`` row.

    Soft-failing by design — every exception path is caught and logged so a
    failed scan never blocks the terminal status write that follows. Returns
    ``True`` when the snapshot row was persisted, ``False`` otherwise (flag
    off, fetch failed, scoring failed, or write failed).

    Shares the preflight feature flag (``GSO_ENABLE_IQ_SCAN_PREFLIGHT``): we
    only persist a ``postflight`` row when we know a ``preflight`` row was
    written for the same run, so the run-detail delta view always sees a
    matched pair.
    """
    if not _iq_scan_postflight_enabled():
        return False

    try:
        from genie_space_optimizer.common.genie_client import fetch_space_config
        from genie_space_optimizer.iq_scan.scoring import calculate_score

        post_config = fetch_space_config(w, space_id)
        parsed = (
            post_config.get("_parsed_space", post_config)
            if isinstance(post_config, dict)
            else {}
        )
        optimization_run: dict[str, Any] | None = None
        if best_accuracy is not None:
            optimization_run = {"accuracy": float(best_accuracy)}
        post_scan = calculate_score(parsed or {}, optimization_run=optimization_run)
        return write_scan_snapshot(
            spark, run_id, space_id, "postflight", post_scan, catalog, schema,
        )
    except Exception:
        logger.warning(
            "Postflight IQ scan failed run=%s space=%s — continuing without delta",
            run_id, space_id, exc_info=True,
        )
        return False
