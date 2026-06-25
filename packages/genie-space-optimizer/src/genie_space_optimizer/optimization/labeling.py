"""Delta-backed flagging of questions for human review.

GSO v2 Phase 5 (D7): the MLflow Review App labeling session (custom label
schemas, ``create_labeling_session``, human-feedback ingestion, and
correction sync) was removed. Human review now relies on the Delta-backed
flagging below — questions GSO could not auto-resolve are recorded in
``genie_opt_flagged_questions`` so the Workbench can surface them — together
with the official Genie Benchmark API's ``manual_assessment`` / ``NEEDS_REVIEW``
signal (surfaced in Phases 1–3).

Public surface:
  - ``flag_for_human_review`` — record persistent/unresolved questions
  - ``resolve_stale_flags`` — clear flags for questions that now pass
  - ``get_flagged_questions`` — read flagged questions for a domain/run
"""

from __future__ import annotations

import logging
from typing import Any

from genie_space_optimizer.common.delta_helpers import execute_delta_write_with_retry

logger = logging.getLogger(__name__)


def _extract_question_id(request_val: Any) -> str:
    """Extract question_id from a trace's request field.

    Routes through the canonical helper at
    ``_qid_extraction.extract_question_id`` so this site cannot diverge from
    the other canonical-qid extractors. The canonical helper's ``request``
    branch handles both dict and JSON-string shapes, so we wrap ``request_val``
    into a minimal row.
    """
    if request_val is None:
        return ""
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    qid, _source = extract_question_id({"request": request_val})
    return qid


# ═══════════════════════════════════════════════════════════════════════
# Flag for Human Review (Delta-backed)
# ═══════════════════════════════════════════════════════════════════════


def _ensure_flagged_questions_table(spark: Any, catalog: str, schema: str) -> None:
    fqn = f"{catalog}.{schema}.genie_opt_flagged_questions"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fqn} (
                run_id              STRING      NOT NULL,
                domain              STRING      NOT NULL,
                question_id         STRING      NOT NULL,
                question_text       STRING,
                flag_reason         STRING,
                iterations_failed   INT,
                patches_tried       STRING,
                status              STRING      NOT NULL,
                flagged_at          TIMESTAMP   NOT NULL,
                resolved_at         TIMESTAMP
            ) USING DELTA
        """)
    except Exception:
        logger.debug("Flagged questions table already exists or creation failed", exc_info=True)


def flag_for_human_review(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    domain: str,
    items: list[dict],
) -> int:
    """Flag questions or patches for human review.

    Each item in *items* should have:
        - ``question_id``: str
        - ``question_text``: str
        - ``reason``: str (e.g. "ADDITIVE_LEVERS_EXHAUSTED", "low-confidence TVF removal")
        - ``iterations_failed``: int
        - ``patches_tried``: str (summary)

    Writes to ``genie_opt_flagged_questions`` Delta table.
    Returns the number of items flagged.
    """
    if not items:
        return 0

    fqn = f"{catalog}.{schema}.genie_opt_flagged_questions"
    _ensure_flagged_questions_table(spark, catalog, schema)

    flagged = 0
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    for item in items:
        qid = item.get("question_id", "")
        if not qid:
            continue
        q_text = (item.get("question_text") or "")[:500]
        reason = (item.get("reason") or "")[:500]
        iters = item.get("iterations_failed", 0)
        patches = (item.get("patches_tried") or "")[:1000]

        try:
            _q_text_esc = q_text.replace("'", "''")
            _reason_esc = reason.replace("'", "''")
            _patches_esc = patches.replace("'", "''")
            merge_stmt = f"""
                MERGE INTO {fqn} AS t
                USING (SELECT '{run_id}' AS run_id, '{domain}' AS domain,
                              '{qid}' AS question_id) AS s
                ON t.question_id = s.question_id AND t.domain = s.domain
                   AND t.status = 'pending'
                WHEN MATCHED THEN UPDATE SET
                    t.run_id = s.run_id,
                    t.flag_reason = '{_reason_esc}',
                    t.iterations_failed = {iters},
                    t.patches_tried = '{_patches_esc}',
                    t.flagged_at = '{now}'
                WHEN NOT MATCHED THEN INSERT (
                    run_id, domain, question_id, question_text, flag_reason,
                    iterations_failed, patches_tried, status, flagged_at
                ) VALUES (
                    s.run_id, s.domain, s.question_id, '{_q_text_esc}',
                    '{_reason_esc}', {iters}, '{_patches_esc}', 'pending', '{now}'
                )
            """
            execute_delta_write_with_retry(
                spark,
                merge_stmt,
                operation_name="flag_for_human_review",
                table_name=fqn,
            )
            flagged += 1
        except Exception:
            logger.warning("Failed to flag question %s", qid, exc_info=True)

    logger.info("Flagged %d questions for human review (run=%s)", flagged, run_id)
    return flagged


def resolve_stale_flags(
    spark: Any,
    catalog: str,
    schema: str,
    domain: str,
    passing_question_ids: set[str],
) -> int:
    """Mark flags as resolved for questions that now pass.

    Finds all ``status='pending'`` flags for the domain whose question_id
    is in *passing_question_ids* and sets ``status='resolved'``.

    Returns the number of flags resolved.
    """
    if not passing_question_ids:
        return 0

    _ensure_flagged_questions_table(spark, catalog, schema)
    fqn = f"{catalog}.{schema}.genie_opt_flagged_questions"

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    resolved = 0
    for qid in passing_question_ids:
        try:
            spark.sql(
                f"UPDATE {fqn} SET status = 'resolved', flagged_at = '{now}' "
                f"WHERE question_id = '{qid}' AND domain = '{domain}' AND status = 'pending'"
            )
            resolved += 1
        except Exception:
            logger.debug("Could not resolve flag for %s", qid, exc_info=True)

    if resolved:
        logger.info(
            "Resolved %d stale flag(s) for domain %s (questions now passing)",
            resolved, domain,
        )
    return resolved


def get_flagged_questions(
    spark: Any,
    catalog: str,
    schema: str,
    domain: str,
    *,
    status: str = "pending",
    run_id: str = "",
) -> list[dict]:
    """Return flagged questions for a domain with the given status.

    When *run_id* is provided, only returns flags from that specific run.
    """
    from genie_space_optimizer.optimization.state import run_query

    _ensure_flagged_questions_table(spark, catalog, schema)
    fqn = f"{catalog}.{schema}.genie_opt_flagged_questions"
    try:
        where = f"WHERE domain = '{domain}' AND status = '{status}'"
        if run_id:
            where += f" AND run_id = '{run_id}'"
        df = run_query(
            spark,
            f"SELECT * FROM {fqn} {where}",
        )
        return df.to_dict("records") if not df.empty else []
    except Exception:
        logger.debug("Could not read flagged questions table", exc_info=True)
        return []
