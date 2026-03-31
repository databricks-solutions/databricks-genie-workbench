"""MLflow Labeling Sessions for human-in-the-loop optimization review.

Provides:
  - Custom labeling schemas for Genie Space optimization
  - Auto-creation of review sessions after lever loop runs
  - Ingestion of human feedback to improve subsequent optimization runs

Uses the MLflow 3.x labeling API:
  - mlflow.genai.labeling.create_labeling_session()
  - session.add_dataset() / session.add_traces()
  - session.sync(to_dataset=...)
  - mlflow.genai.labeling.get_labeling_sessions()
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any

import mlflow

logger = logging.getLogger(__name__)

SCHEMA_JUDGE_VERDICT = "judge_verdict_accuracy"
SCHEMA_CORRECTED_SQL = "corrected_expected_sql"
SCHEMA_PATCH_APPROVAL = "patch_approval"
SCHEMA_IMPROVEMENTS = "improvement_suggestions"

ALL_SCHEMA_NAMES = [
    SCHEMA_JUDGE_VERDICT,
    SCHEMA_CORRECTED_SQL,
    SCHEMA_IMPROVEMENTS,
]


def ensure_labeling_schemas() -> list[str]:
    """Create or reuse custom labeling schemas for Genie optimization review.

    Returns the list of schema names that are available (created or pre-existing).
    Gracefully degrades if the MLflow version doesn't support labeling.
    """
    try:
        import mlflow.genai.label_schemas as schemas
        from mlflow.genai.label_schemas import (
            InputCategorical,
            InputText,
            InputTextList,
        )
    except ImportError:
        print("[Labeling] mlflow.genai.label_schemas not available — skipping")
        return []

    available: list[str] = []
    _schema_defs: list[dict] = [
        {
            "name": SCHEMA_JUDGE_VERDICT,
            "type": "feedback",
            "title": "Is this question truly failing?",
            "input": InputCategorical(options=[
                "Yes - Genie answer is wrong",
                "No - Genie answer is actually correct",
                "Benchmark is wrong - expected SQL needs fixing",
                "Ambiguous - question is unclear",
            ]),
            "instruction": (
                "Compare the generated SQL response with the expected SQL shown in the "
                "trace inputs. Review the comparison result (match/mismatch, column "
                "differences). Is the Genie response actually incorrect, or is the "
                "benchmark expectation wrong?"
            ),
            "enable_comment": True,
        },
        {
            "name": SCHEMA_CORRECTED_SQL,
            "type": "expectation",
            "title": "Provide the correct expected SQL (if benchmark is wrong)",
            "input": InputText(),
            "instruction": (
                "If the benchmark's expected SQL is incorrect or suboptimal, "
                "provide the corrected SQL. Leave blank if the benchmark is correct."
            ),
        },
        {
            "name": SCHEMA_IMPROVEMENTS,
            "type": "expectation",
            "title": "Suggest improvements for this Genie Space",
            "input": InputTextList(max_count=5, max_length_each=500),
            "instruction": (
                "Provide specific, actionable improvements using the format:\n"
                "  [type] target: suggestion\n"
                "\n"
                "Supported types:\n"
                "  [column] table.column: description of what the column means\n"
                "  [rule] question or topic: business rule Genie should follow\n"
                "  [join] tableA JOIN tableB: correct join condition\n"
                "  [instruction] topic: instruction Genie should follow\n"
                "  [general] suggestion text (no target needed)\n"
                "\n"
                "Examples:\n"
                "  [column] orders.revenue: Net revenue after returns and discounts\n"
                "  [rule] What is total revenue?: Use net_revenue, not gross_revenue\n"
                "  [join] orders JOIN customers: Join on customer_id, not name\n"
                "  [instruction] date handling: Fiscal year starts April 1"
            ),
        },
    ]

    for defn in _schema_defs:
        name = defn["name"]
        try:
            schemas.create_label_schema(**defn, overwrite=True)
            available.append(name)
        except Exception as exc:
            try:
                existing = schemas.get_label_schema(name)
                if existing is not None:
                    available.append(name)
                    print(f"[Labeling] Schema '{name}' exists (referenced by session) — reusing")
                else:
                    print(f"[Labeling] Failed to create schema '{name}': {exc}")
                    logger.warning("Failed to create label schema '%s': %s", name, exc)
            except Exception:
                print(f"[Labeling] Failed to create schema '{name}': {exc}")
                logger.warning("Failed to create label schema '%s': %s", name, exc)

    print(f"[Labeling] Ensured {len(available)}/{len(_schema_defs)} schemas: {', '.join(available)}")
    return available


def create_review_session(
    run_id: str,
    domain: str,
    experiment_name: str,
    uc_schema: str,
    failure_trace_ids: list[str],
    regression_trace_ids: list[str],
    reviewers: list[str] | None = None,
    eval_mlflow_run_ids: list[str] | None = None,
    failure_question_ids: list[str] | None = None,
    flagged_trace_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Create a labeling session populated with evaluation traces for human review.

    Uses *eval_mlflow_run_ids* (the MLflow run IDs from each evaluation)
    as the primary source of traces.  This is far more reliable than
    searching the whole experiment and filtering by individual trace IDs,
    which can fail due to format mismatches, indexing lag, or the 200-trace
    cap on ``mlflow.search_traces()``.

    Falls back to experiment-wide search when no eval run IDs are provided.

    When *flagged_trace_ids* is provided, those traces are prioritized first
    in the session (before regressions and other failures).

    Returns dict with session_name, session_run_id, session_url,
    trace_count (or empty on failure).
    """
    try:
        import mlflow.genai.labeling as labeling
    except ImportError:
        print("[Labeling] mlflow.genai.labeling not available — skipping session creation")
        return {}

    schema_names = ensure_labeling_schemas()
    if not schema_names:
        print("[Labeling] No schemas available — cannot create labeling session")
        return {}

    mlflow.set_experiment(experiment_name)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{run_id[:8]}_{ts}"
    prefix = f"opt_{domain}"
    max_prefix = 64 - len(suffix)
    if len(prefix) > max_prefix:
        prefix = prefix[:max_prefix]
    session_name = f"{prefix}{suffix}"

    print(
        f"[Labeling] Creating session '{session_name}' in experiment '{experiment_name}' "
        f"({len(failure_trace_ids)} failures, {len(regression_trace_ids)} regressions, "
        f"{len(eval_mlflow_run_ids or [])} eval runs)"
    )

    try:
        session = labeling.create_labeling_session(
            name=session_name,
            label_schemas=schema_names,
            assigned_users=reviewers or [],
        )
    except Exception as exc:
        print(f"[Labeling] Failed to create session: {exc}")
        logger.exception("Failed to create labeling session")
        return {}

    traces_added = 0
    try:
        traces_added = _populate_session_traces(
            session=session,
            session_name=session_name,
            experiment_name=experiment_name,
            failure_trace_ids=failure_trace_ids,
            regression_trace_ids=regression_trace_ids,
            eval_mlflow_run_ids=eval_mlflow_run_ids or [],
            flagged_trace_ids=flagged_trace_ids,
            failure_question_ids=failure_question_ids,
        )
    except Exception as exc:
        print(f"[Labeling] Failed to add traces to session: {exc}")
        logger.exception("Failed to add traces to labeling session")

    session_run_id = getattr(session, "mlflow_run_id", "")
    session_id = getattr(session, "labeling_session_id", "")
    session_url = getattr(session, "url", "")

    logger.info(
        "Created labeling session: %s (run_id=%s, session_id=%s, traces=%d, url=%s)",
        session_name, session_run_id, session_id, traces_added,
        session_url or "(none)",
    )
    return {
        "session_name": session_name,
        "session_run_id": session_run_id,
        "labeling_session_id": session_id,
        "session_url": session_url,
        "trace_count": traces_added,
    }


_MAX_SESSION_TRACES = 100


def _extract_question_id(request_val: Any) -> str:
    """Extract question_id from a trace's request field."""
    if not request_val:
        return ""
    try:
        req = json.loads(request_val) if isinstance(request_val, str) else request_val
        if isinstance(req, dict):
            return str(
                req.get("question_id")
                or req.get("kwargs", {}).get("question_id", "")
            )
    except Exception:
        pass
    return ""


def _populate_session_traces(
    *,
    session: Any,
    session_name: str,
    experiment_name: str,
    failure_trace_ids: list[str],
    regression_trace_ids: list[str],
    eval_mlflow_run_ids: list[str],
    flagged_trace_ids: list[str] | None = None,
    failure_question_ids: list[str] | None = None,
) -> int:
    """Collect traces from eval runs and add them to the labeling session.

    Strategy:
      1. If *eval_mlflow_run_ids* are available, search traces per eval run.
         This is the reliable path — each eval run's traces are directly
         addressable by ``mlflow.search_traces(run_id=...)``.
      2. Fall back to experiment-wide search (legacy path) when no run IDs
         are provided.

    When *failure_question_ids* is provided, only traces matching those
    question IDs are included (no backfill with passing traces).  When not
    provided, falls back to priority/backfill behavior.
    """
    import pandas as pd

    all_traces: pd.DataFrame | None = None

    if eval_mlflow_run_ids:
        frames: list[pd.DataFrame] = []
        unique_run_ids = list(dict.fromkeys(eval_mlflow_run_ids))
        for rid in unique_run_ids:
            try:
                run_traces = mlflow.search_traces(run_id=rid)
                if run_traces is not None and len(run_traces) > 0:
                    logger.info("Found %d traces in eval run %s", len(run_traces), rid)
                    frames.append(run_traces)
            except Exception as exc:
                print(f"[Labeling] Failed to search traces for eval run {rid}: {exc}")
                logger.warning("Failed to search traces for eval run %s", rid, exc_info=True)
        if frames:
            all_traces = pd.concat(frames, ignore_index=True)
            all_traces = all_traces.drop_duplicates(subset=["trace_id"], keep="first")
            logger.info(
                "Collected %d unique traces from %d eval runs",
                len(all_traces), len(unique_run_ids),
            )

    if all_traces is None or len(all_traces) == 0:
        print(f"[Labeling] Falling back to experiment-wide trace search for session {session_name}")
        exp = mlflow.get_experiment_by_name(experiment_name)
        if not exp:
            print(f"[Labeling] Experiment '{experiment_name}' not found — session will be empty")
            logger.warning("Experiment '%s' not found — session will be empty", experiment_name)
            return 0
        all_traces = mlflow.search_traces(
            experiment_ids=[exp.experiment_id],
            max_results=500,
        )
        if all_traces is None or len(all_traces) == 0:
            print(f"[Labeling] No traces found for experiment '{experiment_name}'")
            logger.warning("No traces found for experiment '%s'", experiment_name)
            return 0

    # --- Filter to failed questions only (when question IDs are provided) ---
    if failure_question_ids and "request" in all_traces.columns:
        fail_set = set(failure_question_ids)
        total_before = len(all_traces)
        qid_col = all_traces["request"].apply(_extract_question_id)
        failed_mask = qid_col.isin(fail_set)
        all_traces = all_traces[failed_mask]

        # Keep only the latest trace per question for clean 1:1 review
        all_traces = all_traces.assign(_qid=qid_col[failed_mask])
        if "timestamp" in all_traces.columns:
            all_traces = all_traces.sort_values("timestamp", ascending=False)
        all_traces = all_traces.drop_duplicates(subset=["_qid"], keep="first")
        all_traces = all_traces.drop(columns=["_qid"])

        print(
            f"[Labeling] Filtered to {len(all_traces)} failed-question traces "
            f"from {total_before} total ({len(fail_set)} failure question IDs)"
        )

        if all_traces.empty:
            print(f"[Labeling] No traces matched failure question IDs — session will be empty")
            logger.warning("No traces matched failure question IDs for session %s", session_name)
            return 0

        batch = all_traces.head(_MAX_SESSION_TRACES)
        session.add_traces(batch)
        traces_added = len(batch)
        print(f"[Labeling] Added {traces_added} failed-question traces to session {session_name}")
        return traces_added

    # No question-level filtering provided — session should not have been created
    print(f"[Labeling] No failure_question_ids provided — session {session_name} will be empty")
    return 0


def _find_session_by_name(session_name: str) -> Any | None:
    """Find a labeling session by name. Returns None if not found."""
    try:
        import mlflow.genai.labeling as labeling
        all_sessions = labeling.get_labeling_sessions()
        for s in all_sessions:
            if s.name == session_name:
                return s
    except Exception:
        logger.debug("Failed to search labeling sessions", exc_info=True)
    return None


_SUGGESTION_TYPE_MAP: dict[str, str] = {
    "column": "column_description",
    "rule": "business_rule",
    "join": "join_condition",
    "instruction": "instruction",
    "general": "general",
}

_SUGGESTION_RE = re.compile(
    r"^\[(?P<type>[a-zA-Z_]+)\]\s*(?:(?P<target>.+?):\s*)?(?P<suggestion>.+)$",
    re.DOTALL,
)


def _parse_structured_suggestion(text: str) -> dict[str, Any]:
    """Parse a ``[type] target: suggestion`` string into a typed dict.

    Falls back to ``target_type="general"`` for entries that don't match
    the expected format.
    """
    text = text.strip()
    m = _SUGGESTION_RE.match(text)
    if m:
        raw_type = m.group("type").lower().strip()
        target_type = _SUGGESTION_TYPE_MAP.get(raw_type, "general")
        target_id = (m.group("target") or "").strip()
        suggestion = m.group("suggestion").strip()
        return {
            "type": "improvement",
            "target_type": target_type,
            "target_identifier": target_id,
            "suggestion": suggestion,
        }
    return {
        "type": "improvement",
        "target_type": "general",
        "target_identifier": "",
        "suggestion": text,
    }


def ingest_human_feedback(
    session_name: str,
) -> dict[str, Any]:
    """Read human labels from a labeling session and return actionable feedback.

    Finds the session by name, then reads traces via the session's
    ``mlflow_run_id`` to extract assessments.

    Returns a dict with:
      - corrections: list of actionable items
      - total_reviewed: number of traces reviewed
    """
    session = _find_session_by_name(session_name)
    if session is None:
        logger.info("Labeling session '%s' not found — no feedback to ingest", session_name)
        return {"corrections": [], "total_reviewed": 0}

    session_run_id = getattr(session, "mlflow_run_id", "")
    if not session_run_id:
        logger.warning("Labeling session '%s' has no mlflow_run_id", session_name)
        return {"corrections": [], "total_reviewed": 0}

    try:
        traces_df = mlflow.search_traces(run_id=session_run_id)
    except Exception:
        logger.exception("Failed to search traces for session %s", session_name)
        return {"corrections": [], "total_reviewed": 0}

    if traces_df is None or len(traces_df) == 0:
        return {"corrections": [], "total_reviewed": 0}

    corrections: list[dict[str, Any]] = []
    total_reviewed = len(traces_df)

    for _, row in traces_df.iterrows():
        assessments = row.get("assessments")
        if not assessments:
            continue
        if not isinstance(assessments, list):
            try:
                assessments = list(assessments)
            except (TypeError, ValueError):
                continue

        trace_id = row.get("trace_id", "")
        for a in assessments:
            a_name = getattr(a, "name", None) or (a.get("name") if isinstance(a, dict) else None)
            a_value = getattr(a, "value", None) or (a.get("value") if isinstance(a, dict) else None)
            a_rationale = getattr(a, "rationale", None) or (a.get("rationale") if isinstance(a, dict) else None)

            if a_name == SCHEMA_JUDGE_VERDICT and a_value:
                _val = str(a_value)
                _feedback_code: str | None = None
                # New schema: "Is this question truly failing?"
                if "No" in _val and "correct" in _val.lower():
                    _feedback_code = "genie_correct"
                elif "Benchmark" in _val:
                    _feedback_code = "benchmark_wrong"
                elif "Ambiguous" in _val:
                    _feedback_code = "ambiguous"
                elif "Yes" in _val and "wrong" in _val.lower():
                    pass  # Confirms failure — no override needed
                # Legacy schema options (pre-update)
                elif "Wrong" in _val and "fine" in _val.lower():
                    _feedback_code = "genie_correct"
                elif "Wrong" in _val and "both" in _val.lower():
                    _feedback_code = "benchmark_wrong"

                if _feedback_code:
                    _jq = ""
                    _jqid = ""
                    try:
                        _jreq = row.get("request") or ""
                        if isinstance(_jreq, str):
                            import json as _json
                            _jq = _json.loads(_jreq).get("messages", [{}])[-1].get("content", "")
                        _jqid = _extract_question_id(row.get("request"))
                        if not _jqid:
                            _jqid = row.get("request_id", "") or ""
                        if not _jqid:
                            _a_meta = getattr(a, "metadata", None) or (a.get("metadata") if isinstance(a, dict) else None)
                            if isinstance(_a_meta, dict):
                                _jqid = _a_meta.get("question_id", "")
                    except Exception:
                        pass
                    corrections.append({
                        "type": "judge_override",
                        "trace_id": trace_id,
                        "feedback": _feedback_code,
                        "comment": a_rationale or "",
                        "question": _jq,
                        "question_id": _jqid,
                    })
            elif a_name == SCHEMA_CORRECTED_SQL and a_value:
                _q = ""
                try:
                    _req = row.get("request") or ""
                    if isinstance(_req, str):
                        import json
                        _q = json.loads(_req).get("messages", [{}])[-1].get("content", "")
                except Exception:
                    pass
                corrections.append({
                    "type": "benchmark_correction",
                    "trace_id": trace_id,
                    "corrected_sql": a_value,
                    "question": _q,
                })
            elif a_name == SCHEMA_IMPROVEMENTS and a_value:
                items = a_value if isinstance(a_value, list) else [a_value]
                for item in items:
                    parsed = _parse_structured_suggestion(str(item))
                    parsed["trace_id"] = trace_id
                    corrections.append(parsed)

    logger.info(
        "Ingested %d corrections from %d reviewed traces (session '%s')",
        len(corrections), total_reviewed, session_name,
    )
    return {"corrections": corrections, "total_reviewed": total_reviewed}


def sync_corrections_to_dataset(
    session_name: str,
    dataset_name: str,
) -> bool:
    """Sync corrected expectations from a labeling session to the eval dataset.

    Finds the session by name and calls ``session.sync(to_dataset=...)``
    to propagate human corrections back to the UC-backed evaluation dataset.
    """
    session = _find_session_by_name(session_name)
    if session is None:
        logger.warning("Labeling session '%s' not found — cannot sync", session_name)
        return False

    try:
        session.sync(to_dataset=dataset_name)
        logger.info("Synced session '%s' to dataset %s", session_name, dataset_name)
        return True
    except Exception:
        logger.exception("Failed to sync session '%s' to dataset %s", session_name, dataset_name)
        return False


# ═══════════════════════════════════════════════════════════════════════
# Flag for Human Review
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

    from genie_space_optimizer.optimization.state import run_query

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
            spark.sql(f"""
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
            """)
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
