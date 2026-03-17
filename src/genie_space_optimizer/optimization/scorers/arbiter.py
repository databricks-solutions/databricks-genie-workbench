"""Arbiter scorer — Layer 3 conditional LLM judge.

Fires only when result_correctness reports a mismatch. Arbitrates between
the ground-truth SQL and Genie SQL to determine which is correct.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.config import JUDGE_PROMPTS, LLM_ENDPOINT
from genie_space_optimizer.common.genie_client import resolve_sql, sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    LLM_SOURCE,
    build_temporal_note,
    slim_comparison,
    _call_llm_for_scoring,
    _extract_response_text,
    build_asi_metadata,
    format_asi_markdown,
    get_registered_prompt_name,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _parse_arbiter_verdict(rationale: str) -> str:
    """Extract verdict from arbiter feedback text."""
    text = rationale.lower()
    for v in ("genie_correct", "both_correct", "neither_correct", "ground_truth_correct"):
        if v in text:
            return v
    return "ground_truth_correct"


def _make_arbiter_scorer(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    loaded_prompts: dict[str, str] | None = None,
):
    """Factory that binds the workspace client and SQL resolution context."""
    _loaded = loaded_prompts or {}

    @scorer
    def arbiter_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """Conditional scorer that fires only when results disagree.

        Returns value="skipped" when results match (no LLM call).
        """
        question_id = inputs.get("question_id", "")
        cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}

        if cmp.get("match"):
            logger.info(
                "\n"
                "┌─── JUDGE [arbiter] BOTH_CORRECT (results match) ──────────────────────\n"
                "│ Question: %s\n"
                "│ Reason:   Results match (%s) — both queries correct\n"
                "└─────────────────────────────────────────────────────────────────────────",
                inputs.get("question", "")[:80],
                cmp.get("match_type", "unknown"),
            )
            return Feedback(
                name="arbiter",
                value="both_correct",
                rationale=format_asi_markdown(
                    judge_name="arbiter",
                    value="both_correct",
                    rationale=f"Results match ({cmp.get('match_type', 'unknown')}) — both queries are correct.",
                    extra={"comparison": slim_comparison(cmp)},
                    question_id=question_id,
                ),
                source=CODE_SOURCE,
            )

        if cmp.get("error"):
            error_type = cmp.get("error_type", "")
            gt_rows = cmp.get("gt_rows", -1)
            if error_type == "genie_result_unavailable" and gt_rows == 0:
                logger.info(
                    "\n"
                    "┌─── JUDGE [arbiter] BOTH_CORRECT (null-result defense) ────────────────\n"
                    "│ Question: %s\n"
                    "│ Reason:   GT returned 0 rows, Genie results unavailable\n"
                    "└─────────────────────────────────────────────────────────────────────────",
                    inputs.get("question", "")[:80],
                )
                return Feedback(
                    name="arbiter",
                    value="both_correct",
                    rationale=format_asi_markdown(
                        judge_name="arbiter",
                        value="both_correct",
                        rationale=(
                            "GT returned 0 rows and Genie results unavailable — "
                            "both are effectively correct (empty result set)."
                        ),
                        extra={"comparison": slim_comparison(cmp)},
                        question_id=question_id,
                    ),
                    source=CODE_SOURCE,
                )

            _SKIP_ERROR_TYPES = frozenset({
                "infrastructure", "permission_blocked", "query_execution",
            })
            if error_type in _SKIP_ERROR_TYPES:
                logger.info(
                    "\n"
                    "┌─── JUDGE [arbiter] SKIPPED ─────────────────────────────────────────────\n"
                    "│ Question: %s\n"
                    "│ Reason:   SQL execution error — cannot arbitrate\n"
                    "│ Error:    %s\n"
                    "└─────────────────────────────────────────────────────────────────────────",
                    inputs.get("question", "")[:80],
                    str(cmp["error"])[:200],
                )
                return Feedback(
                    name="arbiter",
                    value="skipped",
                    rationale=format_asi_markdown(
                        judge_name="arbiter",
                        value="skipped",
                        rationale=f"SQL execution error — cannot arbitrate: {cmp['error']}",
                        extra={"comparison": slim_comparison(cmp)},
                        question_id=question_id,
                    ),
                    source=CODE_SOURCE,
                )

        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")

        arbiter_instructions = _loaded.get("arbiter", JUDGE_PROMPTS.get("arbiter", ""))
        gt_sample = cmp.get("gt_sample", "(unavailable)")
        genie_sample = cmp.get("genie_sample", "(unavailable)")

        _GENIE_ROW_CAP = 5000
        genie_rows = cmp.get("genie_rows", 0)
        gt_rows = cmp.get("gt_rows", 0)
        row_cap_note = ""
        if genie_rows == _GENIE_ROW_CAP and gt_rows > _GENIE_ROW_CAP:
            row_cap_note = (
                f"\nIMPORTANT: Genie returned exactly {_GENIE_ROW_CAP} rows (the platform "
                f"row cap) while GT has {gt_rows} rows. This row count difference is a "
                "PLATFORM LIMITATION, not a query error. Do NOT penalize Genie for "
                "returning fewer rows when it hit the cap.\n"
            )

        gt_empty_note = ""
        if gt_rows == 0 and genie_rows > 0:
            gt_empty_note = (
                f"\nIMPORTANT: The Ground Truth returned 0 rows while Genie returned "
                f"{genie_rows} rows. This may indicate the GT has overly restrictive "
                "filters that the user did not request (e.g. date ranges, status filters). "
                "If the GT adds filters NOT present in the user's question and those "
                "filters cause it to return no data, the GT is WRONG for this question. "
                "Evaluate Genie on its own merit against the question.\n"
            )

        genie_empty_note = ""
        _err = cmp.get("error", "")
        _err_type = cmp.get("error_type", "")
        if (
            _err_type == "genie_result_unavailable"
            and (gt_rows or 0) > 0
            and (genie_rows or 0) == 0
        ):
            genie_empty_note = (
                f"\nIMPORTANT: Genie returned 0 rows (result unavailable) while GT "
                f"returned {gt_rows} rows. COMPARE THE SQL APPROACHES, not just the "
                "results. If the question uses relative time references (e.g. 'this year') "
                "and Genie's SQL uses a date column that more naturally matches the "
                "question's intent (e.g. booking_created_date for 'bookings this year') "
                "while GT uses a different date column (e.g. check_in_date), Genie may "
                "be correct — the 0-row result could be valid for the current time period. "
                "Judge based on which SQL better answers the USER'S QUESTION.\n"
            )

        prompt = (
            f"{arbiter_instructions}\n\n"
            "YOUR ROLE: You are the final arbiter. The Ground Truth is NOT always correct.\n"
            "Your job is to independently judge whether each query correctly answers the\n"
            "USER'S QUESTION. The user's question is the SOLE source of truth.\n\n"
            "VERDICT RULES:\n"
            "- genie_correct: Genie answers the question correctly, GT does not\n"
            "  (e.g. GT adds filters the user never requested, or GT returns 0 rows\n"
            "  due to overly restrictive conditions).\n"
            "- ground_truth_correct: GT answers correctly, Genie does not\n"
            "  (e.g. Genie misses something the user asked for, wrong table, wrong metric).\n"
            "- both_correct: Both answer the question correctly (cosmetic SQL differences\n"
            "  like aliases, ORDER BY, IS NOT NULL, GROUP BY ALL are irrelevant).\n"
            "- neither_correct: Neither query answers the question correctly.\n\n"
            "- TEMPORAL RELATIVITY: If the question uses relative time ('this year',\n"
            "  'last month'), the CURRENT DATE determines the correct window. If Genie\n"
            "  uses dates matching the current temporal context while GT uses stale dates\n"
            "  from a different period, Genie is correct.\n\n"
            "COSMETIC DIFFERENCES (never penalize):\n"
            "- Column aliases, ORDER BY, GROUP BY ALL vs explicit, IS NOT NULL guards\n"
            "- MEASURE() on metric view vs SUM/AVG on fact table\n"
            "- Extra columns in GT that the user did not ask for\n"
            "- Platform row cap (Genie max 5000 rows)\n\n"
            "DISTINCT / DEDUP SEMANTICS:\n"
            "If the question asks 'which X', 'list X', or otherwise implies a distinct "
            "set, and one query uses DISTINCT producing fewer rows while the other does "
            "not, prefer the DISTINCT version as more correct. Mark the non-DISTINCT "
            "result as ground_truth_correct (if GT uses DISTINCT) or genie_correct "
            "(if Genie uses DISTINCT) unless the question explicitly asks for all "
            "instances including duplicates.\n\n"
            f"Question: {question}\n"
            f"Ground Truth SQL: {gt_sql}\n"
            f"Genie SQL: {genie_sql}\n\n"
            f"Result comparison: match={cmp.get('match')}, "
            f"match_type={cmp.get('match_type')}, "
            f"gt_rows={gt_rows}, genie_rows={genie_rows}\n"
            f"{row_cap_note}{gt_empty_note}{genie_empty_note}{build_temporal_note(cmp)}\n"
            f"Ground Truth Result (first 5 rows):\n{gt_sample}\n\n"
            f"Genie Result (first 5 rows):\n{genie_sample}\n\n"
            'Respond with JSON only: {"verdict": "<genie_correct|ground_truth_correct|both_correct|neither_correct>", '
            '"failure_type": "<wrong_aggregation|wrong_filter|wrong_table|other>", '
            '"blame_set": ["<blamed_object>"], '
            '"rationale": "<brief explanation>"}'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [arbiter] INPUT ──────────────────────────────────────────────\n"
            "│ Question: %s\n"
            "│ Genie SQL:\n"
            "│   %s\n"
            "│ GT SQL:\n"
            "│   %s\n"
            "│ Comparison: %s\n"
            "│ Trigger:    results MISMATCH (arbiter invoked)\n"
            "│ Prompt length: %d chars\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question,
            genie_sql or "(none)",
            gt_sql or "(none)",
            json.dumps(cmp, indent=2),
            len(prompt),
        )

        try:
            result = _call_llm_for_scoring(w, prompt, prompt_name=get_registered_prompt_name("arbiter"))
            verdict = result.get("verdict", "ground_truth_correct")
            valid_verdicts = {
                "genie_correct",
                "ground_truth_correct",
                "both_correct",
                "neither_correct",
            }
            if verdict not in valid_verdicts:
                verdict = _parse_arbiter_verdict(result.get("rationale", str(result)))

            logger.info(
                "\n"
                "┌─── JUDGE [arbiter] VERDICT ─────────────────────────────────────────────\n"
                "│ Question:    %s\n"
                "│ Verdict:     %s\n"
                "│ Rationale:   %s\n"
                "│ Failure:     type=%s | blame=%s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                question[:80],
                verdict,
                result.get("rationale", "(none)"),
                result.get("failure_type", "n/a"),
                result.get("blame_set", []),
            )

            _meta = None
            if verdict in ("ground_truth_correct", "neither_correct"):
                _meta = build_asi_metadata(
                    failure_type=result.get("failure_type", "other"),
                    severity="major",
                    confidence=0.85,
                    blame_set=result.get("blame_set", []),
                    counterfactual_fix=result.get("rationale", ""),
                )
            return Feedback(
                name="arbiter",
                value=verdict,
                rationale=format_asi_markdown(
                    judge_name="arbiter",
                    value=verdict,
                    rationale=result.get("rationale", verdict),
                    metadata=_meta,
                    extra={"llm_response": result, "comparison": slim_comparison(cmp)},
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
                metadata=_meta,
            )
        except Exception as e:
            logger.error(
                "\n"
                "┌─── JUDGE [arbiter] ERROR ──────────────────────────────────────────────\n"
                "│ Question:    %s\n"
                "│ Error:       %s\n"
                "│ Prompt len:  %d chars\n"
                "│ LLM endpoint: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                question[:80], str(e)[:300], len(prompt), LLM_ENDPOINT,
            )
            metadata = build_asi_metadata(
                failure_type="other",
                severity="info",
                confidence=0.0,
                counterfactual_fix="LLM judge unavailable — retry or check endpoint",
            )
            return Feedback(
                name="arbiter",
                value="ground_truth_correct",
                rationale=format_asi_markdown(
                    judge_name="arbiter",
                    value="ground_truth_correct",
                    rationale=f"Arbiter LLM call failed, defaulting to ground_truth_correct: {e}",
                    metadata=metadata,
                    extra={"comparison": slim_comparison(cmp)},
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

    return arbiter_scorer
