"""Semantic equivalence judge — LLM scorer.

Determines whether two SQL queries are semantically equivalent, measuring
the same business metric even if written differently.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.config import LLM_ENDPOINT
from genie_space_optimizer.common.genie_client import resolve_sql, sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    LLM_SOURCE,
    _call_llm_for_scoring,
    _extract_response_text,
    build_asi_metadata,
    format_asi_markdown,
    get_registered_prompt_name,
)
from genie_space_optimizer.optimization.scorers import build_scorer_context

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _make_semantic_equivalence_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def semantic_equivalence_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: semantic equivalence with structured ASI output."""
        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")
        question_id = inputs.get("question_id", "")
        cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}

        context = build_scorer_context(
            question=question, genie_sql=genie_sql, gt_sql=gt_sql, cmp=cmp,
            include_column_note=True,
        )

        prompt = (
            "You are a SQL semantics expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the two SQL queries measure the SAME business metric and would "
            "answer the same question, even if written differently.\n\n"
            "IMPORTANT — Equivalence rules:\n"
            "- Extra columns in the Expected SQL do not change the business metric.\n"
            "- ILIKE '%value%' vs = 'value' for proper nouns are equivalent.\n"
            "- Defensive IS NOT NULL guards do not change the metric being measured.\n"
            "- GROUP BY ALL vs explicit GROUP BY is semantically identical.\n"
            "- ORDER BY differences are cosmetic.\n"
            "- MEASURE() on a metric view is equivalent to SUM/AVG on the fact table.\n"
            "- A TVF and metric view covering the same domain are equivalent.\n"
            "- Focus on whether BOTH queries answer the SAME question.\n\n"
            f"{context}\n\n"
            'Respond with JSON only: {"equivalent": true/false, "failure_type": "<different_metric|different_grain|different_scope>", '
            '"blame_set": ["<metric_or_dimension>"], '
            '"counterfactual_fix": "<specific Genie Space metadata change that would fix this, referencing exact table/column names>", '
            '"rationale": "<brief explanation>"}\n'
            'If equivalent, set failure_type to "", blame_set to [], and counterfactual_fix to "".'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [semantic_equivalence] INPUT ──────────────────────────────────\n"
            "│ Question: %s\n"
            "│ Genie SQL:\n"
            "│   %s\n"
            "│ GT SQL:\n"
            "│   %s\n"
            "│ Result Comparison: match=%s | type=%s | gt_rows=%s | genie_rows=%s\n"
            "│ Prompt length: %d chars\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question,
            genie_sql or "(none)",
            gt_sql or "(none)",
            cmp.get("match"), cmp.get("match_type"), cmp.get("gt_rows"), cmp.get("genie_rows"),
            len(prompt),
        )

        try:
            result = _call_llm_for_scoring(w, prompt, prompt_name=get_registered_prompt_name("semantic_equivalence"))
        except Exception as e:
            logger.error(
                "\n"
                "┌─── JUDGE [semantic_equivalence] ERROR ──────────────────────────────────\n"
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
                name="semantic_equivalence",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="semantic_equivalence",
                    value="unknown",
                    rationale=f"LLM call failed: {e}",
                    metadata=metadata,
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        result_matched = cmp.get("match", False)
        _GENIE_ROW_CAP = 5000
        row_cap_hit = (
            cmp.get("genie_rows") == _GENIE_ROW_CAP
            and (cmp.get("gt_rows") or 0) > _GENIE_ROW_CAP
        )
        result_override = (result_matched or row_cap_hit) and not result.get("equivalent", False)

        logger.info(
            "\n"
            "┌─── JUDGE [semantic_equivalence] VERDICT ────────────────────────────────\n"
            "│ Question:  %s\n"
            "│ Verdict:   %s\n"
            "│ Rationale: %s\n"
            "│ Failure:   type=%s | blame=%s\n"
            "│ Override:  %s\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question[:80],
            "PASS" if result.get("equivalent") else "FAIL",
            result.get("rationale", "(none)"),
            result.get("failure_type", "n/a"),
            result.get("blame_set", []),
            (
                "result_match -> forced PASS" if result_matched
                else "row_cap -> forced PASS" if row_cap_hit
                else "none"
            ) if result_override else "none",
        )

        if result_override:
            override_reason = "result_match" if result_matched else "row_cap"
            override_detail = cmp.get("match_type", "unknown") if result_matched else f"genie={cmp.get('genie_rows')}/gt={cmp.get('gt_rows')}"
            return Feedback(
                name="semantic_equivalence",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="semantic_equivalence",
                    value="yes",
                    rationale=(
                        f"OVERRIDE ({override_reason}): {override_detail}. "
                        f"LLM noted SQL differences: {result.get('rationale', '')}"
                    ),
                    extra={"llm_response": result, "override_reason": "result_match"},
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
            )

        if result.get("equivalent", False):
            return Feedback(
                name="semantic_equivalence",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="semantic_equivalence",
                    value="yes",
                    rationale=result.get("rationale", "Semantically equivalent"),
                    extra={"llm_response": result},
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
            )

        base_confidence = 0.95
        if cmp.get("match"):
            base_confidence = 0.5
        elif cmp.get("gt_rows", -1) == 0 and cmp.get("error"):
            base_confidence = 0.3
        elif cmp.get("error"):
            base_confidence = 0.6

        metadata = build_asi_metadata(
            failure_type=result.get("failure_type", "different_metric"),
            severity="major",
            confidence=base_confidence,
            blame_set=result.get("blame_set", []),
            counterfactual_fix=result.get("counterfactual_fix") or (
                f"Fix {result.get('failure_type', 'semantic mismatch')} "
                f"involving {', '.join(result.get('blame_set', ['unknown']))}"
            ),
        )
        return Feedback(
            name="semantic_equivalence",
            value="no",
            rationale=format_asi_markdown(
                judge_name="semantic_equivalence",
                value="no",
                rationale=result.get("rationale", "Semantic mismatch"),
                metadata=metadata,
                extra={"llm_response": result},
                question_id=question_id,
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return semantic_equivalence_judge
