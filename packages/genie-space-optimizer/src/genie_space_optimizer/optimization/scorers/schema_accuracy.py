"""Schema accuracy judge — LLM scorer.

Determines whether generated SQL references the correct tables, columns,
and joins.
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


def _make_schema_accuracy_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def schema_accuracy_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: schema correctness with structured ASI output."""
        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")
        question_id = inputs.get("question_id", "")
        cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}

        context = build_scorer_context(
            question=question, genie_sql=genie_sql, gt_sql=gt_sql, cmp=cmp,
        )

        prompt = (
            "You are a SQL schema expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the GENERATED SQL references the correct tables, columns, and joins.\n\n"
            "IMPORTANT RULES:\n"
            "1. Evaluate column selection against the USER'S QUESTION, not the Expected SQL.\n"
            "   The Expected SQL is a reference that may include extra columns. Only flag\n"
            "   missing_column when a column the USER asked for is absent.\n"
            "2. ASSET EQUIVALENCE: A metric view (using MEASURE()), a TVF (table-valued function),\n"
            "   and the underlying fact table are ALL valid schema choices when they answer the\n"
            "   same question. Do NOT flag wrong_table when Generated SQL uses one asset type\n"
            "   while Expected uses another. The asset_routing judge handles routing preference\n"
            "   separately — schema_accuracy only cares about correctness.\n"
            "3. COSMETIC DIFFERENCES are NOT schema errors:\n"
            "   - GROUP BY ALL vs explicit GROUP BY (identical semantics)\n"
            "   - Defensive WHERE col IS NOT NULL filters\n"
            "   - ORDER BY differences\n"
            "   - Column aliasing differences\n\n"
            f"{context}\n\n"
            'Respond with JSON only: {"correct": true/false, '
            '"failure_type": "<wrong_table|wrong_column|wrong_join|missing_join_spec|wrong_join_spec|missing_column>", '
            '"wrong_clause": "<the problematic SQL clause>", "blame_set": ["<table_or_column>"], '
            '"counterfactual_fix": "<specific Genie Space metadata change that would fix this, referencing exact table/column names>", '
            '"rationale": "<brief explanation>", '
            '"join_assessment": null}\n'
            'If correct, set failure_type to "", blame_set to [], counterfactual_fix to "", and join_assessment to null.\n'
            'If failure_type is wrong_join, missing_join_spec, or wrong_join_spec, '
            'set join_assessment to: {"issue": "<missing_join|wrong_condition|wrong_direction>", '
            '"left_table": "<fully_qualified_table_name>", "right_table": "<fully_qualified_table_name>", '
            '"suggested_condition": "<left_table.col = right_table.col>", '
            '"relationship": "<many_to_one|one_to_many|one_to_one>", '
            '"evidence": "<brief explanation of why this join is needed based on the SQL comparison>"}. '
            'Use fully-qualified table names (catalog.schema.table) from the SQL.'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [schema_accuracy] INPUT ──────────────────────────────────────\n"
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
            result = _call_llm_for_scoring(w, prompt, prompt_name=get_registered_prompt_name("schema_accuracy"))
        except Exception as e:
            logger.error(
                "\n"
                "┌─── JUDGE [schema_accuracy] ERROR ──────────────────────────────────────\n"
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
                name="schema_accuracy",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="schema_accuracy",
                    value="unknown",
                    rationale=f"LLM call failed: {e}",
                    metadata=metadata,
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        result_matched = cmp.get("match", False)
        result_override = result_matched and not result.get("correct", False)

        logger.info(
            "\n"
            "┌─── JUDGE [schema_accuracy] VERDICT ────────────────────────────────────\n"
            "│ Question:  %s\n"
            "│ Verdict:   %s\n"
            "│ Rationale: %s\n"
            "│ Failure:   type=%s | blame=%s | clause=%s\n"
            "│ Override:  %s\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question[:80],
            "PASS" if result.get("correct") else "FAIL",
            result.get("rationale", "(none)"),
            result.get("failure_type", "n/a"),
            result.get("blame_set", []),
            result.get("wrong_clause", "n/a"),
            "result_match -> forced PASS" if result_override else "none",
        )

        if result_override:
            return Feedback(
                name="schema_accuracy",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="schema_accuracy",
                    value="yes",
                    rationale=(
                        f"OVERRIDE: Results match ({cmp.get('match_type')}). "
                        f"LLM noted SQL differences: {result.get('rationale', '')}"
                    ),
                    extra={"llm_response": result, "override_reason": "result_match"},
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
            )

        if result.get("correct", False):
            return Feedback(
                name="schema_accuracy",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="schema_accuracy",
                    value="yes",
                    rationale=result.get("rationale", "Schema correct"),
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

        _join_assess = result.get("join_assessment")
        if isinstance(_join_assess, dict) and not _join_assess.get("left_table"):
            _join_assess = None

        metadata = build_asi_metadata(
            failure_type=result.get("failure_type", "wrong_column"),
            severity="major",
            confidence=base_confidence,
            wrong_clause=result.get("wrong_clause", ""),
            blame_set=result.get("blame_set", []),
            counterfactual_fix=result.get("counterfactual_fix") or (
                f"Fix {result.get('failure_type', 'schema issue')} "
                f"involving {', '.join(result.get('blame_set', ['unknown']))}"
            ),
            join_assessment=_join_assess,
        )
        return Feedback(
            name="schema_accuracy",
            value="no",
            rationale=format_asi_markdown(
                judge_name="schema_accuracy",
                value="no",
                rationale=result.get("rationale", "Schema mismatch"),
                metadata=metadata,
                extra={"llm_response": result},
                question_id=question_id,
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return schema_accuracy_judge
