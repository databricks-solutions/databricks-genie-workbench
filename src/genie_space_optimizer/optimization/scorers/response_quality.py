"""Response quality judge -- LLM scorer.

Evaluates whether Genie's natural language analysis/explanation
accurately describes the SQL query and answers the user's question.

Returns ``"unknown"`` when no analysis text is available (the Genie
public API does not always include the text attachment), so this scorer
never penalises runs where the NL response is absent.
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

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _make_response_quality_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def response_quality_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: natural language response accuracy.

        Skips gracefully (``value="unknown"``) when no analysis text is
        available from Genie.
        """
        question_id = inputs.get("question_id", "")
        analysis_text = outputs.get("analysis_text") if isinstance(outputs, dict) else None

        if not analysis_text:
            return Feedback(
                name="response_quality",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="response_quality",
                    value="unknown",
                    rationale="No analysis text available from Genie.",
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
            )

        genie_sql = sanitize_sql(_extract_response_text(outputs))
        question = inputs.get("question", "")
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)

        prompt = (
            "You are evaluating the quality of a natural language response "
            "from a Databricks Genie Space AI assistant.\n\n"
            f"User question: {question}\n"
            f"Genie's natural language response:\n  {analysis_text}\n"
            f"Genie's SQL query:\n  {genie_sql}\n"
            f"Expected SQL:\n  {gt_sql}\n\n"
            "Evaluate whether the natural language response:\n"
            "1. Accurately describes what the SQL query does\n"
            "2. Correctly answers the user's question\n"
            "3. Does not make claims unsupported by the query/data\n\n"
            'Respond with JSON only: {"accurate": true/false, '
            '"failure_type": "<inaccurate_description|unsupported_claim|misleading_summary>", '
            '"counterfactual_fix": "<specific change to Genie Space metadata or instructions that would fix this response quality issue>", '
            '"rationale": "<brief explanation>"}\n'
            'If accurate, set failure_type to "" and counterfactual_fix to "".'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [response_quality] INPUT ────────────────────────────────────\n"
            "│ Question: %s\n"
            "│ Analysis text:\n"
            "│   %s\n"
            "│ Genie SQL:\n"
            "│   %s\n"
            "│ GT SQL:\n"
            "│   %s\n"
            "│ Prompt length: %d chars\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question,
            (analysis_text or "(none)")[:200],
            genie_sql or "(none)",
            gt_sql or "(none)",
            len(prompt),
        )

        try:
            result = _call_llm_for_scoring(w, prompt, prompt_name=get_registered_prompt_name("response_quality"))
        except Exception as e:
            logger.error(
                "\n"
                "┌─── JUDGE [response_quality] ERROR ────────────────────────────────────\n"
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
                name="response_quality",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="response_quality",
                    value="unknown",
                    rationale=f"LLM call failed: {e}",
                    metadata=metadata,
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        logger.info(
            "\n"
            "┌─── JUDGE [response_quality] VERDICT ──────────────────────────────────\n"
            "│ Question:  %s\n"
            "│ Verdict:   %s\n"
            "│ Rationale: %s\n"
            "│ Failure:   type=%s\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question[:80],
            "PASS" if result.get("accurate") else "FAIL",
            result.get("rationale", "(none)"),
            result.get("failure_type", "n/a"),
        )

        if result.get("accurate", False):
            return Feedback(
                name="response_quality",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="response_quality",
                    value="yes",
                    rationale=result.get("rationale", "Response is accurate"),
                    extra={"llm_response": result},
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
            )

        metadata = build_asi_metadata(
            failure_type=result.get("failure_type", "inaccurate_description"),
            severity="minor",
            confidence=0.7,
            counterfactual_fix=result.get("counterfactual_fix") or (
                f"Fix {result.get('failure_type', 'response quality issue')} in Genie response"
            ),
        )
        return Feedback(
            name="response_quality",
            value="no",
            rationale=format_asi_markdown(
                judge_name="response_quality",
                value="no",
                rationale=result.get("rationale", "Response quality issue"),
                metadata=metadata,
                extra={"llm_response": result},
                question_id=question_id,
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return response_quality_judge
