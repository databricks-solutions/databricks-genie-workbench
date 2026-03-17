"""Syntax validity scorer — Layer 1 CODE judge.

Validates generated SQL by executing ``EXPLAIN`` via Spark.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.genie_client import sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    _extract_response_text,
    build_asi_metadata,
    format_asi_markdown,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _set_sql_context(spark: SparkSession, catalog: str, schema: str) -> None:
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")


def _make_syntax_validity_scorer(spark: SparkSession, catalog: str, schema: str):
    """Factory that binds ``spark`` into the scorer closure."""

    @scorer
    def syntax_validity_scorer(inputs: dict, outputs: dict) -> Feedback:
        """Check SQL syntax by running EXPLAIN."""
        question_id = inputs.get("question_id", "")
        sql = sanitize_sql(_extract_response_text(outputs))
        if not sql or not sql.strip():
            metadata = build_asi_metadata(
                failure_type="other",
                severity="critical",
                confidence=1.0,
                missing_metadata="Genie returned no SQL",
                counterfactual_fix="Check Genie Space instructions and data asset visibility",
            )
            return Feedback(
                name="syntax_validity",
                value="no",
                rationale=format_asi_markdown(
                    judge_name="syntax_validity",
                    value="no",
                    rationale="No SQL generated.",
                    metadata=metadata,
                    question_id=question_id,
                ),
                source=CODE_SOURCE,
                metadata=metadata,
            )
        try:
            _set_sql_context(spark, catalog, schema)
            spark.sql(f"EXPLAIN {sql}")
            return Feedback(
                name="syntax_validity",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="syntax_validity",
                    value="yes",
                    rationale="SQL parses successfully via EXPLAIN.",
                    question_id=question_id,
                ),
                source=CODE_SOURCE,
            )
        except Exception as e:
            error_msg = str(e)[:200]
            metadata = build_asi_metadata(
                failure_type="other",
                severity="critical",
                confidence=0.9,
                wrong_clause="SELECT",
                actual_value=sql[:100],
                counterfactual_fix="Fix SQL syntax in generated query",
            )
            return Feedback(
                name="syntax_validity",
                value="no",
                rationale=format_asi_markdown(
                    judge_name="syntax_validity",
                    value="no",
                    rationale=f"EXPLAIN failed: {error_msg}",
                    metadata=metadata,
                    question_id=question_id,
                ),
                source=CODE_SOURCE,
                metadata=metadata,
            )

    return syntax_validity_scorer
