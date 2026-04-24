"""Syntax validity scorer — Layer 1 CODE judge.

Validates generated SQL by executing ``EXPLAIN`` via Spark.

Tier 3.7 / 3.8 / 3.9: wraps the EXPLAIN call in ``quiet_grpc_logs`` to
suppress triple-logged gRPC reattach errors, pre-validates balanced
backticks before the gRPC round-trip, and extracts ``line / pos`` from
``PARSE_SYNTAX_ERROR`` messages into ASI metadata so classifiers can
cluster parse errors by location.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.genie_client import sanitize_sql
from genie_space_optimizer.common.logging_utils import quiet_grpc_logs
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


_PARSE_POS_RE = re.compile(r"line\s+(\d+),\s*pos\s+(\d+)", re.IGNORECASE)


def _has_unbalanced_backticks(sql: str) -> bool:
    """Tier 3.8: return True when the SQL has an odd count of backticks.

    Databricks SQL uses backticks to delimit identifiers. An odd count
    means at least one identifier is mis-quoted — EXPLAIN will fail
    with ``PARSE_SYNTAX_ERROR`` at the opener of the next balanced
    identifier. Classify that pattern locally so the error surfaces as
    ``unbalanced_identifier_quoting`` (specific, actionable) instead of
    the generic ``other`` failure type with an EXPLAIN stack trace.
    """
    return sql.count("`") % 2 == 1


def _parse_error_position(error_msg: str) -> tuple[int, int] | None:
    m = _PARSE_POS_RE.search(error_msg)
    if not m:
        return None
    try:
        return int(m.group(1)), int(m.group(2))
    except (TypeError, ValueError):
        return None


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

        # Tier 3.8: local pre-validation for unbalanced backticks.
        # Avoids a gRPC round-trip (and its triplicated log spam) for a
        # well-defined class of Genie output bug.
        if _has_unbalanced_backticks(sql):
            metadata = build_asi_metadata(
                failure_type="unbalanced_identifier_quoting",
                severity="critical",
                confidence=1.0,
                actual_value=sql[:100],
                counterfactual_fix=(
                    "Balance backtick-delimited identifiers in generated SQL. "
                    "Every `ident` must have a matching closing backtick."
                ),
            )
            return Feedback(
                name="syntax_validity",
                value="no",
                rationale=format_asi_markdown(
                    judge_name="syntax_validity",
                    value="no",
                    rationale=(
                        "Pre-EXPLAIN check: SQL has an odd number of backticks, "
                        "indicating an unbalanced identifier quotation."
                    ),
                    metadata=metadata,
                    question_id=question_id,
                ),
                source=CODE_SOURCE,
                metadata=metadata,
            )

        try:
            # Tier 3.7: wrap the EXPLAIN in ``quiet_grpc_logs`` so the
            # gRPC reattach retries don't print three copies of the same
            # stack trace on every failing Genie SQL.
            with quiet_grpc_logs():
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
            # Tier 3.9: stamp parse position into ASI metadata so
            # classifiers can bucket parse errors by (line, pos).
            _pos = _parse_error_position(error_msg)
            metadata = build_asi_metadata(
                failure_type="other",
                severity="critical",
                confidence=0.9,
                wrong_clause="SELECT",
                actual_value=sql[:100],
                counterfactual_fix="Fix SQL syntax in generated query",
            )
            if _pos is not None:
                if isinstance(metadata, dict):
                    metadata.setdefault("parse_position", {"line": _pos[0], "pos": _pos[1]})
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
