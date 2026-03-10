"""Pydantic models for creator agent tool schemas.

These replace ~580 lines of hand-written JSON Schema in
backend/services/create_agent_tools.py. The models serve double duty:

1. Generate JSON Schema for @app_agent tool registration via
   ``GenerateConfigArgs.model_json_schema()``
2. Validate + parse incoming tool arguments at runtime via
   ``GenerateConfigArgs(**kwargs)``

Usage in agents/creator/app.py::

    from agents.creator.schemas import GenerateConfigArgs

    @creator.tool(
        description="Generate a complete Genie Space configuration",
        parameters=GenerateConfigArgs.model_json_schema(),
    )
    async def generate_config(**kwargs) -> dict:
        args = GenerateConfigArgs(**kwargs)  # Validates at runtime
        ...
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


# ── Shared nested types ──────────────────────────────────────────────────────


class ColumnConfig(BaseModel):
    """Column-level configuration within a table."""

    column_name: str
    description: Optional[str] = None
    synonyms: Optional[list[str]] = None
    exclude: Optional[bool] = None
    enable_matching: Optional[bool] = None


class SqlParameter(BaseModel):
    """Parameter definition for parameterized example SQL."""

    name: str
    type_hint: str = Field(
        ..., pattern="^(STRING|NUMBER|DATE|BOOLEAN)$"
    )
    description: Optional[str] = None
    default_value: str


class ExampleSql(BaseModel):
    """Example SQL with natural-language question mapping."""

    question: str
    sql: str
    usage_guidance: Optional[str] = None
    parameters: Optional[list[SqlParameter]] = None


class Measure(BaseModel):
    """Aggregate measure definition (SUM, AVG, COUNT, etc.)."""

    alias: str
    sql: str
    display_name: Optional[str] = None
    synonyms: Optional[list[str]] = None
    instruction: Optional[str] = None
    comment: Optional[str] = None


class Filter(BaseModel):
    """Pre-defined filter (WHERE clause snippet)."""

    display_name: str
    sql: str
    synonyms: Optional[list[str]] = None
    instruction: Optional[str] = None
    comment: Optional[str] = None


class Expression(BaseModel):
    """Computed expression (derived column)."""

    alias: str
    sql: str
    display_name: Optional[str] = None
    synonyms: Optional[list[str]] = None
    instruction: Optional[str] = None
    comment: Optional[str] = None


class JoinSpec(BaseModel):
    """Join specification between two tables."""

    left_table: str
    left_alias: str
    right_table: str
    right_alias: str
    left_column: str
    right_column: str
    relationship: str = Field(
        ...,
        pattern="^(MANY_TO_ONE|ONE_TO_MANY|ONE_TO_ONE|MANY_TO_MANY)$",
    )
    instruction: Optional[str] = None
    comment: Optional[str] = None


class Benchmark(BaseModel):
    """Question/SQL pair for evaluation benchmarks."""

    question: str
    expected_sql: str


class MetricViewColumnConfig(BaseModel):
    """Column configuration within a metric view."""

    column_name: str
    description: Optional[str] = None
    enable_format_assistance: Optional[bool] = None


class MetricView(BaseModel):
    """Metric view definition (curated data view)."""

    identifier: str
    description: Optional[str] = None
    column_configs: Optional[list[MetricViewColumnConfig]] = None


class TableConfig(BaseModel):
    """Table-level configuration with optional column configs."""

    identifier: str
    description: Optional[str] = None
    column_configs: Optional[list[ColumnConfig]] = None


# ── Top-level tool argument models ───────────────────────────────────────────


class GenerateConfigArgs(BaseModel):
    """Arguments for ``generate_config`` and ``present_plan`` tools.

    These tools share the same schema — present_plan previews what
    generate_config will produce.
    """

    tables: list[TableConfig]
    sample_questions: Optional[list[str]] = None
    text_instructions: Optional[list[str]] = None
    example_sqls: Optional[list[ExampleSql]] = Field(
        None, min_length=3
    )
    measures: Optional[list[Measure]] = None
    filters: Optional[list[Filter]] = None
    expressions: Optional[list[Expression]] = None
    join_specs: Optional[list[JoinSpec]] = None
    benchmarks: Optional[list[Benchmark]] = None
    generate_benchmarks: Optional[bool] = None
    metric_views: Optional[list[MetricView]] = None
