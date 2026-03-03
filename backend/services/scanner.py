"""IQ scoring engine for Genie Space configurations."""

import logging
from datetime import datetime
from typing import Optional

from backend.services.genie_client import get_genie_space, get_serialized_space
from backend.services.lakebase import save_scan_result, get_latest_score

logger = logging.getLogger(__name__)

# Maturity levels based on score
MATURITY_LEVELS = [
    (85, "Optimized"),
    (70, "Proficient"),
    (50, "Developing"),
    (30, "Basic"),
    (0, "Nascent"),
]


def get_maturity_label(score: int) -> str:
    """Return maturity label for a given score (0-100)."""
    for threshold, label in MATURITY_LEVELS:
        if score >= threshold:
            return label
    return "Nascent"


def calculate_score(space_data: dict) -> dict:
    """Calculate IQ score for a Genie Space configuration.

    Returns a dict with:
        - score: int (0-100)
        - maturity: str
        - breakdown: dict with dimension scores
        - findings: list of finding strings
        - next_steps: list of recommended actions
    """
    breakdown = {
        "foundation": 0,
        "data_setup": 0,
        "sql_assets": 0,
        "optimization": 0,
    }
    findings = []
    next_steps = []

    # --- Foundation (30 pts) ---
    tables = space_data.get("data_sources", {}).get("tables", [])

    if tables:
        breakdown["foundation"] += 15
    else:
        findings.append("No tables configured")
        next_steps.append("Add at least one table to your Genie Space")

    if tables:
        tables_with_desc = [t for t in tables if t.get("description") or t.get("comment")]
        if tables_with_desc:
            breakdown["foundation"] += 10
        else:
            findings.append("Tables have no descriptions")
            next_steps.append("Add descriptions to your tables to help Genie understand context")

        # Check column descriptions
        has_col_descs = any(
            any(col.get("description") or col.get("comment")
                for col in t.get("columns", []))
            for t in tables
        )
        if has_col_descs:
            breakdown["foundation"] += 5
        else:
            findings.append("Columns have no descriptions")
            next_steps.append("Add column descriptions to improve query accuracy")

    # --- Data Setup (25 pts) ---
    # Unity Catalog check (catalog.schema.table format)
    if tables:
        uc_tables = [t for t in tables if len(t.get("table_name", "").split(".")) == 3
                     or t.get("catalog")]
        if uc_tables:
            breakdown["data_setup"] += 10
        else:
            findings.append("Tables may not be using Unity Catalog")
            next_steps.append("Use fully-qualified Unity Catalog table names (catalog.schema.table)")

        table_count = len(tables)
        if 2 <= table_count <= 10:
            breakdown["data_setup"] += 8
        elif table_count == 1:
            findings.append("Only 1 table configured - consider adding related tables")
            next_steps.append("Add 2-5 related tables for better cross-table query capability")
        elif table_count > 10:
            findings.append("More than 10 tables may reduce Genie accuracy")
            next_steps.append("Consider reducing to the most relevant 5-10 tables")

    filter_snippets = space_data.get("instructions", {}).get("sql_snippets", {}).get("filters", [])
    if filter_snippets:
        breakdown["data_setup"] += 7
    else:
        findings.append("No filter snippets defined")
        next_steps.append("Add filter snippets for common time ranges and business segments")

    # --- SQL Assets (25 pts) ---
    example_sqls = space_data.get("instructions", {}).get("example_question_sqls", [])
    if example_sqls:
        breakdown["sql_assets"] += 10
        if len(example_sqls) >= 3:
            breakdown["sql_assets"] += 8
        else:
            findings.append(f"Only {len(example_sqls)} SQL example(s) - add at least 3")
            next_steps.append("Add at least 3 example SQL questions covering diverse query patterns")
    else:
        findings.append("No example SQL questions configured")
        next_steps.append("Add example SQL questions to teach Genie complex query patterns")

    sql_functions = space_data.get("instructions", {}).get("sql_functions", [])
    expressions = space_data.get("instructions", {}).get("sql_snippets", {}).get("expressions", [])
    measures = space_data.get("instructions", {}).get("sql_snippets", {}).get("measures", [])

    if sql_functions or expressions or measures:
        breakdown["sql_assets"] += 7
    else:
        findings.append("No SQL functions, expressions, or measures configured")
        next_steps.append("Add SQL functions or expression snippets for complex business logic")

    # --- Optimization (20 pts) ---
    benchmarks = space_data.get("benchmarks", {}).get("questions", [])
    if benchmarks:
        breakdown["optimization"] += 8
    else:
        findings.append("No benchmark questions configured")
        next_steps.append("Add benchmark questions to measure and track Genie accuracy")

    text_instructions = space_data.get("instructions", {}).get("text_instructions", [])
    if text_instructions:
        # Check if they have meaningful content
        meaningful = [t for t in text_instructions if len(t.get("content", "")) > 50]
        if meaningful:
            breakdown["optimization"] += 7
        else:
            findings.append("Text instructions are too brief")
            next_steps.append("Expand text instructions with business context and terminology")
    else:
        findings.append("No text instructions configured")
        next_steps.append("Add text instructions to explain business context and terminology")

    join_specs = space_data.get("instructions", {}).get("join_specs", [])
    if join_specs:
        breakdown["optimization"] += 5
    else:
        if len(tables) > 1:
            findings.append("No join specifications for multi-table space")
            next_steps.append("Add join specifications to help Genie correctly join your tables")

    total_score = sum(breakdown.values())
    maturity = get_maturity_label(total_score)

    return {
        "score": total_score,
        "maturity": maturity,
        "breakdown": breakdown,
        "findings": findings[:5],  # Top 5 findings
        "next_steps": next_steps[:5],  # Top 5 next steps
        "scanned_at": datetime.utcnow().isoformat(),
    }


async def scan_space(space_id: str, user_token: Optional[str] = None) -> dict:
    """Fetch space config, calculate IQ score, and persist to Lakebase.

    Args:
        space_id: The Genie Space ID
        user_token: Optional user token for OBO auth (not used directly, SDK handles this)

    Returns:
        ScanResult dict with score, maturity, breakdown, findings, next_steps
    """
    logger.info(f"Scanning space: {space_id}")

    try:
        space_data = get_serialized_space(space_id)
    except Exception as e:
        logger.error(f"Failed to fetch space {space_id}: {e}")
        raise ValueError(f"Cannot scan space {space_id}: {e}")

    scan_result = calculate_score(space_data)
    scan_result["space_id"] = space_id

    # Persist to Lakebase
    try:
        await save_scan_result(space_id, scan_result)
        logger.info(f"Scan result saved for {space_id}: score={scan_result['score']}")
    except Exception as e:
        logger.warning(f"Failed to persist scan result for {space_id}: {e}")

    return scan_result
