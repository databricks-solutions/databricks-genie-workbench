"""IQ scoring engine for Genie Space configurations.

3-tier maturity: Not Ready → Ready to Optimize → Trusted.
12 checks, 1 point each.
"""

import logging
from datetime import datetime
from typing import Optional

from backend.services.genie_client import get_genie_space, get_serialized_space
from backend.services.lakebase import save_scan_result, get_latest_score, get_latest_optimization_run

logger = logging.getLogger(__name__)


# First 10 checks are config checks; the last 2 are optimization checks.
CONFIG_CHECK_COUNT = 10


def get_maturity_label(checks: list[dict]) -> str:
    """Return maturity label based on which checks pass.

    - All checks pass → Trusted
    - First CONFIG_CHECK_COUNT (config) checks pass → Ready to Optimize
    - Otherwise → Not Ready
    """
    if all(c["passed"] for c in checks):
        return "Trusted"
    if all(c["passed"] for c in checks[:CONFIG_CHECK_COUNT]):
        return "Ready to Optimize"
    return "Not Ready"


def _check(checks: list, label: str, passed: bool) -> None:
    """Record a check result (1 point each)."""
    checks.append({
        "label": label,
        "passed": passed,
    })


def calculate_score(space_data: dict, optimization_run: dict | None = None) -> dict:
    """Calculate IQ score for a Genie Space configuration.

    Returns a dict with:
        - score: int (0-12)
        - total: 12
        - maturity: str
        - checks: flat list of {label, passed}
        - findings: list of finding strings
        - next_steps: list of recommended actions
    """
    checks: list[dict] = []
    findings = []
    next_steps = []

    tables = space_data.get("data_sources", {}).get("tables", [])

    # --- Config checks (1-10) ---

    # 1. Tables exist
    passed = bool(tables)
    _check(checks, "Tables exist", passed)
    if not passed:
        findings.append("No tables configured")
        next_steps.append("Add at least one table to your Genie Space")

    # 2. Table descriptions
    passed = bool(tables and any(t.get("description") or t.get("comment") for t in tables))
    _check(checks, "Table descriptions", passed)
    if not passed and tables:
        findings.append("Tables have no descriptions")
        next_steps.append("Add descriptions to your tables to help Genie understand context")

    # 3. Column descriptions
    passed = bool(tables) and any(
        any(col.get("description") or col.get("comment")
            for col in t.get("columns", []) + t.get("column_configs", []))
        for t in tables
    )
    _check(checks, "Column descriptions", passed)
    if not passed and tables:
        findings.append("Columns have no descriptions")
        next_steps.append("Add column descriptions to improve query accuracy")

    # 4. Text instructions > 50 chars
    text_instructions = space_data.get("instructions", {}).get("text_instructions", [])
    passed = bool(text_instructions) and any(
        len("".join(t.get("content", [])) if isinstance(t.get("content"), list) else t.get("content", "")) > 50
        for t in text_instructions
    )
    _check(checks, "Text instructions (>50 chars)", passed)
    if not passed:
        findings.append("No text instructions configured" if not text_instructions else "Text instructions are too brief")
        next_steps.append("Add text instructions to explain business context and terminology")

    # 5. Join specifications
    join_specs = space_data.get("instructions", {}).get("join_specs", [])
    passed = bool(join_specs)
    _check(checks, "Join specifications", passed)
    if not passed and len(tables) > 1:
        findings.append("No join specifications for multi-table space")
        next_steps.append("Add join specifications to help Genie correctly join your tables")

    # 6. Table count 1-10
    table_count = len(tables)
    passed = 1 <= table_count <= 10
    _check(checks, "Table count 1-10", passed)
    if not passed and tables:
        if table_count > 10:
            findings.append("More than 10 tables may reduce Genie accuracy")
            next_steps.append("Consider reducing to the most relevant 5-10 tables")

    # 7. 5+ example SQLs
    example_sqls = space_data.get("instructions", {}).get("example_question_sqls", [])
    passed = len(example_sqls) >= 5
    _check(checks, "5+ example SQLs", passed)
    if not passed:
        if example_sqls:
            findings.append(f"Only {len(example_sqls)} SQL example(s) — add at least 5")
        else:
            findings.append("No example SQL questions configured")
        next_steps.append("Add at least 5 example SQL questions covering diverse query patterns")

    # 8. SQL snippets (functions/expressions/measures/filters)
    sql_functions = space_data.get("instructions", {}).get("sql_functions", [])
    sql_snippets = space_data.get("instructions", {}).get("sql_snippets", {})
    expressions = sql_snippets.get("expressions", [])
    measures = sql_snippets.get("measures", [])
    filters = sql_snippets.get("filters", [])
    passed = bool(sql_functions or expressions or measures or filters)
    _check(checks, "SQL snippets (functions/expressions/measures/filters)", passed)
    if not passed:
        findings.append("No SQL functions, expressions, measures, or filters configured")
        next_steps.append("Add SQL snippets for complex business logic, common filters, and calculated measures")

    # 9. Entity/format matching
    entity_or_format = False
    for t in tables:
        for col in t.get("column_configs", []) + t.get("columns", []):
            if col.get("enable_entity_matching") or col.get("enable_format_assistance") or col.get("format_assistance_enabled"):
                entity_or_format = True
                break
        if entity_or_format:
            break
    _check(checks, "Entity/format matching", entity_or_format)
    if not entity_or_format and tables:
        findings.append("No columns have entity matching or format assistance enabled")
        next_steps.append("Enable entity matching on categorical columns and format assistance on date/number columns")

    # 10. 10+ benchmark questions
    benchmarks = space_data.get("benchmarks", {}).get("questions", [])
    passed = len(benchmarks) >= 10
    _check(checks, "10+ benchmark questions", passed)
    if not passed:
        if benchmarks:
            findings.append(f"Only {len(benchmarks)} benchmark question(s) — add at least 10")
        else:
            findings.append("No benchmark questions configured")
        next_steps.append("Add at least 10 benchmark questions to measure and track Genie accuracy")

    # --- Optimization checks (11-12) ---

    # 11. Optimization run recorded
    has_run = bool(optimization_run)
    _check(checks, "Optimization workflow completed", has_run)
    if not has_run:
        findings.append("Space has not been through the optimization workflow")
        next_steps.append("Use the Optimize tab to benchmark and improve Genie's accuracy")

    # 12. Accuracy ≥ 85%
    accuracy = optimization_run.get("accuracy", 0) if optimization_run else 0
    passed = has_run and accuracy >= 0.85
    _check(checks, "Optimization accuracy ≥ 85%", passed)
    if has_run and not passed:
        findings.append(f"Optimization accuracy is {accuracy:.0%} — target ≥ 85%")
        next_steps.append("Re-run the optimization workflow to improve benchmark accuracy to 85%+")

    score = sum(1 for c in checks if c["passed"])
    maturity = get_maturity_label(checks)

    return {
        "score": score,
        "total": 12,
        "maturity": maturity,
        "checks": checks,
        "optimization_accuracy": accuracy if optimization_run else None,
        "findings": findings[:5],
        "next_steps": next_steps[:5],
        "scanned_at": datetime.utcnow().isoformat(),
    }


async def scan_space(space_id: str, user_token: Optional[str] = None) -> dict:
    """Fetch space config, calculate IQ score, and persist to Lakebase.

    Args:
        space_id: The Genie Space ID
        user_token: Optional user token for OBO auth (not used directly, SDK handles this)

    Returns:
        ScanResult dict with score, maturity, breakdown, checks, findings, next_steps
    """
    logger.info(f"Scanning space: {space_id}")

    try:
        space_data = get_serialized_space(space_id)
    except Exception as e:
        logger.error(f"Failed to fetch space {space_id}: {e}")
        raise ValueError(f"Cannot scan space {space_id}: {e}")

    # Fetch latest optimization run for Trusted tier scoring
    optimization_run = None
    try:
        optimization_run = await get_latest_optimization_run(space_id)
    except Exception as e:
        logger.warning(f"Failed to fetch optimization run for {space_id}: {e}")

    scan_result = calculate_score(space_data, optimization_run=optimization_run)
    scan_result["space_id"] = space_id

    # Persist to Lakebase
    try:
        await save_scan_result(space_id, scan_result)
        logger.info(f"Scan result saved for {space_id}: score={scan_result['score']}")
    except Exception as e:
        logger.warning(f"Failed to persist scan result for {space_id}: {e}")

    return scan_result
