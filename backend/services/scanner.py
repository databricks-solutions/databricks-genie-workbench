"""IQ scoring engine for Genie Space configurations.

4-tier maturity curve: Connected → Configured → Calibrated → Trusted
14 rules, 100 total points.
"""

import logging
from datetime import datetime
from typing import Optional

from backend.services.genie_client import get_genie_space, get_serialized_space
from backend.services.lakebase import save_scan_result, get_latest_score, get_latest_optimization_run

logger = logging.getLogger(__name__)

# Maturity levels based on score (4-tier maturity curve)
MATURITY_LEVELS = [
    (76, "Trusted"),
    (51, "Calibrated"),
    (26, "Configured"),
    (0, "Connected"),
]


def get_maturity_label(score: int) -> str:
    """Return maturity label for a given score (0-100)."""
    for threshold, label in MATURITY_LEVELS:
        if score >= threshold:
            return label
    return "Connected"


def _check(checks: list, label: str, max_points: int, passed: bool) -> int:
    """Record a check result and return points earned."""
    points = max_points if passed else 0
    checks.append({
        "label": label,
        "points": points,
        "max_points": max_points,
        "passed": passed,
    })
    return points


def calculate_score(space_data: dict, optimization_run: dict | None = None) -> dict:
    """Calculate IQ score for a Genie Space configuration.

    Returns a dict with:
        - score: int (0-100)
        - maturity: str
        - breakdown: dict with tier scores
        - checks: dict mapping tier -> list of {label, points, max_points, passed}
        - findings: list of finding strings
        - next_steps: list of recommended actions
    """
    breakdown = {
        "connected": 0,
        "configured": 0,
        "calibrated": 0,
        "trusted": 0,
        "optimized": 0,
    }
    checks: dict[str, list] = {
        "connected": [],
        "configured": [],
        "calibrated": [],
        "trusted": [],
        "optimized": [],
    }
    findings = []
    next_steps = []

    tables = space_data.get("data_sources", {}).get("tables", [])

    # --- Connected (20 pts) ---

    # Tables exist → 8 pts
    passed = bool(tables)
    breakdown["connected"] += _check(checks["connected"], "Tables exist", 8, passed)
    if not passed:
        findings.append("No tables configured")
        next_steps.append("Add at least one table to your Genie Space")

    # Table descriptions → 6 pts
    passed = bool(tables and any(t.get("description") or t.get("comment") for t in tables))
    breakdown["connected"] += _check(checks["connected"], "Table descriptions", 6, passed)
    if not passed and tables:
        findings.append("Tables have no descriptions")
        next_steps.append("Add descriptions to your tables to help Genie understand context")

    # Column descriptions → 4 pts
    passed = bool(tables) and any(
        any(col.get("description") or col.get("comment")
            for col in t.get("columns", []) + t.get("column_configs", []))
        for t in tables
    )
    breakdown["connected"] += _check(checks["connected"], "Column descriptions", 4, passed)
    if not passed and tables:
        findings.append("Columns have no descriptions")
        next_steps.append("Add column descriptions to improve query accuracy")

    # UC 3-part names → 2 pts
    passed = bool(tables) and any(
        len((t.get("table_name") or t.get("identifier") or "").split(".")) == 3
        or t.get("catalog")
        for t in tables
    )
    breakdown["connected"] += _check(checks["connected"], "UC 3-part names", 2, passed)
    if not passed and tables:
        findings.append("Tables may not be using Unity Catalog")
        next_steps.append("Use fully-qualified Unity Catalog table names (catalog.schema.table)")

    # --- Configured (20 pts) ---

    # Text instructions > 50 chars → 6 pts
    text_instructions = space_data.get("instructions", {}).get("text_instructions", [])
    passed = bool(text_instructions) and any(
        len("".join(t.get("content", [])) if isinstance(t.get("content"), list) else t.get("content", "")) > 50
        for t in text_instructions
    )
    breakdown["configured"] += _check(checks["configured"], "Text instructions (>50 chars)", 6, passed)
    if not passed:
        findings.append("No text instructions configured" if not text_instructions else "Text instructions are too brief")
        next_steps.append("Add text instructions to explain business context and terminology")

    # Filter snippets → 5 pts
    filter_snippets = space_data.get("instructions", {}).get("sql_snippets", {}).get("filters", [])
    passed = bool(filter_snippets)
    breakdown["configured"] += _check(checks["configured"], "Filter snippets", 5, passed)
    if not passed:
        findings.append("No filter snippets defined")
        next_steps.append("Add filter snippets for common time ranges and business segments")

    # Join specs (if >1 table) → 5 pts
    join_specs = space_data.get("instructions", {}).get("join_specs", [])
    passed = bool(join_specs)
    breakdown["configured"] += _check(checks["configured"], "Join specifications", 5, passed)
    if not passed and len(tables) > 1:
        findings.append("No join specifications for multi-table space")
        next_steps.append("Add join specifications to help Genie correctly join your tables")

    # Table count 2-10 → 4 pts
    table_count = len(tables)
    passed = 2 <= table_count <= 10
    breakdown["configured"] += _check(checks["configured"], "Table count 2-10", 4, passed)
    if not passed and tables:
        if table_count == 1:
            findings.append("Only 1 table configured — consider adding related tables")
            next_steps.append("Add 2-5 related tables for better cross-table query capability")
        elif table_count > 10:
            findings.append("More than 10 tables may reduce Genie accuracy")
            next_steps.append("Consider reducing to the most relevant 5-10 tables")

    # --- Calibrated (20 pts) ---

    # 5+ example SQLs → 8 pts
    example_sqls = space_data.get("instructions", {}).get("example_question_sqls", [])
    passed = len(example_sqls) >= 5
    breakdown["calibrated"] += _check(checks["calibrated"], "5+ example SQLs", 8, passed)
    if not passed:
        if example_sqls:
            findings.append(f"Only {len(example_sqls)} SQL example(s) — add at least 5")
        else:
            findings.append("No example SQL questions configured")
        next_steps.append("Add at least 5 example SQL questions covering diverse query patterns")

    # SQL functions/expressions/measures → 8 pts
    sql_functions = space_data.get("instructions", {}).get("sql_functions", [])
    expressions = space_data.get("instructions", {}).get("sql_snippets", {}).get("expressions", [])
    measures = space_data.get("instructions", {}).get("sql_snippets", {}).get("measures", [])
    passed = bool(sql_functions or expressions or measures)
    breakdown["calibrated"] += _check(checks["calibrated"], "SQL functions/expressions/measures", 8, passed)
    if not passed:
        findings.append("No SQL functions, expressions, or measures configured")
        next_steps.append("Add SQL functions or expression snippets for complex business logic")

    # Entity/format matching → 4 pts
    entity_or_format = False
    for t in tables:
        for col in t.get("column_configs", []) + t.get("columns", []):
            if col.get("enable_entity_matching") or col.get("enable_format_assistance") or col.get("format_assistance_enabled"):
                entity_or_format = True
                break
        if entity_or_format:
            break
    breakdown["calibrated"] += _check(checks["calibrated"], "Entity/format matching", 4, entity_or_format)
    if not entity_or_format and tables:
        findings.append("No columns have entity matching or format assistance enabled")
        next_steps.append("Enable entity matching on categorical columns and format assistance on date/number columns")

    # --- Trusted (20 pts) ---

    # 10+ benchmark questions → 10 pts
    benchmarks = space_data.get("benchmarks", {}).get("questions", [])
    passed = len(benchmarks) >= 10
    breakdown["trusted"] += _check(checks["trusted"], "10+ benchmark questions", 10, passed)
    if not passed:
        if benchmarks:
            findings.append(f"Only {len(benchmarks)} benchmark question(s) — add at least 10")
        else:
            findings.append("No benchmark questions configured")
        next_steps.append("Add at least 10 benchmark questions to measure and track Genie accuracy")

    # Metric views → 10 pts
    metric_views = space_data.get("data_sources", {}).get("metric_views", [])
    passed = bool(metric_views)
    breakdown["trusted"] += _check(checks["trusted"], "Metric views exist", 10, passed)
    if not passed:
        findings.append("No metric views configured")
        next_steps.append("Add metric views for pre-aggregated business metrics")

    # --- Optimized (20 pts) ---

    # Optimization run recorded → 8 pts
    has_run = bool(optimization_run)
    breakdown["optimized"] += _check(checks["optimized"], "Optimization workflow completed", 8, has_run)
    if not has_run:
        findings.append("Space has not been through the optimization workflow")
        next_steps.append("Use the Optimize tab to benchmark and improve Genie's accuracy")

    # Accuracy ≥ 85% → 12 pts
    accuracy = optimization_run.get("accuracy", 0) if optimization_run else 0
    passed = has_run and accuracy >= 0.85
    breakdown["optimized"] += _check(checks["optimized"], "Optimization accuracy ≥ 85%", 12, passed)
    if has_run and not passed:
        findings.append(f"Optimization accuracy is {accuracy:.0%} — target ≥ 85%")
        next_steps.append("Re-run the optimization workflow to improve benchmark accuracy to 85%+")

    total_score = sum(breakdown.values())
    maturity = get_maturity_label(total_score)

    return {
        "score": total_score,
        "maturity": maturity,
        "breakdown": breakdown,
        "checks": checks,
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

    # Fetch latest optimization run for Optimized tier scoring
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
