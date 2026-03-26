"""IQ scoring engine for Genie Space configurations.

3-tier maturity: Not Ready → Ready to Optimize → Trusted.
12 checks, 1 point each.
"""

import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Optional

from backend.services.genie_client import get_genie_space, get_serialized_space
from backend.services.lakebase import save_scan_result, get_latest_score, get_latest_optimization_run

logger = logging.getLogger(__name__)


# First 10 checks are config checks; the last 2 are optimization checks.
CONFIG_CHECK_COUNT = 10

# Terminal GSO run statuses that indicate a completed optimization.
# Subset of auto_optimize._TERMINAL_RUN_STATUSES — only includes statuses
# where best_accuracy is meaningful for IQ scoring.
_GSO_TERMINAL = {"CONVERGED", "STALLED", "MAX_ITERATIONS"}


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


def _check(checks: list, label: str, passed: bool,
           detail: str | None = None, severity: str | None = None) -> None:
    """Record a check result (1 point each).

    severity: "pass" | "warning" | "fail" (auto-set from passed if None).
    """
    if severity is None:
        severity = "pass" if passed else "fail"
    checks.append({
        "label": label,
        "passed": passed,
        "detail": detail,
        "severity": severity,
    })


_SQL_IN_TEXT_RE = re.compile(
    r"\b(SELECT|WHERE|JOIN|GROUP\s+BY|ORDER\s+BY|HAVING)\b", re.IGNORECASE
)


def calculate_score(space_data: dict, optimization_run: dict | None = None) -> dict:
    """Calculate IQ score for a Genie Space configuration.

    Returns a dict with:
        - score: int (0-12)
        - total: 12
        - maturity: str
        - checks: flat list of {label, passed, detail, severity}
        - findings / next_steps: from failed checks (fix agent input)
        - warnings / warning_next_steps: advisory guidance from warning-severity checks
    """
    checks: list[dict] = []
    findings = []
    next_steps = []
    warnings = []
    warning_next_steps = []

    tables = space_data.get("data_sources", {}).get("tables", [])

    # --- Config checks (1-10) ---

    # 1. Tables exist
    passed = bool(tables)
    _check(checks, "Tables exist", passed,
           detail=f"{len(tables)} table(s) configured" if passed else "No tables configured")
    if not passed:
        findings.append("No tables configured")
        next_steps.append("Add at least one table to your Genie Space")

    # 2. Table descriptions — require ≥80% coverage (Gap 2)
    if tables:
        described_tables = sum(
            1 for t in tables if t.get("description") or t.get("comment")
        )
        total_tables = len(tables)
        pct = described_tables / total_tables
        passed = pct >= 0.80
        detail = f"{described_tables}/{total_tables} tables have descriptions ({pct:.0%})"
        if not passed:
            detail += " — 80%+ required"
        severity = "fail" if not passed else ("warning" if pct < 1.0 else "pass")
        if severity == "warning":
            detail += " — aim for 100%"
        _check(checks, "Table descriptions", passed, detail=detail, severity=severity)
        if not passed:
            findings.append(detail)
            next_steps.append("Add descriptions to your tables to help Genie understand context")
        elif severity == "warning":
            warnings.append(detail)
            warning_next_steps.append("Add descriptions to remaining tables for better Intent Agent routing")
    else:
        _check(checks, "Table descriptions", False, detail="No tables configured", severity="fail")

    # 3. Column descriptions — require ≥50% coverage (Gap 1)
    if tables:
        total_cols = 0
        described_cols = 0
        has_synonyms = False
        for t in tables:
            for col in t.get("columns", []) + t.get("column_configs", []):
                total_cols += 1
                if col.get("description") or col.get("comment"):
                    described_cols += 1
                if col.get("synonyms"):
                    has_synonyms = True
        pct = described_cols / total_cols if total_cols > 0 else 0
        passed = pct >= 0.50
        detail = f"{described_cols}/{total_cols} columns have descriptions ({pct:.0%})"
        if not passed:
            detail += " — 50%+ required"
        severity = "fail" if not passed else ("warning" if pct < 0.80 else "pass")
        if severity == "warning":
            detail += " — aim for 80%+"
        _check(checks, "Column descriptions", passed, detail=detail, severity=severity)
        if not passed:
            findings.append(detail)
            next_steps.append("Add column descriptions to improve query accuracy")
        elif severity == "warning":
            warnings.append(detail)
            warning_next_steps.append("Higher coverage improves SQL generation accuracy")
        # Advisory: column synonyms (Gap 7)
        if passed and not has_synonyms:
            warnings.append("No column synonyms defined")
            warning_next_steps.append("Add synonyms for columns with abbreviated or technical names")
    else:
        _check(checks, "Column descriptions", False, detail="No tables configured", severity="fail")

    # 4. Text instructions > 50 chars (Gap 6: length + SQL-in-text warnings)
    text_instructions = space_data.get("instructions", {}).get("text_instructions", [])
    total_chars = 0
    all_text = ""
    for t in text_instructions:
        content = t.get("content", "")
        text = "".join(content) if isinstance(content, list) else content
        total_chars += len(text)
        all_text += text
    passed = bool(text_instructions) and total_chars > 50
    detail = f"{len(text_instructions)} instruction(s), {total_chars:,} chars total" if text_instructions else "No text instructions configured"
    severity = "pass" if passed else "fail"
    # Check for warnings on passing check
    ti_warnings = []
    if passed:
        if total_chars > 2000:
            ti_warnings.append(f"Instructions total {total_chars:,} chars — keep under 2,000 to avoid pushing out higher-value SQL context")
        if _SQL_IN_TEXT_RE.search(all_text):
            ti_warnings.append("SQL patterns found in text instructions — move to Example SQLs or SQL Expressions")
        if ti_warnings:
            severity = "warning"
            detail += f" — {len(ti_warnings)} warning(s)"
    _check(checks, "Text instructions (>50 chars)", passed, detail=detail, severity=severity)
    if not passed:
        findings.append("No text instructions configured" if not text_instructions else "Text instructions are too brief")
        next_steps.append("Add text instructions to explain business context and terminology")
    for w in ti_warnings:
        warnings.append(w)
        warning_next_steps.append("Restructure text instructions for optimal LLM context usage")

    # 5. Join specifications
    join_specs = space_data.get("instructions", {}).get("join_specs", [])
    passed = bool(join_specs)
    detail = f"{len(join_specs)} join spec(s) for {len(tables)} tables" if passed else None
    _check(checks, "Join specifications", passed, detail=detail)
    if not passed and len(tables) > 1:
        findings.append("No join specifications for multi-table space")
        next_steps.append("Add join specifications to help Genie correctly join your tables")

    # 6. Table count 1-12 (Gap 10: adjusted from 1-10)
    table_count = len(tables)
    passed = 1 <= table_count <= 12
    detail = f"{table_count} tables"
    severity = "pass"
    if not passed and table_count > 12:
        detail += " — consider multi-room architecture"
        severity = "fail"
    elif passed and table_count > 8:
        detail += " — consider splitting into focused rooms for >8 tables"
        severity = "warning"
    _check(checks, "Table count 1-12", passed, detail=detail, severity=severity)
    if not passed and tables:
        if table_count > 12:
            findings.append(f"{table_count} tables — more than 12 reduces Genie accuracy")
            next_steps.append("Consider multi-room architecture or reducing to the most relevant 5-12 tables")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Consider splitting into focused rooms for better accuracy")

    # 7. 8+ example SQLs (Gap 4: tightened from 5; Gap 9: usage_guidance check)
    example_sqls = space_data.get("instructions", {}).get("example_question_sqls", [])
    n_examples = len(example_sqls)
    passed = n_examples >= 8
    detail = f"{n_examples} example SQLs"
    severity = "pass" if passed else "fail"
    if not passed:
        detail += " — 8+ required"
    elif n_examples < 15:
        detail += " — 10-15 is the sweet spot for largest accuracy jump"
        severity = "warning"
    _check(checks, "8+ example SQLs", passed, detail=detail, severity=severity)
    if not passed:
        if example_sqls:
            findings.append(f"Only {n_examples} SQL example(s) — add at least 8")
        else:
            findings.append("No example SQL questions configured")
        next_steps.append("Add at least 8 example SQL questions covering diverse query patterns")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Adding 10-15 example SQLs provides the largest single accuracy improvement")
    # Advisory: usage_guidance (Gap 9)
    if example_sqls:
        missing_guidance = sum(1 for e in example_sqls if not e.get("usage_guidance"))
        if missing_guidance > len(example_sqls) / 2:
            warnings.append(f"{missing_guidance}/{n_examples} example SQLs lack usage_guidance")
            warning_next_steps.append("Add descriptions of when each example should be applied")

    # 8. SQL snippets (Gap 8: type breakdown detail)
    sql_functions = space_data.get("instructions", {}).get("sql_functions", [])
    sql_snippets = space_data.get("instructions", {}).get("sql_snippets", {})
    expressions = sql_snippets.get("expressions", [])
    measures = sql_snippets.get("measures", [])
    filters = sql_snippets.get("filters", [])
    passed = bool(sql_functions or expressions or measures or filters)
    detail = f"{len(sql_functions)} functions, {len(measures)} measures, {len(filters)} filters, {len(expressions)} expressions"
    severity = "pass" if passed else "fail"
    if passed and (not filters or not measures):
        missing = []
        if not filters:
            missing.append("filters")
        if not measures:
            missing.append("measures")
        detail += f" — add {' and '.join(missing)} for better coverage"
        severity = "warning"
    _check(checks, "SQL snippets (functions/expressions/measures/filters)", passed,
           detail=detail, severity=severity)
    if not passed:
        findings.append("No SQL functions, expressions, measures, or filters configured")
        next_steps.append("Add SQL snippets for complex business logic, common filters, and calculated measures")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Add missing SQL snippet types for better query coverage")

    # 9. Entity/format matching (Gap 5: count + RLS detection)
    entity_count = 0
    format_count = 0
    rls_tables = []
    for t in tables:
        table_has_rls = bool(t.get("row_filter") or t.get("column_mask"))
        for col in t.get("column_configs", []) + t.get("columns", []):
            if col.get("enable_entity_matching"):
                entity_count += 1
            if col.get("enable_format_assistance") or col.get("format_assistance_enabled"):
                format_count += 1
            if col.get("row_filter") or col.get("column_mask"):
                table_has_rls = True
        if table_has_rls:
            table_name = t.get("name") or t.get("table_name") or "unknown"
            rls_tables.append(table_name)
    entity_or_format = entity_count > 0 or format_count > 0
    detail = f"{entity_count} columns with entity matching, {format_count} with format assistance"
    severity = "pass" if entity_or_format else "fail"
    if entity_or_format:
        if entity_count > 120:
            detail += f" — exceeds 120/space limit, excess will be ignored"
            severity = "warning"
        elif entity_count > 100:
            detail += f" — approaching 120/space limit"
            severity = "warning"
    _check(checks, "Entity/format matching", entity_or_format, detail=detail, severity=severity)
    if not entity_or_format and tables:
        findings.append("No columns have entity matching or format assistance enabled")
        next_steps.append("Enable entity matching on categorical columns and format assistance on date/number columns")
    elif severity == "warning":
        warnings.append(detail)
        warning_next_steps.append("Reduce entity matching columns to stay within the 120/space limit")
    # Advisory: RLS disables entity matching silently
    if rls_tables and entity_or_format:
        rls_msg = f"Tables with row-level security ({', '.join(rls_tables[:3])}) — entity matching is silently disabled for these"
        warnings.append(rls_msg)
        warning_next_steps.append("Entity matching won't work on tables with row filters or column masks")

    # 10. 10+ benchmark questions
    benchmarks = space_data.get("benchmarks", {}).get("questions", [])
    n_benchmarks = len(benchmarks)
    passed = n_benchmarks >= 10
    detail = f"{n_benchmarks} benchmark question(s)"
    _check(checks, "10+ benchmark questions", passed, detail=detail)
    if not passed:
        if benchmarks:
            findings.append(f"Only {n_benchmarks} benchmark question(s) — add at least 10")
        else:
            findings.append("No benchmark questions configured")
        next_steps.append("Add at least 10 benchmark questions to measure and track Genie accuracy")

    # --- Optimization checks (11-12) ---

    # 11. Optimization run recorded
    has_run = bool(optimization_run)
    _check(checks, "Optimization workflow completed", has_run)
    if not has_run:
        findings.append("Space has not been through the optimization workflow")
        next_steps.append("Benchmark and improve Genie's accuracy with Optimization")

    # 12. Accuracy ≥ 85%
    accuracy = optimization_run.get("accuracy", 0) if optimization_run else 0
    passed = has_run and accuracy >= 0.85
    detail = f"Accuracy: {accuracy:.0%}" if has_run else None
    _check(checks, "Optimization accuracy ≥ 85%", passed, detail=detail)
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
        "findings": findings[:8],
        "next_steps": next_steps[:8],
        "warnings": warnings[:8],
        "warning_next_steps": warning_next_steps[:8],
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

    # Fetch optimization runs from both sources concurrently
    async def _fetch_opt_run():
        try:
            return await get_latest_optimization_run(space_id)
        except Exception as e:
            logger.warning(f"Failed to fetch optimization run for {space_id}: {e}")
            return None

    async def _fetch_gso_runs():
        try:
            from backend.services import gso_lakebase
            runs = await gso_lakebase.load_gso_runs_for_space(space_id)
            # Delta table fallback when Lakebase synced tables are empty
            if not runs:
                catalog = os.environ.get("GSO_CATALOG", "")
                schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
                wh_id = os.environ.get("GSO_WAREHOUSE_ID") or os.environ.get("SQL_WAREHOUSE_ID", "")
                if catalog and wh_id:
                    try:
                        from genie_space_optimizer.common.warehouse import sql_warehouse_query
                        from backend.services.auth import get_workspace_client
                        ws = get_workspace_client()
                        df = sql_warehouse_query(
                            ws, wh_id,
                            f"SELECT run_id, space_id, status, best_accuracy, completed_at, started_at "
                            f"FROM {catalog}.{schema}.genie_opt_runs "
                            f"WHERE space_id = '{space_id}' ORDER BY started_at DESC"
                        )
                        if not df.empty:
                            runs = df.to_dict(orient="records")
                    except Exception as e:
                        logger.warning(f"GSO Delta fallback failed for {space_id}: {e}")
            return runs or []
        except Exception as e:
            logger.warning(f"Failed to check GSO runs for {space_id}: {e}")
            return []

    optimization_run, gso_runs = await asyncio.gather(
        _fetch_opt_run(), _fetch_gso_runs()
    )

    # Use the best accuracy from either source
    for gso_run in gso_runs:  # already sorted most recent first
        status = str(gso_run.get("status", "")).upper()
        best_acc = gso_run.get("best_accuracy")
        if status in _GSO_TERMINAL and best_acc is not None:
            acc = float(best_acc)
            # GSO stores accuracy as percentage (0-100); normalize to 0.0-1.0
            if acc > 1.0:
                acc = acc / 100.0
            gso_as_opt = {
                "accuracy": acc,
                "created_at": gso_run.get("completed_at") or gso_run.get("started_at"),
            }
            if optimization_run is None or gso_as_opt["accuracy"] > optimization_run.get("accuracy", 0):
                optimization_run = gso_as_opt
            break  # only consider most recent terminal GSO run

    scan_result = calculate_score(space_data, optimization_run=optimization_run)
    scan_result["space_id"] = space_id

    # Persist to Lakebase
    try:
        await save_scan_result(space_id, scan_result)
        logger.info(f"Scan result saved for {space_id}: score={scan_result['score']}")
    except Exception as e:
        logger.warning(f"Failed to persist scan result for {space_id}: {e}")

    return scan_result
