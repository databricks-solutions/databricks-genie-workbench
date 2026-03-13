"""Config-driven scoring engine for Genie Space maturity.

Criteria check functions are registered by ID. The maturity config
(YAML default + admin overrides) controls which criteria are active,
their point weights, and stage thresholds.
"""

import logging
from datetime import datetime
from typing import Callable, Optional

from backend.services.genie_client import get_serialized_space
from backend.services.lakebase import save_scan_result
from backend.services.maturity_config import get_active_config

logger = logging.getLogger(__name__)

# Type for a criterion check function.
# Takes space_data dict, returns:
#   - bool for boolean criteria (pass/fail)
#   - float for count criteria (raw value, scaled by config)
CheckFn = Callable[[dict], bool | float]

# --- Criterion check functions (keyed by criterion id) ---

_CHECKS: dict[str, CheckFn] = {}


def _register(criterion_id: str):
    """Decorator to register a check function for a criterion ID."""
    def decorator(fn: CheckFn) -> CheckFn:
        _CHECKS[criterion_id] = fn
        return fn
    return decorator


# Nascent stage checks

@_register("tables_attached")
def _tables_attached(space: dict) -> bool:
    return len(space.get("data_sources", {}).get("tables", [])) > 0


@_register("table_count")
def _table_count(space: dict) -> float:
    return float(len(space.get("data_sources", {}).get("tables", [])))


@_register("columns_exist")
def _columns_exist(space: dict) -> bool:
    tables = space.get("data_sources", {}).get("tables", [])
    return any(len(t.get("columns", [])) > 0 for t in tables)


# Basic / Developing stage checks

@_register("instructions_defined")
def _instructions_defined(space: dict) -> bool:
    return len(space.get("instructions", {}).get("text_instructions", [])) > 0


@_register("instruction_quality")
def _instruction_quality(space: dict) -> float:
    texts = space.get("instructions", {}).get("text_instructions", [])
    return float(sum(1 for t in texts if len(t.get("content", "")) > 50))


@_register("sample_questions")
def _sample_questions(space: dict) -> float:
    return float(len(space.get("instructions", {}).get("example_question_sqls", [])))


@_register("joins_defined")
def _joins_defined(space: dict) -> bool:
    return len(space.get("instructions", {}).get("join_specs", [])) > 0


@_register("column_descriptions")
def _column_descriptions(space: dict) -> float:
    tables = space.get("data_sources", {}).get("tables", [])
    total_cols = 0
    described_cols = 0
    for t in tables:
        for col in t.get("columns", []):
            total_cols += 1
            if col.get("description") or col.get("comment"):
                described_cols += 1
    return described_cols / total_cols if total_cols > 0 else 0.0


# Proficient stage checks

@_register("trusted_sql_queries")
def _trusted_sql_queries(space: dict) -> float:
    return float(len(space.get("instructions", {}).get("example_question_sqls", [])))


@_register("filter_snippets")
def _filter_snippets(space: dict) -> bool:
    return len(space.get("instructions", {}).get("sql_snippets", {}).get("filters", [])) > 0


@_register("expressions_defined")
def _expressions_defined(space: dict) -> float:
    snippets = space.get("instructions", {}).get("sql_snippets", {})
    return float(
        len(snippets.get("expressions", []))
        + len(snippets.get("measures", []))
    )


@_register("table_descriptions")
def _table_descriptions(space: dict) -> bool:
    tables = space.get("data_sources", {}).get("tables", [])
    return any(t.get("description") or t.get("comment") for t in tables)


# Optimized stage checks

@_register("benchmark_questions")
def _benchmark_questions(space: dict) -> float:
    return float(len(space.get("benchmarks", {}).get("questions", [])))


@_register("sql_coverage")
def _sql_coverage(space: dict) -> float:
    return float(len(space.get("instructions", {}).get("example_question_sqls", [])))


@_register("unity_catalog")
def _unity_catalog(space: dict) -> bool:
    tables = space.get("data_sources", {}).get("tables", [])
    if not tables:
        return False
    return all(
        len(t.get("table_name", "").split(".")) == 3 or t.get("catalog")
        for t in tables
    )


@_register("sql_functions")
def _sql_functions(space: dict) -> bool:
    return len(space.get("instructions", {}).get("sql_functions", [])) > 0


# --- Scoring engine ---


def _scale_count(value: float, scale: dict) -> float:
    """Scale a count value to 0.0-1.0 based on config scale thresholds."""
    target = scale.get("target", 1)
    if target <= 0:
        return 1.0 if value > 0 else 0.0
    return min(value / target, 1.0)


def get_maturity_stage(score: int, stages: list[dict]) -> str:
    """Return the maturity stage name for a given score."""
    for stage in reversed(stages):
        low = stage["range"][0]
        if score >= low:
            return stage["name"]
    return stages[0]["name"] if stages else "Unknown"


def calculate_score(space_data: dict, config: dict) -> dict:
    """Calculate maturity score using the provided config.

    Returns a dict with score, maturity stage, breakdown by stage,
    per-criterion results, findings, and next_steps.
    """
    stages = config.get("stages", [])
    criteria = config.get("criteria", [])

    # Track points by stage
    stage_points: dict[str, int] = {}
    for stage in stages:
        stage_points[stage["name"]] = 0

    criteria_results = []
    findings = []
    next_steps = []

    for criterion in criteria:
        if not criterion.get("enabled", True):
            continue

        cid = criterion["id"]
        stage = criterion["stage"]
        ctype = criterion["type"]
        max_points = criterion["points"]
        description = criterion["description"]

        check_fn = _CHECKS.get(cid)
        if check_fn is None:
            logger.warning("No check function registered for criterion: %s", cid)
            continue

        try:
            result = check_fn(space_data)
        except Exception as e:
            logger.warning("Check %s failed: %s", cid, e)
            result = False if ctype == "boolean" else 0.0

        if ctype == "boolean":
            passed = bool(result)
            points = max_points if passed else 0
            criteria_results.append({
                "id": cid,
                "stage": stage,
                "description": description,
                "passed": passed,
                "value": None,
                "points_earned": points,
                "points_possible": max_points,
            })
            if not passed:
                findings.append(f"Missing: {description}")
                next_steps.append(description)
        else:
            # Count type — scale value
            raw_value = float(result)
            scale = criterion.get("scale", {"target": 1})
            ratio = _scale_count(raw_value, scale)
            points = round(max_points * ratio)
            passed = points > 0
            criteria_results.append({
                "id": cid,
                "stage": stage,
                "description": description,
                "passed": passed,
                "value": raw_value,
                "points_earned": points,
                "points_possible": max_points,
            })
            if ratio < 1.0:
                target = scale.get("target", 1)
                findings.append(f"Below target: {description} ({raw_value:.0f}/{target})")
                next_steps.append(f"Improve: {description}")

        if stage in stage_points:
            stage_points[stage] += points

    total_score = sum(stage_points.values())
    # Cap at 100
    total_score = min(total_score, 100)

    maturity = get_maturity_stage(total_score, stages)

    breakdown = {
        "nascent": stage_points.get("Nascent", 0),
        "basic": stage_points.get("Basic", 0),
        "developing": stage_points.get("Developing", 0),
        "proficient": stage_points.get("Proficient", 0),
        "optimized": stage_points.get("Optimized", 0),
    }

    return {
        "score": total_score,
        "maturity": maturity,
        "breakdown": breakdown,
        "criteria_results": criteria_results,
        "findings": findings[:5],
        "next_steps": next_steps[:5],
        "scanned_at": datetime.utcnow().isoformat(),
    }


async def scan_space(space_id: str, user_token: Optional[str] = None) -> dict:
    """Fetch space config, calculate score using active maturity config, persist.

    Args:
        space_id: The Genie Space ID
        user_token: Optional user token for OBO auth

    Returns:
        Scan result dict with score, maturity, breakdown, criteria_results, findings, next_steps
    """
    logger.info("Scanning space: %s", space_id)

    try:
        space_data = get_serialized_space(space_id)
    except Exception as e:
        logger.error("Failed to fetch space %s: %s", space_id, e)
        raise ValueError(f"Cannot scan space {space_id}: {e}")

    config = await get_active_config()
    scan_result = calculate_score(space_data, config)
    scan_result["space_id"] = space_id

    try:
        await save_scan_result(space_id, scan_result)
        logger.info("Scan result saved for %s: score=%s", space_id, scan_result["score"])
    except Exception as e:
        logger.warning("Failed to persist scan result for %s: %s", space_id, e)

    return scan_result
