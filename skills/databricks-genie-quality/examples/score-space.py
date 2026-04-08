"""Score a Genie Space against the maturity model.

Usage (Databricks notebook or local with token):
    python score-space.py <space_id>

Retrieves the serialized space config via empty PATCH, evaluates 16 criteria,
and prints a score report with maturity stage, breakdown, and next steps.
"""

import json
import sys

import requests


# --- Configuration ---

CRITERIA = [
    # Nascent
    {"id": "tables_attached", "stage": "Nascent", "type": "boolean", "points": 10,
     "description": "At least one table attached"},
    {"id": "table_count", "stage": "Nascent", "type": "count", "points": 10,
     "description": "Number of tables (target: 5)",
     "scale": {"target": 5, "max": 10}},
    {"id": "columns_exist", "stage": "Nascent", "type": "boolean", "points": 5,
     "description": "Tables have columns defined"},
    # Basic
    {"id": "instructions_defined", "stage": "Basic", "type": "boolean", "points": 5,
     "description": "Text instructions are set"},
    {"id": "table_descriptions", "stage": "Basic", "type": "boolean", "points": 5,
     "description": "Tables have descriptions"},
    {"id": "column_descriptions", "stage": "Basic", "type": "count", "points": 5,
     "description": "Proportion of columns with descriptions (target: 80%)",
     "scale": {"target": 0.8, "max": 1.0}},
    # Developing
    {"id": "instruction_quality", "stage": "Developing", "type": "count", "points": 5,
     "description": "Instructions with meaningful content (target: 2)",
     "scale": {"target": 2, "max": 5}},
    {"id": "sample_questions", "stage": "Developing", "type": "count", "points": 5,
     "description": "Example SQL questions (target: 5)",
     "scale": {"target": 5, "max": 10}},
    {"id": "joins_defined", "stage": "Developing", "type": "boolean", "points": 5,
     "description": "Join specifications configured"},
    {"id": "filter_snippets", "stage": "Developing", "type": "boolean", "points": 5,
     "description": "Filter snippets defined"},
    # Proficient
    {"id": "trusted_sql_queries", "stage": "Proficient", "type": "count", "points": 10,
     "description": "Example SQL queries (target: 10)",
     "scale": {"target": 10, "max": 20}},
    {"id": "expressions_defined", "stage": "Proficient", "type": "count", "points": 5,
     "description": "SQL expressions and measures (target: 3)",
     "scale": {"target": 3, "max": 10}},
    {"id": "unity_catalog", "stage": "Proficient", "type": "boolean", "points": 7,
     "description": "All tables use 3-part UC names"},
    # Optimized
    {"id": "benchmark_questions", "stage": "Optimized", "type": "count", "points": 8,
     "description": "Benchmark questions (target: 10)",
     "scale": {"target": 10, "max": 20}},
    {"id": "sql_coverage", "stage": "Optimized", "type": "count", "points": 5,
     "description": "SQL example breadth (target: 15)",
     "scale": {"target": 15, "max": 30}},
    {"id": "sql_functions", "stage": "Optimized", "type": "boolean", "points": 5,
     "description": "Custom SQL functions defined"},
]

STAGES = [
    {"name": "Nascent", "range": [0, 29]},
    {"name": "Basic", "range": [30, 49]},
    {"name": "Developing", "range": [50, 69]},
    {"name": "Proficient", "range": [70, 84]},
    {"name": "Optimized", "range": [85, 100]},
]


# --- Check functions ---

def check_tables_attached(space: dict) -> bool:
    return len(space.get("data_sources", {}).get("tables", [])) > 0


def check_table_count(space: dict) -> float:
    return float(len(space.get("data_sources", {}).get("tables", [])))


def check_columns_exist(space: dict) -> bool:
    tables = space.get("data_sources", {}).get("tables", [])
    return any(len(t.get("columns", [])) > 0 for t in tables)


def check_instructions_defined(space: dict) -> bool:
    return len(space.get("instructions", {}).get("text_instructions", [])) > 0


def check_table_descriptions(space: dict) -> bool:
    tables = space.get("data_sources", {}).get("tables", [])
    return any(t.get("description") or t.get("comment") for t in tables)


def check_column_descriptions(space: dict) -> float:
    tables = space.get("data_sources", {}).get("tables", [])
    total, described = 0, 0
    for t in tables:
        for col in t.get("columns", []):
            total += 1
            if col.get("description") or col.get("comment"):
                described += 1
    return described / total if total > 0 else 0.0


def check_instruction_quality(space: dict) -> float:
    texts = space.get("instructions", {}).get("text_instructions", [])
    count = 0
    for t in texts:
        content = t.get("content", "")
        if isinstance(content, list):
            content = "".join(content)
        if len(content) > 50:
            count += 1
    return float(count)


def check_sample_questions(space: dict) -> float:
    return float(len(space.get("instructions", {}).get("example_question_sqls", [])))


def check_joins_defined(space: dict) -> bool:
    return len(space.get("instructions", {}).get("join_specs", [])) > 0


def check_filter_snippets(space: dict) -> bool:
    return len(space.get("instructions", {}).get("sql_snippets", {}).get("filters", [])) > 0


def check_trusted_sql_queries(space: dict) -> float:
    return float(len(space.get("instructions", {}).get("example_question_sqls", [])))


def check_expressions_defined(space: dict) -> float:
    snippets = space.get("instructions", {}).get("sql_snippets", {})
    return float(len(snippets.get("expressions", [])) + len(snippets.get("measures", [])))


def check_unity_catalog(space: dict) -> bool:
    tables = space.get("data_sources", {}).get("tables", [])
    if not tables:
        return False
    return all(len(t.get("identifier", "").split(".")) == 3 for t in tables)


def check_benchmark_questions(space: dict) -> float:
    return float(len(space.get("benchmarks", {}).get("questions", [])))


def check_sql_coverage(space: dict) -> float:
    return float(len(space.get("instructions", {}).get("example_question_sqls", [])))


def check_sql_functions(space: dict) -> bool:
    return len(space.get("instructions", {}).get("sql_functions", [])) > 0


CHECKS = {
    "tables_attached": check_tables_attached,
    "table_count": check_table_count,
    "columns_exist": check_columns_exist,
    "instructions_defined": check_instructions_defined,
    "table_descriptions": check_table_descriptions,
    "column_descriptions": check_column_descriptions,
    "instruction_quality": check_instruction_quality,
    "sample_questions": check_sample_questions,
    "joins_defined": check_joins_defined,
    "filter_snippets": check_filter_snippets,
    "trusted_sql_queries": check_trusted_sql_queries,
    "expressions_defined": check_expressions_defined,
    "unity_catalog": check_unity_catalog,
    "benchmark_questions": check_benchmark_questions,
    "sql_coverage": check_sql_coverage,
    "sql_functions": check_sql_functions,
}


# --- Scoring engine ---

def get_maturity_stage(score: int) -> str:
    for stage in reversed(STAGES):
        if score >= stage["range"][0]:
            return stage["name"]
    return "Nascent"


def score_space(space_data: dict) -> dict:
    """Score a space against the maturity model. Returns score report."""
    stage_points = {s["name"]: 0 for s in STAGES}
    findings = []
    results = []

    for criterion in CRITERIA:
        cid = criterion["id"]
        check_fn = CHECKS.get(cid)
        if not check_fn:
            continue

        result = check_fn(space_data)

        if criterion["type"] == "boolean":
            passed = bool(result)
            points = criterion["points"] if passed else 0
            if not passed:
                findings.append(f"Missing: {criterion['description']}")
        else:
            raw = float(result)
            target = criterion.get("scale", {}).get("target", 1)
            ratio = min(raw / target, 1.0) if target > 0 else (1.0 if raw > 0 else 0.0)
            points = round(criterion["points"] * ratio)
            passed = points > 0
            if ratio < 1.0:
                findings.append(f"Below target: {criterion['description']} ({raw:.0f}/{target})")

        stage_points[criterion["stage"]] += points
        results.append({
            "id": cid, "stage": criterion["stage"],
            "points": points, "max": criterion["points"],
            "passed": passed, "description": criterion["description"],
        })

    total = min(sum(stage_points.values()), 100)
    maturity = get_maturity_stage(total)

    return {
        "score": total,
        "maturity": maturity,
        "breakdown": stage_points,
        "criteria": results,
        "findings": findings,
    }


# --- API helpers ---

def get_serialized_space(host: str, token: str, space_id: str) -> dict:
    """Retrieve full space config via empty PATCH (GET doesn't return serialized_space)."""
    resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{space_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={},
    )
    resp.raise_for_status()
    data = resp.json()
    return json.loads(data["serialized_space"])


def print_report(report: dict) -> None:
    """Print a formatted score report."""
    print(f"\n{'=' * 50}")
    print(f"  Score: {report['score']}/100  |  Stage: {report['maturity']}")
    print(f"{'=' * 50}\n")

    print("Breakdown by stage:")
    for stage_name, points in report["breakdown"].items():
        max_pts = sum(c["points"] for c in CRITERIA if c["stage"] == stage_name)
        bar = "█" * (points * 20 // max(max_pts, 1)) + "░" * (20 - points * 20 // max(max_pts, 1))
        print(f"  {stage_name:<12} {bar} {points}/{max_pts}")

    print(f"\nCriteria ({sum(1 for c in report['criteria'] if c['passed'])}/{len(report['criteria'])} passing):")
    for c in report["criteria"]:
        icon = "✓" if c["passed"] else "✗"
        print(f"  {icon} {c['description']:<50} {c['points']}/{c['max']}")

    if report["findings"]:
        print(f"\nTop findings:")
        for i, f in enumerate(report["findings"][:5], 1):
            print(f"  {i}. {f}")

    print()


# --- Main ---

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python score-space.py <space_id>")
        print("\nSet DATABRICKS_HOST and DATABRICKS_TOKEN environment variables,")
        print("or run inside a Databricks notebook (uses dbutils automatically).")
        sys.exit(1)

    space_id = sys.argv[1]

    # Try dbutils first (Databricks notebook), then env vars
    try:
        import os
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if not host or not token:
            raise ValueError("Set DATABRICKS_HOST and DATABRICKS_TOKEN")
    except ValueError:
        print("Error: Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
        sys.exit(1)

    print(f"Scoring space: {space_id}")
    space_data = get_serialized_space(host, token, space_id)
    report = score_space(space_data)
    print_report(report)
