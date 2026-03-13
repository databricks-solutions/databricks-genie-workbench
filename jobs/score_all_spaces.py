# Databricks notebook source
"""Scheduled job: score all Genie Spaces and persist results to Lakebase.

Self-contained — uses Databricks SDK and psycopg2 directly so it can run
as a notebook task without depending on the app's source tree.

Scoring uses the 5-level Maturity Curve:
  Nascent (0-29) → Basic (30-49) → Developing (50-69) → Proficient (70-84) → Optimized (85-100)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import logging
from datetime import datetime

from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("score_all_spaces")

LAKEBASE_INSTANCE_NAME = "genie-workbench-sz"
LAKEBASE_HOST = "instance-e5f92571-9961-4b1a-a11d-2a6eda65244d.database.cloud.databricks.com"
LAKEBASE_PORT = 5432
LAKEBASE_DATABASE = "databricks_postgres"
ALERT_THRESHOLD = 40

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maturity Curve Stages

# COMMAND ----------

MATURITY_LEVELS = [
    (85, "Optimized"),
    (70, "Proficient"),
    (50, "Developing"),
    (30, "Basic"),
    (0, "Nascent"),
]


def get_maturity_label(score: int) -> str:
    for threshold, label in MATURITY_LEVELS:
        if score >= threshold:
            return label
    return "Nascent"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Logic
# MAGIC
# MAGIC Mirrors `backend/services/scanner.py` — criteria checks by stage.

# COMMAND ----------


def calculate_score(space_data: dict) -> dict:
    """Calculate maturity score for a Genie Space configuration."""
    breakdown = {"nascent": 0, "basic": 0, "developing": 0, "proficient": 0, "optimized": 0}
    findings = []
    next_steps = []

    tables = space_data.get("data_sources", {}).get("tables", [])
    instructions = space_data.get("instructions", {})
    snippets = instructions.get("sql_snippets", {})

    # --- Nascent (25 pts max) ---
    if tables:
        breakdown["nascent"] += 10  # tables_attached
    else:
        findings.append("No tables configured")
        next_steps.append("Add at least one table to your Genie Space")

    # table_count (10 pts, scaled to target=5)
    table_count = len(tables)
    breakdown["nascent"] += min(round(10 * min(table_count / 5, 1.0)), 10)

    # columns_exist (5 pts)
    if tables and any(len(t.get("columns", [])) > 0 for t in tables):
        breakdown["nascent"] += 5
    elif tables:
        findings.append("Tables have no columns defined")
        next_steps.append("Ensure tables have columns defined")

    # --- Basic (15 pts max) ---
    text_instructions = instructions.get("text_instructions", [])
    if text_instructions:
        breakdown["basic"] += 5  # instructions_defined
    else:
        findings.append("No text instructions configured")
        next_steps.append("Add text instructions to explain business context")

    # table_descriptions (5 pts)
    if tables and any(t.get("description") or t.get("comment") for t in tables):
        breakdown["basic"] += 5
    elif tables:
        findings.append("Tables have no descriptions")
        next_steps.append("Add descriptions to tables to help Genie understand context")

    # column_descriptions (5 pts, scaled to target=0.8)
    total_cols = sum(len(t.get("columns", [])) for t in tables)
    desc_cols = sum(
        1 for t in tables for col in t.get("columns", [])
        if col.get("description") or col.get("comment")
    )
    col_ratio = desc_cols / total_cols if total_cols > 0 else 0.0
    breakdown["basic"] += min(round(5 * min(col_ratio / 0.8, 1.0)), 5)

    # --- Developing (20 pts max) ---
    example_sqls = instructions.get("example_question_sqls", [])

    # instruction_quality (5 pts, scaled to target=2)
    meaningful = sum(1 for t in text_instructions if len(t.get("content", "")) > 50)
    breakdown["developing"] += min(round(5 * min(meaningful / 2, 1.0)), 5)

    # sample_questions (5 pts, scaled to target=5)
    breakdown["developing"] += min(round(5 * min(len(example_sqls) / 5, 1.0)), 5)

    # joins_defined (5 pts)
    if instructions.get("join_specs"):
        breakdown["developing"] += 5
    elif table_count > 1:
        findings.append("No join specifications for multi-table space")
        next_steps.append("Add join specifications to help Genie join tables correctly")

    # filter_snippets (5 pts)
    if snippets.get("filters"):
        breakdown["developing"] += 5
    else:
        findings.append("No filter snippets defined")
        next_steps.append("Add filter snippets for common time ranges and business segments")

    # --- Proficient (22 pts max) ---
    # trusted_sql_queries (10 pts, scaled to target=10)
    breakdown["proficient"] += min(round(10 * min(len(example_sqls) / 10, 1.0)), 10)

    # expressions_defined (5 pts, scaled to target=3)
    expr_count = len(snippets.get("expressions", [])) + len(snippets.get("measures", []))
    breakdown["proficient"] += min(round(5 * min(expr_count / 3, 1.0)), 5)

    # unity_catalog (7 pts)
    if tables and all(
        len(t.get("table_name", "").split(".")) == 3 or t.get("catalog")
        for t in tables
    ):
        breakdown["proficient"] += 7
    elif tables:
        findings.append("Tables may not be using Unity Catalog")
        next_steps.append("Use fully-qualified Unity Catalog table names (catalog.schema.table)")

    # --- Optimized (18 pts max) ---
    # benchmark_questions (8 pts, scaled to target=10)
    benchmarks = space_data.get("benchmarks", {}).get("questions", [])
    breakdown["optimized"] += min(round(8 * min(len(benchmarks) / 10, 1.0)), 8)
    if not benchmarks:
        findings.append("No benchmark questions configured")
        next_steps.append("Add benchmark questions to measure Genie accuracy")

    # sql_coverage (5 pts, scaled to target=15)
    breakdown["optimized"] += min(round(5 * min(len(example_sqls) / 15, 1.0)), 5)

    # sql_functions (5 pts)
    if instructions.get("sql_functions"):
        breakdown["optimized"] += 5

    total_score = min(sum(breakdown.values()), 100)
    maturity = get_maturity_label(total_score)

    return {
        "score": total_score,
        "maturity": maturity,
        "breakdown": breakdown,
        "findings": findings[:5],
        "next_steps": next_steps[:5],
        "scanned_at": datetime.utcnow().isoformat(),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover and Score All Spaces

# COMMAND ----------

w = WorkspaceClient()

# List all Genie Spaces
logger.info("Listing Genie Spaces...")
spaces = []
page_token = None
while True:
    params = {"page_size": 100}
    if page_token:
        params["page_token"] = page_token
    resp = w.api_client.do(method="GET", path="/api/2.0/genie/spaces", query=params)
    spaces.extend(resp.get("spaces", []))
    page_token = resp.get("next_page_token")
    if not page_token:
        break

logger.info(f"Found {len(spaces)} Genie Space(s)")

# Score each space
results = []
failed = 0

for space in spaces:
    space_id = space.get("id", "")
    display_name = space.get("display_name", "Unknown")
    if not space_id:
        continue

    try:
        raw = w.api_client.do(
            method="GET",
            path=f"/api/2.0/genie/spaces/{space_id}",
            query={"include_serialized_space": "true"},
        )
        space_data = json.loads(raw.get("serialized_space", "{}"))
        result = calculate_score(space_data)
        result["space_id"] = space_id
        result["display_name"] = display_name
        results.append(result)
        logger.info(f"  {display_name}: {result['score']}/100 ({result['maturity']})")
    except Exception as e:
        logger.warning(f"  {display_name}: FAILED - {e}")
        failed += 1

logger.info(f"Scored {len(results)}, failed {failed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist to Lakebase

# COMMAND ----------

import psycopg2

# Generate Lakebase credential
cred_resp = w.api_client.do(
    method="POST",
    path="/api/2.0/database/credentials",
    body={"request_id": "score-job", "instance_names": [LAKEBASE_INSTANCE_NAME]},
)
lb_token = cred_resp["token"]
lb_user = w.current_user.me().user_name

conn = psycopg2.connect(
    host=LAKEBASE_HOST,
    port=LAKEBASE_PORT,
    dbname=LAKEBASE_DATABASE,
    user=lb_user,
    password=lb_token,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Ensure table exists
cur.execute("""
    CREATE TABLE IF NOT EXISTS scan_results (
        space_id TEXT NOT NULL,
        score INTEGER NOT NULL,
        maturity TEXT,
        breakdown JSONB,
        findings JSONB,
        next_steps JSONB,
        scanned_at TIMESTAMPTZ NOT NULL,
        UNIQUE (space_id, scanned_at)
    )
""")

# Insert results
for r in results:
    cur.execute("""
        INSERT INTO scan_results (space_id, score, maturity, breakdown, findings, next_steps, scanned_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (space_id, scanned_at) DO UPDATE SET
            score = EXCLUDED.score,
            maturity = EXCLUDED.maturity,
            breakdown = EXCLUDED.breakdown,
            findings = EXCLUDED.findings,
            next_steps = EXCLUDED.next_steps
    """, (
        r["space_id"],
        r["score"],
        r["maturity"],
        json.dumps(r["breakdown"]),
        json.dumps(r["findings"]),
        json.dumps(r["next_steps"]),
        datetime.fromisoformat(r["scanned_at"]),
    ))

cur.close()
conn.close()
logger.info(f"Persisted {len(results)} results to Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

alerts = [r for r in results if r["score"] < ALERT_THRESHOLD]

summary = {
    "timestamp": datetime.utcnow().isoformat(),
    "total_spaces": len(spaces),
    "scored": len(results),
    "failed": failed,
    "alerts": [
        {"space_id": a["space_id"], "display_name": a["display_name"],
         "score": a["score"], "maturity": a["maturity"],
         "top_finding": a["findings"][0] if a["findings"] else None}
        for a in alerts
    ],
}

if alerts:
    logger.warning(f"{len(alerts)} space(s) below threshold ({ALERT_THRESHOLD}):")
    for a in alerts:
        logger.warning(f"  {a['display_name']}: {a['score']}/100")

print(json.dumps(summary, indent=2))
