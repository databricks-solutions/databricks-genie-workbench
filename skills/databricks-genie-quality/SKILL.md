---
name: databricks-genie-quality
description: "Assess and optimize Databricks Genie Space quality using a 5-stage maturity model. Use when scoring a Genie Space, identifying missing configuration, adding instructions/joins/descriptions, or improving Genie answer accuracy."
---

# Genie Space Quality Assessment

Score, analyze, and optimize Genie Spaces using a structured maturity model. Complements the `databricks-genie` skill (which covers creation and querying) by adding quality assessment and optimization.

## When to Use This Skill

Use this skill when:
- Scoring a Genie Space to understand its quality level
- Identifying what's missing or misconfigured in a space
- Adding instructions, join specs, column descriptions, or sample questions
- Improving Genie answer accuracy for an existing space
- Auditing multiple spaces across a workspace

**Prerequisites:** The `databricks-genie` skill and its MCP tools (`get_genie`, `create_or_update_genie`, `ask_genie`, etc.) should be available for executing changes.

## The Maturity Model

Every Genie Space progresses through 5 stages. The score shows where a space is on the journey — not how well someone did.

| Stage | Score | Key Question | What It Means |
|-------|-------|-------------|---------------|
| **Nascent** | 0–29 | Can Genie see my data? | Tables attached but minimal config. Answers are unpredictable. |
| **Basic** | 30–49 | Does Genie understand my domain? | Some instructions and descriptions. Starting to understand context. |
| **Developing** | 50–69 | Does Genie speak my language? | Instructions, sample questions, joins. Understands the domain. |
| **Proficient** | 70–84 | Are answers consistent? | Trusted SQL, expressions. Reliable, metrics-accurate answers. |
| **Optimized** | 85–100 | Is it ready for everyone? | Full SQL coverage, benchmarks, feedback loops. Production-grade. |

## Scoring Criteria

### Nascent (25 points possible)

| Criterion | Type | Points | What to Check |
|-----------|------|--------|---------------|
| `tables_attached` | boolean | 10 | `data_sources.tables` has at least one entry |
| `table_count` | scale | 0–10 | Number of tables (target: 5, max credit at 10) |
| `columns_exist` | boolean | 5 | At least one table has columns defined |

### Basic (15 points possible)

| Criterion | Type | Points | What to Check |
|-----------|------|--------|---------------|
| `instructions_defined` | boolean | 5 | `instructions.text_instructions` is non-empty |
| `table_descriptions` | boolean | 5 | At least one table has a description/comment |
| `column_descriptions` | scale | 0–5 | Proportion of columns with descriptions (target: 80%) |

### Developing (20 points possible)

| Criterion | Type | Points | What to Check |
|-----------|------|--------|---------------|
| `instruction_quality` | scale | 0–5 | Instructions with meaningful content (>50 chars, target: 2) |
| `sample_questions` | scale | 0–5 | Example SQL questions (target: 5) |
| `joins_defined` | boolean | 5 | `instructions.join_specs` is non-empty |
| `filter_snippets` | boolean | 5 | `instructions.sql_snippets.filters` is non-empty |

### Proficient (22 points possible)

| Criterion | Type | Points | What to Check |
|-----------|------|--------|---------------|
| `trusted_sql_queries` | scale | 0–10 | Example SQL queries (target: 10) |
| `expressions_defined` | scale | 0–5 | SQL expressions + measures (target: 3) |
| `unity_catalog` | boolean | 7 | All tables use 3-part names (`catalog.schema.table`) |

### Optimized (18 points possible)

| Criterion | Type | Points | What to Check |
|-----------|------|--------|---------------|
| `benchmark_questions` | scale | 0–8 | Benchmark questions for accuracy tracking (target: 10) |
| `sql_coverage` | scale | 0–5 | Breadth of SQL examples (target: 15) |
| `sql_functions` | boolean | 5 | Custom SQL functions defined |

**Scale scoring:** `points = max_points × min(value / target, 1.0)`

## Assessment Workflow

### Step 1: Retrieve the Serialized Space

The full space configuration lives in `serialized_space`, which is **not returned by GET**. Use an empty PATCH to retrieve it:

```python
import json, requests

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Empty PATCH returns the full serialized_space
resp = requests.patch(
    f"{host}/api/2.0/genie/spaces/{space_id}",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={}
)
space = resp.json()
space_data = json.loads(space["serialized_space"])
```

Or with MCP tools: `get_genie(space_id=space_id)` — though this may not include `serialized_space` depending on the tool version.

### Step 2: Evaluate Against Criteria

Walk through each criterion and tally points. See [examples/score-space.py](examples/score-space.py) for a complete implementation.

Key paths in `space_data`:
- **Tables:** `data_sources.tables[]` — each has `identifier`, `columns[]`, `description`
- **Instructions:** `instructions.text_instructions[]` — each has `content` (string or list)
- **Sample questions:** `instructions.example_question_sqls[]`
- **Joins:** `instructions.join_specs[]`
- **Filters:** `instructions.sql_snippets.filters[]`
- **Expressions:** `instructions.sql_snippets.expressions[]`
- **Measures:** `instructions.sql_snippets.measures[]`
- **SQL functions:** `instructions.sql_functions[]`
- **Benchmarks:** `benchmarks.questions[]`

### Step 3: Identify Gaps and Optimize

Based on the score, prioritize fixes by stage (fix Nascent/Basic gaps before Developing/Proficient). See [optimization.md](optimization.md) for detailed patterns.

## Quick Start: Score and Fix

```
User: "Score my Genie Space sales_analytics and tell me what to improve"

Agent workflow:
1. List spaces → find space_id for "sales_analytics"
2. Empty PATCH → get serialized_space
3. Evaluate 16 criteria → calculate score
4. Report: "Score: 42/100 (Basic). Missing: join specs, sample questions,
   column descriptions. Next: add join specs for your 3 tables."
5. Offer to fix: "Want me to add join specs based on your table schemas?"
```

## Genie API Gotchas

| Gotcha | Details |
|--------|---------|
| **`serialized_space` not in GET** | Must use an empty PATCH (`json={}`) to retrieve it |
| **`table_identifiers` silently ignored** | PATCH body `table_identifiers` field is a no-op — modify tables via `serialized_space` |
| **Tables must be sorted** | `data_sources.tables` must be sorted alphabetically by `identifier` or the API returns `INVALID_PARAMETER_VALUE` |
| **Nested JSON escaping** | `serialized_space` is a JSON string inside JSON — use `curl` for PATCH updates (Databricks CLI has escaping issues with `--json`) |
| **Instructions content format** | `text_instructions[].content` can be a string or list of strings — handle both |

## Reference Files

- [optimization.md](optimization.md) — Common findings and programmatic fixes
- [examples/score-space.py](examples/score-space.py) — Score a space against the maturity model
- [examples/add-instructions.py](examples/add-instructions.py) — Programmatically add instructions
- [examples/add-joins.py](examples/add-joins.py) — Add join specifications from table schemas

## Relationship to Other Skills

| Skill | Role |
|-------|------|
| `databricks-genie` | Create spaces, ask questions (CRUD + query) |
| **`databricks-genie-quality`** | Score spaces, identify gaps, optimize configuration |
| `databricks-unity-catalog` | Browse catalogs/schemas/tables for metadata enrichment |
