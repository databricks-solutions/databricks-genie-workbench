# Genie Space Optimization Patterns

Common findings from scoring, organized by maturity stage, with programmatic fixes.

## Priority Order

Fix gaps **bottom-up by stage**. A space stuck at Nascent won't benefit from Optimized-stage work.

```
Nascent → Basic → Developing → Proficient → Optimized
 (fix)    (fix)    (fix)         (fix)        (polish)
```

---

## Nascent Stage Fixes

### Finding: No tables attached

**Cause:** Space was created without specifying tables, or tables were removed.

**Fix:** Add tables via `serialized_space`. Tables must be sorted alphabetically by `identifier`.

```python
space_data["data_sources"]["tables"] = sorted(
    [
        {"identifier": "catalog.schema.customers"},
        {"identifier": "catalog.schema.orders"},
        {"identifier": "catalog.schema.products"},
    ],
    key=lambda t: t["identifier"]
)
```

### Finding: Tables have no columns

**Cause:** Columns aren't populated automatically — they come from UC metadata or manual definition.

**Fix:** Usually resolves itself after the first query. If columns are still missing, verify the tables exist in Unity Catalog and the warehouse can access them.

---

## Basic Stage Fixes

### Finding: No text instructions

**Cause:** Space was created without guidance for Genie.

**Fix:** Generate instructions from table schemas. Good instructions describe:
- What the data represents (domain context)
- Key terminology and abbreviations
- Date ranges and data freshness
- Common business rules

```python
import uuid

instruction = {
    "id": str(uuid.uuid4()),
    "content": [
        "This space contains retail sales data.\n",
        "Key tables:\n",
        "- customers: Customer demographics with region and segment\n",
        "- orders: Transaction history with amounts and dates\n",
        "- products: Product catalog with categories and pricing\n",
        "\n",
        "Business rules:\n",
        "- 'Active' customers have placed an order in the last 90 days\n",
        "- Revenue = order total_amount (not including returns)\n",
        "- Fiscal year starts April 1\n",
    ]
}

if "text_instructions" not in space_data.get("instructions", {}):
    space_data.setdefault("instructions", {})["text_instructions"] = []
space_data["instructions"]["text_instructions"].append(instruction)
```

### Finding: Missing table descriptions

**Cause:** Tables lack descriptions in the Genie Space config.

**Fix:** Pull descriptions from Unity Catalog comments and apply to the space config:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for table in space_data["data_sources"]["tables"]:
    parts = table["identifier"].split(".")
    if len(parts) == 3:
        catalog, schema, table_name = parts
        try:
            uc_table = w.tables.get(f"{catalog}.{schema}.{table_name}")
            if uc_table.comment:
                table["description"] = uc_table.comment
        except Exception:
            pass  # Table may not be accessible
```

### Finding: Missing column descriptions

**Cause:** Columns exist but lack descriptions, reducing Genie's ability to understand the data.

**Fix:** Pull column comments from Unity Catalog:

```python
for table in space_data["data_sources"]["tables"]:
    parts = table["identifier"].split(".")
    if len(parts) != 3:
        continue
    catalog, schema, table_name = parts
    try:
        uc_table = w.tables.get(f"{catalog}.{schema}.{table_name}")
        uc_col_map = {c.name: c.comment for c in (uc_table.columns or []) if c.comment}
        for col in table.get("columns", []):
            if not col.get("description") and col.get("name") in uc_col_map:
                col["description"] = uc_col_map[col["name"]]
    except Exception:
        pass
```

---

## Developing Stage Fixes

### Finding: No sample questions

**Cause:** Space has no example SQL questions to guide users and train Genie.

**Fix:** Generate questions from the table schemas. Good sample questions:
- Cover common use cases (aggregations, filters, joins)
- Reference actual column and table names
- Use natural language (not SQL syntax)
- Include time-based queries if date columns exist

```python
# Generate from schema inspection
questions = [
    {
        "question": "What were total sales last month?",
        "sql": "SELECT SUM(total_amount) FROM catalog.schema.orders WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH)"
    },
    {
        "question": "Who are our top 10 customers by revenue?",
        "sql": "SELECT c.name, SUM(o.total_amount) as revenue FROM catalog.schema.customers c JOIN catalog.schema.orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY revenue DESC LIMIT 10"
    },
]

space_data.setdefault("instructions", {})["example_question_sqls"] = questions
```

### Finding: No join specifications

**Cause:** Multi-table spaces don't tell Genie how tables relate.

**Fix:** Infer joins from foreign key relationships or column naming conventions. See [examples/add-joins.py](examples/add-joins.py) for a complete implementation.

```python
join_spec = {
    "left_table": "catalog.schema.orders",
    "right_table": "catalog.schema.customers",
    "join_type": "INNER",
    "conditions": [
        {
            "left_column": "customer_id",
            "right_column": "id"
        }
    ]
}

space_data.setdefault("instructions", {})["join_specs"] = [join_spec]
```

### Finding: No filter snippets

**Cause:** No predefined filters for common business segments.

**Fix:** Add filters for frequently-used dimensions:

```python
filters = [
    {
        "name": "active_customers",
        "description": "Customers who ordered in the last 90 days",
        "sql": "order_date >= CURRENT_DATE - INTERVAL 90 DAY"
    },
    {
        "name": "current_quarter",
        "description": "Current fiscal quarter",
        "sql": "order_date >= DATE_TRUNC('quarter', CURRENT_DATE)"
    }
]

space_data.setdefault("instructions", {}).setdefault("sql_snippets", {})["filters"] = filters
```

---

## Proficient Stage Fixes

### Finding: Few trusted SQL queries

**Cause:** Not enough example SQL to guide Genie's query generation.

**Fix:** Add more `example_question_sqls` covering diverse patterns:
- Simple aggregations (COUNT, SUM, AVG)
- Group-by with filters
- Multi-table joins
- Date ranges and window functions
- CASE expressions

Target: 10+ example queries covering the most common business questions.

### Finding: No SQL expressions or measures

**Cause:** No reusable business metrics defined.

**Fix:** Define expressions for key business metrics:

```python
expressions = [
    {
        "name": "customer_lifetime_value",
        "description": "Total revenue from a customer across all orders",
        "sql": "SUM(total_amount)"
    },
    {
        "name": "avg_order_value",
        "description": "Average order amount",
        "sql": "AVG(total_amount)"
    }
]

space_data.setdefault("instructions", {}).setdefault("sql_snippets", {})["expressions"] = expressions
```

### Finding: Tables not using Unity Catalog 3-part names

**Cause:** Tables referenced with 2-part names or aliases instead of `catalog.schema.table`.

**Fix:** Update table identifiers to fully-qualified 3-part names:

```python
for table in space_data["data_sources"]["tables"]:
    parts = table["identifier"].split(".")
    if len(parts) == 2:
        # Assume default catalog
        table["identifier"] = f"main.{table['identifier']}"
```

---

## Optimized Stage Fixes

### Finding: No benchmark questions

**Cause:** No questions defined for tracking answer accuracy over time.

**Fix:** Create benchmark questions with known-correct SQL. These serve as regression tests when the space configuration changes.

### Finding: Low SQL coverage

**Cause:** Example SQL queries don't cover enough query patterns.

**Fix:** Audit existing examples and add queries that cover:
- Different aggregation types
- Various join patterns
- Subqueries and CTEs
- Window functions
- String and date manipulation

### Finding: No SQL functions

**Cause:** No custom SQL functions for complex business logic.

**Fix:** Define functions for calculations that Genie shouldn't invent:

```python
sql_functions = [
    {
        "name": "fiscal_quarter",
        "description": "Convert a date to fiscal quarter (FY starts April 1)",
        "sql": "CASE WHEN MONTH(date_col) >= 4 THEN CONCAT('Q', CEILING((MONTH(date_col) - 3) / 3.0)) ELSE CONCAT('Q', CEILING((MONTH(date_col) + 9) / 3.0)) END"
    }
]

space_data.setdefault("instructions", {})["sql_functions"] = sql_functions
```

---

## Applying Changes

After modifying `space_data`, write it back via PATCH:

```python
import json, requests

# CRITICAL: Sort tables alphabetically before writing
space_data["data_sources"]["tables"] = sorted(
    space_data["data_sources"]["tables"],
    key=lambda t: t["identifier"]
)

serialized = json.dumps(space_data)

resp = requests.patch(
    f"{host}/api/2.0/genie/spaces/{space_id}",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={"serialized_space": serialized}
)

if resp.status_code != 200:
    print(f"Error: {resp.status_code} - {resp.text}")
else:
    print("Space updated successfully")
```

**Remember:** Use `curl` or `requests` for the PATCH — the Databricks CLI has issues with nested JSON escaping.
