# serialized_space JSON Schema (version 2)

Reference for `generate_config` and `update_config` tools. The tools handle all mechanical formatting
(IDs, sorting, SQL splitting). This documents the **output** structure they produce.

## Complete Example

```json
{
  "version": 2,
  "config": {
    "sample_questions": [
      {
        "id": "a1b2c3d4e5f60000000000000000000a",
        "question": ["What were total sales last month?"]
      }
    ]
  },
  "data_sources": {
    "tables": [
      {
        "identifier": "catalog.schema.orders",
        "description": ["Daily sales transactions with line-item details"],
        "column_configs": [
          {
            "column_name": "region",
            "description": ["Sales region code: AMER, EMEA, APJ, LATAM"],
            "synonyms": ["area", "territory", "sales region"],
            "exclude": false,
            "enable_entity_matching": true,
            "enable_format_assistance": true,
            "get_example_values": true,
            "build_value_dictionary": true
          },
          {
            "column_name": "etl_timestamp",
            "exclude": true,
            "enable_entity_matching": false,
            "enable_format_assistance": false
          }
        ]
      }
    ],
    "metric_views": [
      {
        "identifier": "catalog.schema.revenue_metrics",
        "description": ["Pre-aggregated revenue metrics by region and time period"],
        "column_configs": [
          {
            "column_name": "period",
            "description": ["Time period for the metric (monthly, quarterly, yearly)"],
            "enable_format_assistance": true
          }
        ]
      }
    ]
  },
  "instructions": {
    "text_instructions": [
      {
        "id": "b2c3d4e5f6a70000000000000000000b",
        "content": [
          "## PURPOSE\n",
          "- Answer questions about order revenue for FY2024 US retail orders.\n",
          "\n",
          "## DISAMBIGUATION\n",
          "- When asked about 'last month', use the previous calendar month.\n",
          "\n",
          "## Instructions you must follow when providing summaries\n",
          "- Round all monetary values to 2 decimal places.\n"
        ]
      }
    ],
    "example_question_sqls": [
      {
        "id": "c3d4e5f6a7b80000000000000000000c",
        "question": ["Show top 10 customers by revenue"],
        "sql": [
          "SELECT customer_name, SUM(order_amount) as total_revenue\n",
          "FROM catalog.schema.orders o\n",
          "JOIN catalog.schema.customers c ON o.customer_id = c.customer_id\n",
          "GROUP BY customer_name\n",
          "ORDER BY total_revenue DESC\n",
          "LIMIT 10"
        ],
        "usage_guidance": ["Use this pattern for any top-N ranking question by a numeric metric"]
      },
      {
        "id": "d4e5f6a7b8c90000000000000000000d",
        "question": ["Show sales for a specific region"],
        "sql": [
          "SELECT SUM(order_amount) as total_sales\n",
          "FROM catalog.schema.orders\n",
          "WHERE region = :region_name"
        ],
        "parameters": [
          {
            "name": "region_name",
            "type_hint": "STRING",
            "description": ["The region to filter by (e.g., 'North America', 'Europe')"],
            "default_value": {"values": ["North America"]}
          }
        ],
        "usage_guidance": ["Use when user asks about sales filtered by a specific geographic region"]
      }
    ],
    "sql_functions": [
      {
        "id": "e5f6a7b8c9d00000000000000000000e",
        "identifier": "catalog.schema.fiscal_quarter",
        "description": "Calculates the fiscal quarter from a date (fiscal year starts April 1)"
      }
    ],
    "join_specs": [
      {
        "id": "f6a7b8c9d0e10000000000000000000f",
        "left": {"identifier": "catalog.schema.orders", "alias": "orders"},
        "right": {"identifier": "catalog.schema.customers", "alias": "customers"},
        "sql": [
          "`orders`.`customer_id` = `customers`.`customer_id`",
          "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"
        ],
        "comment": ["Join orders to customers on customer_id"],
        "instruction": ["Use this join when relating orders to customer demographics"]
      }
    ],
    "sql_snippets": {
      "filters": [
        {
          "id": "a7b8c9d0e1f20000000000000000000a",
          "display_name": "high value orders",
          "sql": ["orders.order_amount > 1000"],
          "synonyms": ["large orders", "big purchases"],
          "instruction": ["Apply when users ask about high-value or large orders"],
          "comment": ["Threshold aligned with finance team's definition"]
        }
      ],
      "expressions": [
        {
          "id": "b8c9d0e1f2a30000000000000000000b",
          "alias": "order_year",
          "display_name": "Order Year",
          "sql": ["YEAR(orders.order_date)"],
          "synonyms": ["year"],
          "instruction": ["Use for any year-based grouping of orders"],
          "comment": ["Standard date dimension for annual reporting"]
        }
      ],
      "measures": [
        {
          "id": "c9d0e1f2a3b40000000000000000000c",
          "alias": "total_revenue",
          "display_name": "Total Revenue",
          "sql": ["SUM(orders.quantity * orders.unit_price)"],
          "synonyms": ["revenue", "sales", "total sales"],
          "instruction": ["Use for any revenue aggregation"],
          "comment": ["Revenue includes all non-cancelled order line items"]
        }
      ]
    }
  },
  "benchmarks": {
    "questions": [
      {
        "id": "d0e1f2a3b4c50000000000000000000d",
        "question": ["What is average order value?"],
        "answer": [{"format": "SQL", "content": ["SELECT AVG(order_amount) as avg_order_value\n", "FROM catalog.schema.orders"]}]
      }
    ]
  }
}
```

## Validation Rules

### IDs
- All IDs: exactly **32 lowercase hex characters** (UUID without hyphens)
- Generate with `secrets.token_hex(16)`
- **Question IDs**: All IDs in `config.sample_questions` and `benchmarks.questions` must be unique across both collections
- **Instruction IDs**: All IDs across `text_instructions`, `example_question_sqls`, `sql_functions`, `join_specs`, and all `sql_snippets` types must be unique
- **Column configs**: The combination of `(table_identifier, column_name)` must be unique within the space

### Sorting (API rejects unsorted arrays)
| Collection | Sort key |
|---|---|
| `data_sources.tables` | `identifier` alphabetically |
| `data_sources.tables[].column_configs` | `column_name` alphabetically |
| `data_sources.metric_views` | `identifier` alphabetically |
| `data_sources.metric_views[].column_configs` | `column_name` alphabetically |
| `config.sample_questions` | `id` alphabetically |
| `instructions.text_instructions` | `id` alphabetically |
| `instructions.example_question_sqls` | `id` alphabetically |
| `instructions.example_question_sqls[].parameters` | `name` alphabetically |
| `instructions.sql_functions` | `(id, identifier)` tuple |
| `instructions.join_specs` | `id` alphabetically |
| `instructions.sql_snippets.filters` | `id` alphabetically |
| `instructions.sql_snippets.expressions` | `id` alphabetically |
| `instructions.sql_snippets.measures` | `id` alphabetically |
| `benchmarks.questions` | `id` alphabetically |

### SQL formatting
- `sql` fields: string arrays, each clause on a separate element with `\n` suffix
- `sql_snippets` require table-qualified column references: `table_alias.column_name`
- Filters must NOT include `WHERE` keyword — only the boolean condition
- `join_specs.sql`: exactly **TWO elements** — (1) backtick-quoted condition `` `alias`.`col` = `alias`.`col` `` (2) `--rt=FROM_RELATIONSHIP_TYPE_...--` relationship annotation. **Without the `--rt=` annotation the API rejects the request** with a protobuf parsing error.
- `join_specs` required fields: `id`, `left` (object: `identifier` + `alias`), `right` (object: `identifier` + `alias`), `sql` (2 elements). Optional: `comment`, `instruction`. **Omitting `left` or `right` causes a protobuf parsing error.**

### Size limits
- `version`: Required. Must be `2`.
- `text_instructions`: Max **1** entry per space. Each content element **must end with `\n`** (the API concatenates without separators — omitting `\n` jams text together). For section vocabulary and format rules (PURPOSE / DISAMBIGUATION / DATA QUALITY NOTES / CONSTRAINTS / summary-behavior), see `docs/gsl-instruction-schema.md`.
- Max **100** total instructions (each example SQL + each function + 1 for text block).
- Table identifiers: three-level namespace `catalog.schema.table`.
- Individual strings: max 25,000 characters.
- Array items: max 10,000 per array.
- Benchmark answers: exactly 1 answer per question, format must be `"SQL"`.

### Prompt matching (column_configs)
- `enable_format_assistance`: Shows representative values to help Genie understand data formats
- `enable_entity_matching`: Matches user terms to actual column values (e.g., "NY" → "New York")
- `get_example_values`: Fetches sample values from the column for context
- `build_value_dictionary`: Builds a dictionary of distinct values for precise matching
- `enable_format_assistance` and `enable_entity_matching` are **OFF by default** in the API, but `generate_config` turns them **ON by default** for non-excluded columns
- `enable_entity_matching` requires `enable_format_assistance` to also be true
- Set all to `false` when `exclude: true`

## Tool Usage Guide

### Initial creation: `generate_config`
The LLM provides content; the tool handles formatting (IDs, sorting, SQL splitting).
```
generate_config(
  tables=[{"identifier": "cat.sch.tbl", "description": "...", "column_configs": [...]}],
  sample_questions=["What is total revenue?", "Show top customers"],
  text_instructions=["Revenue = SUM(amount).", "Fiscal year starts April 1."],
  example_sqls=[{"question": "Top 10 customers", "sql": "SELECT ..."}],
  measures=[{"alias": "total_rev", "sql": "SUM(amount)", "display_name": "Total Revenue"}],
)
```

### Post-creation changes: `update_config`
Patches the existing config in-place — no rebuild, no new IDs, instant.
```
update_config(actions=[
  {"action": "enable_prompt_matching"},
  {"action": "update_instructions", "instructions": ["New instruction line 1"]},
  {"action": "add_example_sql", "question": "...", "sql": "SELECT ..."},
])
```
Then call `update_space(space_id=...)` to push the changes.
