# Databricks Genie Space Configuration Schema

This document describes the serialized Genie Space JSON schema used by the Genie Conversation API. It is aligned with the [official Databricks doc](https://docs.databricks.com/aws/en/genie/conversation-api#create-or-select-a-genie-space). Last verified on 2026-02-18.

## Root Structure

| Field | Type | Required | Description |
| ------- | ------ | ---------- | ------------- |
| `version` | integer | Yes | Schema version. Use `2` for newly created spaces. |
| `config` | object | Yes | Configuration settings including `sample_questions`. |
| `data_sources` | object | Yes | Data source definitions (`tables`, optional `metric_views`). |
| `instructions` | object | Yes | Instructions for the Genie agent. |
| `benchmarks` | object | No | Benchmark questions used for quality evaluation. |

## Serialization Note

When calling the create/update APIs, this object is passed as an escaped JSON string in `serialized_space`.

## JSON Example

```json
{
  "version": 2,
  "config": {
    "sample_questions": [
      {
        "id": "a1b2c3d4e5f60000000000000000000a",
        "question": ["What were total sales last month?"]
      },
      {
        "id": "b2c3d4e5f6a70000000000000000000b",
        "question": ["Show top 10 customers by revenue"]
      }
    ]
  },
  "data_sources": {
    "tables": [
      {
        "identifier": "sales.analytics.customers",
        "description": ["Customer master data including contact information and account details"],
        "column_configs": [
          {
            "column_name": "customer_id",
            "description": ["Unique identifier for each customer"],
            "synonyms": ["cust_id", "account_id"]
          },
          {
            "column_name": "customer_name",
            "enable_entity_matching": true
          },
          {
            "column_name": "internal_notes",
            "exclude": true
          }
        ]
      },
      {
        "identifier": "sales.analytics.orders",
        "description": ["Transactional order data including order date, amount, and customer information"],
        "column_configs": [
          {
            "column_name": "order_date",
            "enable_format_assistance": true
          },
          {
            "column_name": "region",
            "enable_format_assistance": true,
            "enable_entity_matching": true
          },
          {
            "column_name": "status",
            "enable_format_assistance": true,
            "enable_entity_matching": true
          }
        ]
      },
      {
        "identifier": "sales.analytics.products"
      }
    ],
    "metric_views": [
      {
        "identifier": "sales.analytics.revenue_metrics",
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
        "id": "01f0b37c378e1c9100000000000000a1",
        "content": [
          "When calculating revenue, sum the order_amount column.",
          "When asked about last month, use the previous calendar month.",
          "Round all monetary values to 2 decimal places."
        ]
      }
    ],
    "example_question_sqls": [
      {
        "id": "01f0821116d912db00000000000000b1",
        "question": ["Show top 10 customers by revenue"],
        "sql": [
          "SELECT customer_name, SUM(order_amount) as total_revenue\n",
          "FROM sales.analytics.orders o\n",
          "JOIN sales.analytics.customers c ON o.customer_id = c.customer_id\n",
          "GROUP BY customer_name\n",
          "ORDER BY total_revenue DESC\n",
          "LIMIT 10"
        ]
      },
      {
        "id": "01f099751a3a1df300000000000000b2",
        "question": ["What were total sales last month"],
        "sql": [
          "SELECT SUM(order_amount) as total_sales\n",
          "FROM sales.analytics.orders\n",
          "WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH)\n",
          "AND order_date < DATE_TRUNC('month', CURRENT_DATE)"
        ]
      },
      {
        "id": "01f099751a3a1df300000000000000b3",
        "question": ["Show sales for a specific region"],
        "sql": [
          "SELECT SUM(order_amount) as total_sales\n",
          "FROM sales.analytics.orders\n",
          "WHERE region = :region_name"
        ],
        "parameters": [
          {
            "name": "region_name",
            "type_hint": "STRING",
            "description": ["The region to filter by (e.g., North America, Europe)"],
            "default_value": {
              "values": ["North America"]
            }
          }
        ],
        "usage_guidance": ["Use this example when the user asks about sales filtered by a specific geographic region."]
      }
    ],
    "sql_functions": [
      {
        "id": "01f0c0b4e815100000000000000000f1",
        "identifier": "sales.analytics.fiscal_quarter"
      }
    ],
    "join_specs": [
      {
        "id": "01f0c0b4e815100000000000000000c1",
        "left": {
          "identifier": "sales.analytics.orders",
          "alias": "orders"
        },
        "right": {
          "identifier": "sales.analytics.customers",
          "alias": "customers"
        },
        "sql": ["orders.customer_id = customers.customer_id"],
        "comment": ["Join orders to customers on customer_id."],
        "instruction": ["Use this join when customer attributes are required for order analysis."]
      }
    ],
    "sql_snippets": {
      "filters": [
        {
          "id": "01f09972e66d100000000000000000d1",
          "sql": ["orders.order_amount > 1000"],
          "display_name": "high value orders",
          "synonyms": ["large orders", "big purchases"],
          "comment": ["Filters to orders over 1000."],
          "instruction": ["Use when the user asks about high-value or large orders."]
        }
      ],
      "expressions": [
        {
          "id": "01f09974563a100000000000000000e1",
          "alias": "order_year",
          "sql": ["YEAR(orders.order_date)"],
          "display_name": "year",
          "synonyms": ["fiscal year", "calendar year"],
          "comment": ["Extracts year from order date."],
          "instruction": ["Use for year-over-year analysis."]
        }
      ],
      "measures": [
        {
          "id": "01f09972611f100000000000000000f1",
          "alias": "total_revenue",
          "sql": ["SUM(orders.order_amount)"],
          "display_name": "total revenue",
          "synonyms": ["revenue", "total sales"],
          "comment": ["Sum of order amounts."],
          "instruction": ["Use this measure for revenue calculations."]
        }
      ]
    }
  },
  "benchmarks": {
    "questions": [
      {
        "id": "01f0d0b4e815100000000000000000a1",
        "question": ["What is the average order value?"],
        "answer": [
          {
            "format": "SQL",
            "content": ["SELECT AVG(order_amount) as avg_order_value\n", "FROM sales.analytics.orders"]
          }
        ]
      }
    ]
  }
}
```

## Validation Rules (Databricks Conversation API)

### ID Rules

- All IDs must be 32-character lowercase hexadecimal strings with no hyphens.
- IDs are required in: `config.sample_questions`, `instructions.text_instructions`, `instructions.example_question_sqls`, `instructions.sql_functions`, `instructions.join_specs`, `instructions.sql_snippets.filters`, `instructions.sql_snippets.expressions`, `instructions.sql_snippets.measures`, and `benchmarks.questions`.
- All IDs in `serialized_space` must be unique.

### Sorting Rules

- `data_sources.tables` sorted by `identifier` (ascending).
- `data_sources.tables[].column_configs` sorted by `column_name` (ascending).
- `data_sources.metric_views` sorted by `identifier` (ascending).
- `data_sources.metric_views[].column_configs` sorted by `column_name` (ascending).
- `instructions.example_question_sqls[].parameters` sorted by `name` (ascending).

### Size and Length Limits

- `serialized_space` string must be <= 3.5 MB.
- Combined size of `comment`, `instruction`, and `usage_guidance` fields must be <= 64 KB.
- Any single string value inside `description`, `content`, `question`, `sql`, `instruction`, or `synonyms` must be <= 1 KB.
- Each table identifier part (`catalog`, `schema`, `table`) must be <= 255 characters.

### Content Constraints

- Use schema version `2` for new spaces.
- Maximum number of tables/metric views is workspace-dependent.
- `benchmarks.questions[].answer` must contain exactly one item, and its `format` must be `SQL`.
- Databricks guidance recommends at least 5 tested `example_question_sqls` and at least 5 benchmark questions.
