# Genie Space Maturity Curve

> Every Genie Space starts at Nascent. Your score shows where you are on the journey — not how well you did.

## Configuration

The maturity scoring is **configurable by workspace admins**.

- **Default config:** [`backend/maturity_config_default.yaml`](../backend/maturity_config_default.yaml) — shipped with the app
- **Admin overrides:** Applied via the Admin Settings UI (`/api/admin/maturity-config`)
- **How it works:** Each criterion has a registered check function in the scanner. Admins can adjust point weights, enable/disable criteria, change stage thresholds, and add custom criteria IDs (which require a corresponding check function in code).

## Stages

### 1. Nascent | Score: 0–29

**Key Question:** Can Genie see my data?

Tables attached but minimal configuration. Answers are unpredictable.

| Criteria | Type | Points | Description |
|----------|------|--------|-------------|
| tables_attached | boolean | 10 | At least one table is attached to the space |
| table_count | count | 0–10 | Number of tables attached (2-10 ideal range) |
| columns_exist | boolean | 5 | Tables have columns defined |

### 2. Basic | Score: 30–49

**Key Question:** Does Genie understand my domain?

Some instructions and table descriptions. Genie is starting to understand context.

| Criteria | Type | Points | Description |
|----------|------|--------|-------------|
| instructions_defined | boolean | 5 | General text instructions are set for the space |
| table_descriptions | boolean | 5 | Tables have descriptions or comments |
| column_descriptions | count | 0–5 | Proportion of columns with descriptions |

### 3. Developing | Score: 50–69

**Key Question:** Does Genie speak my language?

Instructions, sample questions, and joins defined. Genie understands the domain.

| Criteria | Type | Points | Description |
|----------|------|--------|-------------|
| instruction_quality | count | 0–5 | Text instructions have meaningful content (>50 chars) |
| sample_questions | count | 0–5 | Number of example SQL questions defined |
| joins_defined | boolean | 5 | Join specifications configured for multi-table spaces |
| filter_snippets | boolean | 5 | Filter snippets defined for common business segments |

### 4. Proficient | Score: 70–84

**Key Question:** Are Genie's answers consistent?

Trusted SQL queries and expressions added. Reliable, metrics-accurate answers.

| Criteria | Type | Points | Description |
|----------|------|--------|-------------|
| trusted_sql_queries | count | 0–10 | Number of example SQL queries |
| expressions_defined | count | 0–5 | SQL expressions and measures for business metrics |
| unity_catalog | boolean | 7 | All tables use fully-qualified Unity Catalog names |

### 5. Optimized | Score: 85–100

**Key Question:** Is Genie ready for everyone?

Full SQL coverage, benchmarks, and feedback loops. Production-grade self-service.

| Criteria | Type | Points | Description |
|----------|------|--------|-------------|
| benchmark_questions | count | 0–8 | Benchmark questions for accuracy tracking |
| sql_coverage | count | 0–5 | Breadth of SQL examples covering diverse patterns |
| sql_functions | boolean | 5 | Custom SQL functions for complex business logic |

## Scoring Summary

| Stage | Score Range | Key Signal |
|-------|------------|------------|
| Nascent | 0–29 | Tables attached |
| Basic | 30–49 | Instructions + table descriptions |
| Developing | 50–69 | Sample questions + joins + filters |
| Proficient | 70–84 | Trusted SQL queries + expressions + UC compliance |
| Optimized | 85–100 | Benchmarks + full SQL coverage + functions |

## Admin Configuration API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/admin/maturity-config` | GET | Get active config (default + overrides) |
| `/api/admin/maturity-config` | PUT | Save admin overrides (partial merge) |
| `/api/admin/maturity-config/reset` | POST | Reset to defaults |

### Example: Adjust a criterion's weight

```json
PUT /api/admin/maturity-config
{
  "config": {
    "criteria": [
      { "id": "tables_attached", "points": 15 }
    ]
  }
}
```

### Example: Disable a criterion

```json
PUT /api/admin/maturity-config
{
  "config": {
    "criteria": [
      { "id": "sql_functions", "enabled": false }
    ]
  }
}
```

### Example: Change stage thresholds

```json
PUT /api/admin/maturity-config
{
  "config": {
    "stages": [
      { "name": "Nascent", "range": [0, 24], "key_question": "Can Genie see my data?", "description": "..." },
      { "name": "Basic", "range": [25, 44], "key_question": "Does Genie understand my domain?", "description": "..." },
      { "name": "Developing", "range": [45, 64], "key_question": "Does Genie speak my language?", "description": "..." },
      { "name": "Proficient", "range": [65, 84], "key_question": "Are Genie's answers consistent?", "description": "..." },
      { "name": "Optimized", "range": [85, 100], "key_question": "Is Genie ready for everyone?", "description": "..." }
    ]
  }
}
```

## Axes

- **X-axis:** Genie Space Maturity (stage progression)
- **Y-axis:** Business User Confidence (exponential growth with maturity)
