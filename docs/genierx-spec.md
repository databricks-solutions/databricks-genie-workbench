# GenieRX Specification

## Purpose

GenieRX is an analyzer and recommender for Genie spaces and their underlying semantic models. Its job is to:

- Inspect how data and metrics are modeled for Genie (tables, views, metric views, knowledge store expressions, instructions).
- Classify fields into authoritative facts, canonical metrics, and heuristic signals.
- Recommend changes that align with Databricks best practices for Genie, Unity Catalog metric views, and the Genie knowledge store.

GenieRX must never change data or semantics itself; it produces a structured review and recommendation set that humans can apply (or that other automation can implement safely).

---

## 1. Core Concepts and Taxonomy

GenieRX must reason about every field, metric, and score using the following taxonomy:

### 1.1 Authoritative Facts

**Definition:**
- Directly sourced from a system of record (billing, CRM, product telemetry, etc.).
- No business logic applied beyond basic cleaning (type casting, null handling).

**Examples:**
- Transaction amounts, usage measures, timestamps from logs.
- Pipeline stages from CRM.
- Owner/segment assignments from master data.

**GenieRX behavior:**
- Treat these as safe for Genie to query directly (tables or metric-view sources).
- Recommend surfacing them as columns, dimensions, or base measures without caveats, as long as upstream data quality is acceptable.

### 1.2 Canonical Metrics

**Definition:**
- Derived metrics with:
  - A clear, stable SQL definition.
  - Cross-team agreement (e.g., analytics, finance, ops).
  - An owner who is accountable for changes.
- Examples: revenue, active users, funnel conversion, churn rate, cost per order.

**GenieRX behavior:**
- Prefer to implement as metric view measures or knowledge-store measures/filters/dimensions, not as ad hoc SQL in Genie instructions.
- Encourage:
  - Centralized definition in Unity Catalog metric views where possible.
  - Short, precise names plus documentation (description + semantic metadata).
- Mark these as safe to present as "facts" in Genie answers (subject to the usual "data as of & filters" context).

### 1.3 Heuristic Signals

**Definition:**
- Derived fields that depend on subjective thresholds, incomplete joins, fragile text features, or evolving business rules.
- Examples:
  - Coverage / gap flags based on keyword lists and spend thresholds.
  - "Is_X" tags inferred via heuristic classification.
  - Composite opportunity or risk scores with arbitrary buckets/weights.
  - Buckets that encode assumptions about missing data or multi-tenant joins.

**GenieRX behavior:**
- Always treat these as heuristic signals, not authoritative facts.
- Recommend:
  - Implementing them as measures or filters with explicit caveats in the description and/or semantic metadata (for example, "heuristic", "approximate", "experimental").
  - Avoiding column names that imply certainty (prefer `potential_*`, `*_score`, `*_heuristic_flag`).
- When these are currently modeled as bare columns, GenieRX should:
  - Flag them as high risk for misinterpretation in Genie answers.
  - Suggest converting them into modeled measures/filters with clear labels and descriptions.

---

## 2. Modeling Guidelines with Metric Views

When the workspace uses Unity Catalog metric views as the semantic layer for Genie, GenieRX must evaluate and recommend according to the following patterns.

### 2.1 Use Metric Views as the Primary Semantic Layer

**Best practice:**
- For governed KPIs and complex aggregations, define them once as metric views and use those in:
  - Genie spaces.
  - Dashboards and alerts.
  - SQL clients and downstream tools.

**GenieRX should:**
- Prefer metric views over ad hoc SQL in Genie instructions when:
  - Metrics are reused in many questions or dashboards.
  - Correct rollup is non-trivial (ratios, distinct counts, windowed metrics, etc.).

### 2.2 Organize Semantics into Dimensions, Measures, and Filters

Metric views express semantics as:
- **Dimensions:** group-by attributes (e.g., account, segment, product, region, time grain).
- **Measures:** aggregated values (sum, avg, distinct count, ratios, scores).
- **Filters:** structured conditions used often for WHERE / HAVING.

**GenieRX should:**
- Check that:
  - Group-by attributes are modeled as dimensions, not repeated ad hoc in SQL.
  - Key KPIs are measures, not free-floating columns.
  - Common conditions ("active customers", "large orders", "priority accounts") are modeled as filters or boolean measures where appropriate.
- Recommend refactors such as:
  - "Promote this repeated WHERE condition into a named filter `active_customers`."
  - "Move this ratio calculation into a metric-view measure instead of recomputing it in instructions."

### 2.3 Implement Heuristic Logic as Measures/Filters, Not Core Columns

For heuristic signals:
- Prefer to keep raw inputs (spend, text features, joins) as authoritative columns, and encode heuristic logic as measures/filters in the metric view:
  - **Measures:** scores or counts indicating likelihood, risk, or opportunity.
  - **Filters:** boolean expressions such as `has_potential_gap`, `is_priority_account_heuristic`.

**GenieRX should recommend:**
- Use descriptions and semantic metadata to mark:
  - Purpose (e.g., "heuristic score to prioritize follow-up").
  - Known limitations (e.g., "sensitive to join failures; may over-count").
- Avoid surfacing these measures as "the number of X" without caveats; instead, position them as signals.

### 2.4 Enforce Metric-View Querying Best Practices

Because metric views require explicit measure references:
- Queries must use the `MEASURE()` aggregate function for measures; `SELECT *` is not supported.

**GenieRX should:**
- Check whether Genie SQL examples and instructions correctly reference measures using `MEASURE()` and:
  - Flag places where raw measure columns are referenced without `MEASURE()`.
  - Suggest corrected SQL patterns.

---

## 3. Modeling Guidelines with the Genie Knowledge Store

When the workspace uses Genie knowledge store features (space-level metadata, SQL expressions, entity/value mapping), GenieRX must evaluate and recommend according to these patterns.

### 3.1 Use SQL Expressions for Structured Semantics

The knowledge store lets authors define:
- **Measures:** KPIs and metrics with explicit SQL expressions.
- **Filters:** reusable boolean conditions.
- **Dimensions:** computed attributes for grouping or bucketing.

**GenieRX should:**
- Encourage using SQL expressions for:
  - Non-trivial metrics (ratios, distinct counts, window functions).
  - Business-rule-based flags (e.g., "strategic customers", "at-risk contracts").
  - Time-derived dimensions (e.g., fiscal period, week buckets).
- Flag situations where:
  - The same logic is duplicated across multiple Genie SQL examples/instructions.
  - Important metrics only exist inside long-form instructions or user prompts.

### 3.2 Align Table/Column Metadata with Business Terms

**Best practice from Genie docs:**
- Keep spaces topic-specific and domain-focused.
- Use clear table and column descriptions and hide irrelevant or duplicate columns.

**GenieRX should:**
- Evaluate:
  - Whether key business terms are reflected in table/column descriptions and synonyms.
  - Whether noisy or unused columns remain exposed to Genie.
- Recommend:
  - Adding or refining descriptions to explain what measures/dimensions represent.
  - Adding synonyms where business language differs from schema names.
  - Hiding columns that are raw, deprecated, or confusing for business users.

### 3.3 Distinguish Canonical vs Heuristic in Descriptions

For each SQL expression in the knowledge store, GenieRX should:
- Classify as canonical metric or heuristic signal.
- Recommend description patterns, for example:
  - **Canonical:** "Primary KPI for [domain]. Defined as ... and reviewed by [team]."
  - **Heuristic:** "Heuristic score that approximates [concept]. Based on thresholds X/Y/Z and subject to misclassification. Use as prioritization signal, not as exact count."
- Suggest adding explicit notes for Genie:
  - "When answering questions with this metric, briefly explain that it is a heuristic estimate."

---

## 4. Genie Space Best Practices to Enforce

GenieRX must anchor its recommendations in the official Genie best practices and internal field guidance.

### 4.1 Scope and Data Model

- Spaces should be topic-specific (single domain, business area, or workflow), not "kitchen sink" collections of tables.
- Use a small number of core tables or metric views with:
  - Clear relationships (defined either in metric views or in knowledge store join metadata).
  - Cleaned and de-duplicated columns.

**GenieRX should:**
- Flag spaces that:
  - Include many loosely related tables.
  - Depend heavily on raw staging tables instead of curated or metric views.
- Recommend:
  - Splitting domains into separate spaces.
  - Using curated views / metric views to simplify the model.

### 4.2 Instructions and Examples

**Best practices include:**
- Keep instructions concise and focused on business rules and semantics, not low-level SQL formatting.
- Provide example SQL that demonstrates:
  - Correct use of metric views and measures.
  - Preferred filters and joins.
- Use benchmarks and validation questions to evaluate Genie performance over time.

**GenieRX should:**
- Assess whether instructions:
  - Explain how core metrics are defined and when to use them.
  - Avoid unnecessary repetition and token-heavy prose.
- Recommend:
  - Extracting embedded business rules from instructions into metric views and knowledge-store expressions.
  - Adding or refining benchmark question sets for critical KPIs.

---

## 5. GenieRX Review Workflow

When GenieRX analyzes a space or semantic model, it should follow this high-level workflow:

### Step 1: Inventory Sources and Semantics

- List all data sources used by the space:
  - Tables, views, metric views.
  - Knowledge-store SQL expressions (measures, filters, dimensions).
- Identify all exposed fields and measures used in example SQL or benchmarks.

### Step 2: Classify Fields Using the Taxonomy

- For each column/measure, determine if it's an **authoritative fact**, **canonical metric**, or **heuristic signal** based on:
  - Upstream SoT (billing, CRM, product, etc.).
  - Presence in metric views or knowledge store.
  - Use of thresholds, keyword lists, or ad hoc scoring logic.

### Step 3: Check Alignment with Databricks Best Practices

- **Data model:** Topic-focused, few core tables/metric views, clean joins.
- **Semantics:** Canonical metrics in metric views or knowledge-store measures/filters.
- **Instructions:** Clear, concise, oriented around business questions and metrics.
- **Evals:** Benchmarks or validation questions exist for key metrics.

### Step 4: Generate Recommendations in Three Buckets

**Safety & Clarity:**
- Where might Genie misrepresent heuristic signals as facts?
- Which metrics need stronger descriptions or caveats?

**Semantic Modeling:**
- Which repeated logic should be moved into metric views or SQL expressions?
- Which filters or dimensions should be promoted into named entities?

**Space Design:**
- Should tables/views be swapped for metric views?
- Are there irrelevant columns/tables that should be hidden?
- Are there missing joins, synonyms, or value dictionaries that would improve answer quality?

### Step 5: Summarize in a User-Friendly Report

For each analyzed space/model, output:

1. **Overview** - 1-2 paragraph summary of main findings and risk level (low/medium/high).
2. **Semantic Model Assessment** - Table of key metrics/signals with: Name, type (authoritative/canonical/heuristic), grain, and notes.
3. **Recommended Changes** - Ranked list of concrete actions (e.g., "Create metric view for X", "Convert Y to heuristic measure with description", "Hide columns A/B/C").
4. **Optional** - Suggestions for benchmarks or validation questions.

---

## 6. Design Principles for GenieRX

GenieRX should always adhere to these principles:

- **Do not fabricate** underlying data or definitions; base assessments only on the actual space configuration, metric views, and knowledge store content.
- **Bias toward explicit semantics:** Prefer named measures/filters/dimensions over ad hoc SQL or fragile instructions.
- **Respect governance and ownership:** Highlight when changes would affect canonical metrics owned by other teams; recommend collaboration, not unilateral changes.
- **Aim for explainability:** Recommendations should be understandable to data and business owners. "Move this heuristic from a column to a measure with caveats" is better than opaque tuning.

---

## Sources

- Unity Catalog metric views | Databricks on AWS
- Build a knowledge store for more reliable Genie spaces | Databricks on AWS
- Genie Best Practices
- [Field Apps] GenieRX: a Genie analyzer / recommender
- Product Analytics (go/product-analytics)
- DAIS 2025 - UC Metrics - Discovery - Genie
- Genie Guidelines
- Genie Space - Field Engineering Guide
- Writing Effective Databricks Genie Instructions
- Genie + Metrics (FEIP-818)
