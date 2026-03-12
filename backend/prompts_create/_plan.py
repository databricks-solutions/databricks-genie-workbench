"""Step 4: Build the Plan — compose instructions, sample questions, benchmarks, SQL."""

STEP = """\
### Current Step: Build the Plan

Generate a **complete plan** for user review using the `generate_plan` tool.

**IMPORTANT — use `generate_plan` (not `present_plan`):**
Call `generate_plan` with a `user_requirements` string summarizing the user's goals, audience, business context, terminology, and any rules they mentioned. The tool automatically:
1. Extracts all table metadata and inspection findings from session history
2. Runs **4 parallel LLM calls** to generate every section simultaneously (4x faster)
3. Returns the plan as a `present_plan` result for the user to review

Only fall back to `present_plan` with manually constructed data if the user asks to **revise specific sections** after seeing the generated plan.

**Guiding principle:** Use every schema feature that adds value. The serialized_space schema has many sections — tables, column configs, text instructions, example SQLs, join specs, measures, filters, expressions, SQL functions, metric views, benchmarks, and sample questions. If the data or business context suggests a feature would help Genie answer questions more accurately, **include it**. A rich config produces a more capable space.

The plan should include:

1. **Space title, description, audience**
2. **Selected tables** (with column-level detail)
   - **Column descriptions**: Add descriptions for columns whose names are ambiguous or domain-specific
   - **Column synonyms**: Add synonyms for columns users might refer to by different names (e.g., "cust_id" → "customer ID", "account number")
   - **Excluded columns**: List ETL metadata, internal IDs, and irrelevant columns to hide from Genie
   - **Metric views**: Include any metric views discovered during inspection — they simplify pre-aggregated metrics
3. **Text instructions** — domain knowledge that CAN'T be expressed as SQL snippets, examples, joins, or column metadata

   Text instructions are injected into Genie's LLM prompt. To avoid overlap with other config sections, follow this MECE boundary:

   | Concern | Goes in | NOT in text instructions |
   |---|---|---|
   | Aggregation formulas (SUM, AVG, ...) | **Measures** | ~~"use SUM(net_revenue) for revenue"~~ |
   | Reusable WHERE clauses | **Filters** | ~~"filter by YEAR(date) = YEAR(CURRENT_DATE())"~~ |
   | Computed columns | **Expressions** | ~~"margin = revenue - cost"~~ |
   | Table relationships | **Join specs** | ~~"join trips to zones on zone_id"~~ |
   | Column aliases / alternate names | **Column synonyms** | ~~"'pickup location' means pickup_zone_id"~~ |
   | Full question→SQL patterns | **Example SQLs** | ~~"when asked about top products, use ..."~~ |

   **Text instructions OWN these exclusively:**
   - **Terminology disambiguation**: what a business term *means* conceptually (e.g., "'revenue' means net revenue after returns — NOT gross")
   - **Default assumptions**: implicit scope when the user doesn't specify (e.g., "default to current calendar year", "default region is US")
   - **Fiscal/calendar conventions**: Q1=Feb-Apr, fiscal year starts Feb 1, etc.
   - **Data quality warnings**: casing inconsistencies, high null rates, boolean-as-string columns, deprecated columns to avoid
   - **Business rules that span multiple columns/tables**: "an 'active customer' has at least 1 order in the last 90 days AND a non-null email"
   - **Response behavior**: "when results are empty, suggest the user broaden the date range"

   **Format: categorized sections with if-then rules.**

   ```
   ## Terminology
   - "revenue" means net revenue (after returns and discounts), NOT gross revenue.
   - "active customer" = customer with at least 1 order in the last 90 days AND a non-null email address.

   ## Default Assumptions
   - When no time range is specified, default to the current calendar year.
   - When no region is specified, include all regions.

   ## Fiscal Calendar
   - Fiscal quarters: Q1=Feb-Apr, Q2=May-Jul, Q3=Aug-Oct, Q4=Nov-Jan.
   - If the user says "this quarter", use the current fiscal quarter, not calendar quarter.

   ## Data Quality Warnings
   - The `status` column has inconsistent casing ('Active', 'ACTIVE', 'active'). ALWAYS use LOWER(status) when filtering.
   - `discount_code` is 87% NULL — warn the user if results look sparse.
   - `is_premium` stores booleans as strings ('true'/'false') — use LOWER(is_premium) = 'true', not a boolean comparison.
   ```

   **Formatting rules:**
   - **Categorize** under `##` headers (Terminology, Default Assumptions, Fiscal Calendar, Data Quality, etc.)
   - **Use if-then rules** for conditional behavior: "If the user says X, interpret it as Y"
   - **Use ALWAYS/NEVER** for hard constraints
   - **Put critical rules first** — LLMs have a primacy bias
   - **No SQL snippets** — if a rule needs a SQL formula, it belongs in measures, filters, expressions, or example SQLs instead

   **IMPORTANT**: Integrate any **business context** the user provided in Step 1d into the appropriate category above.

4. **Example SQL pairs** (5-10 question + SQL pairs) — complete question→query patterns that teach Genie by example

   Each pair has a natural-language question and a validated SQL query. The SQL must:
   - Use fully-qualified table names (catalog.schema.table)
   - Be tested via `test_sql` before inclusion
   - Cover the most common question types for this domain (aggregations, filters, joins, time ranges)
   - Demonstrate business rules from text instructions applied in practice (e.g., if text instructions say "default to current year", the example SQL should show that)

   **Use parameterized SQL** (`:param_name` syntax) when the question involves a user-supplied value that varies per query. This teaches Genie to generalize — when a user asks "show me sales for EMEA", Genie matches the pattern and extracts "EMEA" as the parameter value.

   Use parameterized SQL when:
   - The question filters by an entity: "Show sales for North America" → `WHERE region = :region_name`
   - The question involves a threshold: "Show orders above $1000" → `WHERE amount > :min_amount`
   - The question filters by a date: "Show data for January" → `WHERE month = :target_month`

   Use hardcoded SQL when:
   - The pattern is always the same: "What is total revenue?" → no parameters needed
   - The value is a business rule, not user input: "default to current year" → `YEAR(CURRENT_DATE())` hardcoded
   - The example teaches a structural pattern (GROUP BY, JOIN, window functions) where the value doesn't matter

   **The question must be concrete — use the default value, not a placeholder.** Users ask "show me sales for North America", not "show me sales for a specific region". The parameterization lives in the SQL only. Genie learns the pattern and generalizes to other values from the parameter metadata.

   **Every parameter MUST include `default_value` and `description` — both using REAL values from the data** (from `describe_table` or `profile_columns` results). The `default_value` gets used as the initial parameter value when Genie runs the query, so a fake value would produce wrong or empty results. The `description` should list 2-3 real distinct values so Genie knows the value domain.

   Example:
   ```
   question: "Show sales for North America"
   sql: "SELECT ... FROM ... WHERE region = :region_name"
   parameters: [{
     name: "region_name",
     type_hint: "STRING",
     description: "The sales region. Values: North America, EMEA, APJ, LATAM",
     default_value: "North America"
   }]
   ```

   Aim for a mix: ~3-5 hardcoded examples for structural patterns, ~2-5 parameterized examples for entity-specific queries.

   **Usage guidance:** Add `usage_guidance` to each example SQL to tell Genie when this pattern applies (e.g., "Use this pattern for any top-N ranking question by a numeric metric"). This helps Genie pick the right example when a user asks a similar question.

   **Testing parameterized SQL:** When calling `test_sql` on parameterized queries, pass the `parameters` array with each parameter's `name` and `default_value`. The tool substitutes `:param_name` with the default value before execution. Without this, the query will fail with an UNBOUND_SQL_PARAMETER error.

   Incorporate patterns from `profile_table_usage` query history where available — real query patterns make better few-shot examples than synthetic ones. Adapt them: clean up user-specific filters, add a natural question, and test via `test_sql`.

5. **Filters** — reusable WHERE clause snippets for common filter patterns (suggest based on data inspection)

   Each filter has a `display_name`, `sql` (a WHERE condition without the WHERE keyword), and optional `synonyms`, `instruction`, and `comment`.
   - `instruction`: tells Genie WHEN to apply this filter (e.g., "Apply when users ask about high-value or large orders")
   - `comment`: internal note about the threshold/business context (e.g., "Threshold aligned with finance team's $1000 definition")
   These should be self-contained SQL predicates, not conceptual rules. Example: `YEAR(order_date) = YEAR(CURRENT_DATE())` for "current year".

6. **Measures** — reusable aggregation SQL for key metrics

   Each measure has an `alias`, `sql` (an aggregate expression), `display_name`, and optional `synonyms`, `instruction`, and `comment`.
   - `instruction`: tells Genie WHEN to use this measure (e.g., "Use for any revenue aggregation")
   - `comment`: internal note explaining the formula or business context
   Put the actual aggregation formula here, not in text instructions. If the user defined "conversion rate = orders / visits", create a measure with `sql: "CAST(COUNT(DISTINCT order_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT session_id), 0)"`.

7. **Expressions** — reusable computed columns / dimension expressions

   Each expression has an `alias`, `sql` (a dimension expression), `display_name`, and optional `synonyms`, `instruction`, and `comment`.
   Use for date dimensions (`YEAR(order_date)`), computed categories (`CASE WHEN amount > 1000 THEN 'High' ELSE 'Low' END`), or derived columns that Genie should know about.

8. **Join specs** — table relationships for multi-table queries

   Define join specs when 2+ tables need to be joined. Each has `left_table`, `right_table`, `left_column`, `right_column`, `relationship` (MANY_TO_ONE, ONE_TO_MANY, etc.), and optional `instruction` and `comment`.
   - `instruction`: tells Genie WHEN to use this join (e.g., "Use when customer demographics are needed for order analysis")
   - `comment`: describes the relationship in plain language
   Always define joins proactively when multi-table data is selected — don't wait for the user to ask.

9. **SQL functions** — Unity Catalog UDFs available to the space

   If `discover_tables` or the user mentioned custom SQL functions (UDFs) relevant to the domain, include them. Each needs an `identifier` (catalog.schema.function_name). The function must already be registered in Unity Catalog.

10. **Benchmark queries** (5-10 pairs) — for validating the space after creation

   Benchmarks are test questions used to verify Genie produces correct SQL. They should:
   - Include specific expected SQL or expected result characteristics
   - Cover edge cases (nulls, empty results, ambiguous terms)
   - Test business rules the user defined
   - Include at least 1-2 questions that probe time range handling
   - Include at least 1-2 questions that test metric definitions

   Use patterns from `profile_table_usage` query history to make benchmarks realistic.

11. **Sample questions** (3-5) — displayed in the space as conversation starters

   These should match the audience level. For executives: "What were our top 5 products by revenue this quarter?" For analysts: "Show me the daily trend of conversion rate over the past 30 days." Incorporate business context (fiscal definitions, terminology).

**IMPORTANT:** Do NOT write the plan out as a markdown text block — the frontend renders the tool result as an interactive card with collapsible sections and inline editing. A duplicate markdown summary is redundant and clutters the chat.

After calling `generate_plan`, say something brief like:
> "Here's the plan — click any item to edit it inline, add or remove items. When you're ready, choose an action below."

Do NOT add a verbose summary of the plan's contents (purpose, audience, table stats, etc.) — the plan card already shows everything.

**Important:** The user must approve the plan before you move to generation. If they request changes to specific sections, use `present_plan` with the corrected data.

**Skipping:** If the user explicitly says "just create it" or "use defaults," call `generate_plan` with a brief requirements summary, present the result, and proceed after a quick confirmation."""

SUMMARY = "Step 4 (Plan): Call generate_plan (parallel 4x faster) with user requirements. The tool auto-extracts inspection data and generates all sections in parallel."
