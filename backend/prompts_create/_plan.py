"""Step 4: Build the Plan — compose instructions, sample questions, benchmarks, SQL."""

STEP = """\
### Current Step: Build the Plan

Present a **complete plan** for user review in a single, well-structured message. The plan should include:

1. **Space title, description, audience**
2. **Selected tables** (with any excluded columns noted)
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

   Incorporate patterns from `profile_table_usage` query history where available — real query patterns make better few-shot examples than synthetic ones. Adapt them: clean up user-specific filters, add a natural question, and test via `test_sql`.

5. **Filters** — reusable WHERE clause snippets for common filter patterns (suggest based on data inspection)

   Each filter has a `display_name`, `sql` (a WHERE condition without the WHERE keyword), and optional `synonyms`.
   These should be self-contained SQL predicates, not conceptual rules. Example: `YEAR(order_date) = YEAR(CURRENT_DATE())` for "current year".

6. **Measures** — reusable aggregation SQL for key metrics

   Each measure has an `alias`, `sql` (an aggregate expression), and optional `display_name`/`synonyms`.
   Put the actual aggregation formula here, not in text instructions. If the user defined "conversion rate = orders / visits", create a measure with `sql: "CAST(COUNT(DISTINCT order_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT session_id), 0)"`.

7. **Benchmark queries** (5-10 pairs) — for validating the space after creation

   Benchmarks are test questions used to verify Genie produces correct SQL. They should:
   - Include specific expected SQL or expected result characteristics
   - Cover edge cases (nulls, empty results, ambiguous terms)
   - Test business rules the user defined
   - Include at least 1-2 questions that probe time range handling
   - Include at least 1-2 questions that test metric definitions

   Use patterns from `profile_table_usage` query history to make benchmarks realistic.

8. **Sample questions** (3-5) — displayed in the space as conversation starters

   These should match the audience level. For executives: "What were our top 5 products by revenue this quarter?" For analysts: "Show me the daily trend of conversion rate over the past 30 days." Incorporate business context (fiscal definitions, terminology).

**Format the plan clearly** using markdown. End with:
> "Does this look good? I can adjust any section, or proceed to building the configuration."

**Important:** The user must approve the plan before you move to generation. If they request changes, regenerate the affected sections.

**Skipping:** If the user explicitly says "just create it" or "use defaults," generate a minimal plan with sensible defaults, present it briefly, and proceed after a quick confirmation."""

SUMMARY = "Step 4 (Plan): Compose a full plan (instructions, SQL examples, filters, measures, benchmarks, sample questions) using inspection findings + business context."
