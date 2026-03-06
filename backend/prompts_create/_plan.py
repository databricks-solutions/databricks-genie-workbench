"""Step 4: Build the Plan — compose instructions, sample questions, benchmarks, SQL."""

STEP = """\
### Current Step: Build the Plan

Present a **complete plan** for user review in a single, well-structured message. The plan should include:

1. **Space title, description, audience**
2. **Selected tables** (with any excluded columns noted)
3. **Text instructions** — specific guidance for Genie about the domain

   Text instructions tell Genie HOW to answer questions. Write them as numbered rules. Good text instructions:
   - Define ambiguous terms ("revenue" means net revenue after refunds)
   - Specify default behaviors (default time range is current month)
   - Call out data quirks (the `status` column uses 'Active'/'Inactive' — case-sensitive)
   - Describe important joins and relationships
   - Warn about known data quality issues (null patterns, type mismatches)

   **IMPORTANT**: Integrate any **business context** the user provided in Step 1d directly into these rules. For example, if the user said "Q1 = Feb-Apr", add a rule: "When the user asks about Q1, use February through April."

4. **Example SQL pairs** (5-10 question + SQL pairs) — these teach Genie by example

   Each pair has a natural-language question and a validated SQL query. The SQL must:
   - Use fully-qualified table names (catalog.schema.table)
   - Be tested via `test_sql` before inclusion
   - Cover the most common question types for this domain (aggregations, filters, joins, time ranges)

   Incorporate **business context** into examples: if the user said "always use current year," reflect that in the SQL with YEAR(current_date()).

   Also incorporate patterns from `profile_table_usage` query history where available — real query patterns make better few-shot examples than synthetic ones. Adapt them: clean up user-specific filters, add a natural question, and test via `test_sql`.

5. **Filters** — which columns should be filterable (suggest based on data inspection)
6. **Measures** — which columns represent key metrics (suggest aggregations)

   When defining measures, reflect **business context**. If the user defined "conversion rate = orders / visits," create a measure with that exact formula.

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
