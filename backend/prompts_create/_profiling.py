"""Step 5: Data Readiness Profiling — assess whether the data can answer the user's business questions."""

STEP = """\
### Current Step: Data Readiness Profiling

After inspection is complete, evaluate whether the selected tables can actually answer the user's \
business questions from step 1 (requirements). This bridges the gap between "what does this data look like?" \
(inspection) and "can this data answer my questions?" (readiness).

**Call `assess_readiness` once** with:
- `table_identifiers`: the selected tables (same ones you just inspected)
- `business_questions`: the user's business questions from the requirements step

The tool crawls Unity Catalog metadata for those tables and evaluates readiness across four pillars:
1. **Semantic coverage** — do the tables have columns that match the measures and dimensions the questions need?
2. **Data quality & freshness** — are column types well-defined? (full DQ was already done in inspection)
3. **Modelability** — are there enough tables with potential join keys for dimensional analysis?
4. **GenAI context readiness** — do tables and columns have descriptions that help Genie understand the data?

It also produces **per-question confidence bands** (High / Medium / Low) estimating whether each \
business question can be reliably answered.

**Present the readiness report conversationally.** Don't dump the raw output. Lead with the overall \
assessment, then highlight specific gaps:

> "Based on your 5 business questions and the tables we inspected, here's the readiness picture:
>
> **Overall: Medium readiness**
>
> **What looks good:**
> - Your revenue and cost questions map well to columns in `orders` and `expenses`
> - Table and column documentation is solid (72% coverage) — Genie will understand the context
>
> **Gaps to address:**
> - Q3 asks about regional breakdowns, but none of your tables have a geographic dimension
> - Q5 references 'conversion rate' which needs both visits and orders — visits data isn't in scope
>
> **Per-question confidence:**
> - Q1 (revenue by quarter): **High** — `order_date` + `amount` are present
> - Q2 (spend vs budget): **Medium** — spend data exists but no budget table
> - Q3 (regional breakdown): **Low** — no geographic columns found
>
> **Recommendations:**
> 1. Consider adding a `dim_region` table for geographic breakdowns
> 2. Add descriptions to the 28% of columns that are missing them
> 3. Q5 may need a separate `web_visits` table — or adjust the question"

**Let the user decide what to do:**
1. **Proceed to plan** — if readiness is High or Medium and the user is comfortable with gaps
2. **Go back to add more tables** — if gaps can be filled by including additional tables
3. **Adjust business questions** — if some questions can't be supported by available data
4. **Proceed anyway with a warning** — if readiness is Low but the user wants to continue

**Do NOT auto-advance.** The user must explicitly choose to proceed before moving to plan generation."""

SUMMARY = "Step 5 (Profiling): Call `assess_readiness` with selected tables and business questions. Present readiness report with per-question confidence bands and gap recommendations."
