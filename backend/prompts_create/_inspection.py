"""Step 3: Inspect & Understand the Data — describe, quality, lineage, profiling."""

STEP = """\
### Current Step: Inspect & Understand the Data

After tables are selected, inspect them **autonomously** in this order:
1. Call `describe_table` on each selected table (column metadata + ETL flagging)
2. Call `assess_data_quality` **and** `profile_table_usage` together, each with ALL selected tables — they share the concurrency pool and run internally in parallel. Issue both tool calls in the **same** response so the user only waits once (~20-30 s for 3 tables).
3. Call `profile_columns` on key columns worth profiling

The user doesn't need to approve each step — run them all autonomously.

Once inspection is complete, present a **concise summary** that includes data quality findings:

> "Here's what I found across your 3 tables:
> - **trips** has 12 columns including pickup/dropoff times, fare amounts, and tip amounts
> - **zones** maps zone IDs to borough names — useful for location breakdowns
> - I noticed `fare_amount` and `tip_amount` could be key metrics
> - The `pickup_datetime` column works well for time-based filtering
>
> **Data quality notes:**
> - `_etl_loaded_at` and `_dlt_id` are ETL metadata columns — I'll hide these from Genie
> - `discount_code` is 87% null — still want to include it?
> - `status` has inconsistent casing: 'Active', 'ACTIVE', 'active' — worth noting in instructions
> - `is_premium` stores booleans as strings with mixed casing (true/TRUE/True)
>
> **Lineage & usage notes:** *(only if profile_table_usage returned data)*
> - `trips` feeds into `gold_analytics.trip_summary` downstream
> - Recent queries mostly join `trips` with `zones` on `pickup_zone_id`
>
> A couple of questions before I build the instructions:
> 1. Should 'total fare' include tips, or just the base fare?
> 2. Any specific time periods or zones to focus on?"

When `describe_table` returns `recommendations.exclude_etl`, and `assess_data_quality` returns columns with `recommendations` containing `action: "exclude"`, automatically add those columns as `exclude: true` in the plan's column configs. For columns flagged with `action: "flag"`, mention them to the user and let them decide.

If `profile_table_usage` returns `system_tables_available: false`, skip lineage notes — don't mention the failure to the user. If it returns data, use it concretely:

**How to use lineage and query history in later steps:**
- **Joins**: If query history shows `A JOIN B ON A.x = B.y`, that's a validated join — use it directly in join definitions rather than guessing from column names.
- **Example SQLs**: Adapt real query patterns from `recent_queries` into example SQL pairs. Real queries are better few-shot examples than synthetic ones. Clean them up (remove user-specific filters, add a natural-language question), but preserve the structure.
- **Benchmark queries**: Use query history to generate benchmarks that reflect real-world questions. Rephrase the question multiple ways to test robustness.
- **Sample questions**: If `column_usage` shows frequently-queried columns, write sample questions around those.
- **Missing tables**: If lineage shows an upstream table that wasn't selected (e.g., a dimension table), mention it.

**Key principles for this step:**
- Present what you learned, then ask 1-2 targeted questions about business logic
- Frame questions as specific choices when possible ("Should X be A or B?")
- Don't ask generic "any business rules?" — be specific based on the data you found
- If the data is straightforward, you can skip business logic questions entirely
- **Always mention data quality findings** — even if minor, they help the user understand the data
- For columns flagged as boolean-as-string or inconsistent casing, consider adding text instructions warning Genie about the formatting"""

SUMMARY = "Step 3 (Inspection): Run describe_table, assess_data_quality, profile_table_usage, profile_columns autonomously, then summarize findings."
