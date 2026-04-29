"""Step 6: Generate, Validate & Create — produce the JSON, validate it, and create the space.

Note: This is step 6 after discovery (2) + feasibility (3) were added.
"""

STEP = """\
### Current Step: Generate & Create

**IMPORTANT: Only enter this step when the user explicitly approves.** This happens when `action: "create"` is in their message, or they clearly say "create it" / "looks good, go ahead". Do NOT proceed to this step on your own.

Once approved:
1. Call `discover_warehouses` to find SQL warehouses
2. **Auto-select the best warehouse** (prefer running serverless). Mention which one you picked — the user can override.
3. Call `generate_config` with **minimal arguments** — just pass `tables` if you need to override column settings. The system automatically injects all plan data (sample_questions, text_instructions, example_sqls, etc.) from the approved plan. Do NOT regenerate the plan data as arguments — this wastes time and tokens.
4. Call `validate_config` immediately after. If `generate_config` fails, call `get_config_schema` to review the expected parameter shapes, then retry.
5. If validation fails, fix and re-validate automatically
6. Call `create_space` with a `description` — write 1-2 sentences summarising what business questions this space answers and which tables/domains it covers. Then share the URL.

The "Approve & Create" button IS the approval. Go straight from plan approval to creation in one step.

If `validate_config` reports errors:
- Fix `test_sql` failures by adjusting the SQL and re-testing
- Fix schema mismatches by re-running `describe_table`
- Fix other errors based on the error messages
- Re-validate after all fixes

**Do NOT create the space until validation passes with 0 errors.**

After creation, present the result with column-level detail:
> "Your Genie Space **NYC Taxi Analytics** is ready!
> [Open in Databricks →](link)
>
> **What's configured:**
> - 3 tables, 7 example SQL pairs, 4 measures, 2 filters, 3 expressions
> - 2 join specs, 8 text instructions, 5 benchmark questions
> - Format assistance & entity matching: ON for all non-excluded columns
> - Excluded: `_etl_loaded_at`, `_dlt_id` (ETL metadata)
>
> Want me to run the benchmark queries to validate the space? Or would you like to adjust anything?\""""

SUMMARY = "Step 6 (Generate & Create): Discover warehouses, generate_config, validate_config, create_space."
