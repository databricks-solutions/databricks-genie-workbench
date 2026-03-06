"""Step 5: Generate, Validate & Create — produce the JSON, validate it, and create the space."""

STEP = """\
### Current Step: Generate & Create

**IMPORTANT: Only enter this step when the user explicitly approves.** This happens when `action: "create"` is in their message, or they clearly say "create it" / "looks good, go ahead". Do NOT proceed to this step on your own — even in auto-pilot mode.

Once approved:
1. Call `discover_warehouses` to find SQL warehouses
2. **Auto-select the best warehouse** (prefer running serverless). Mention which one you picked — the user can override.
3. Call `generate_config` → `validate_config` in sequence. If `generate_config` fails, call `get_config_schema` to review the expected parameter shapes, then retry.
4. If validation fails, fix and re-validate automatically
5. Call `create_space` immediately and share the URL

The "Approve & Create" button IS the approval. Go straight from plan approval to creation in one step.

If `validate_config` reports errors:
- Fix `test_sql` failures by adjusting the SQL and re-testing
- Fix schema mismatches by re-running `describe_table`
- Fix other errors based on the error messages
- Re-validate after all fixes

**Do NOT create the space until validation passes with 0 errors.**

After creation, present the result:
> "Your Genie Space **NYC Taxi Analytics** is ready!
> [Open in Databricks →](link)
>
> The space has:
> - 3 tables, 7 example SQL pairs
> - 8 text instructions teaching Genie about your domain
> - 5 benchmark questions you can use to test it
>
> Want me to run the benchmark queries to validate the space? Or would you like to adjust anything?\""""

SUMMARY = "Step 5 (Generate & Create): Discover warehouses, generate_config, validate_config, create_space."
