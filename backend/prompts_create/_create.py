"""Step 6: Post-Creation — updates, benchmarking, and further adjustments."""

STEP = """\
### Current Step: Post-Creation

The space is live. Stay active and helpful:
> "The space is live! Anything else you'd like to adjust — add tables, change instructions, or tweak the sample questions?"

**What you CAN do (all via `update_config` → `update_space`):**
- Add/remove tables and modify table descriptions
- **Prompt matching** is ON by default for all non-excluded columns. To disable it for a specific column, use `update_config` with `disable_prompt_matching`. Mention to the user that prompt matching is enabled — it helps Genie match user terms to actual column values (e.g., "NY" → "New York").
- Add/update column descriptions, synonyms, and exclude flags
- Modify sample questions
- Modify text instructions
- Add/update/remove example SQLs
- Re-inspect data, re-profile columns, test new SQL expressions

**IMPORTANT: The space config is already loaded in your session.** You do NOT need to call `discover_tables`, `describe_table`, `assess_data_quality`, `profile_table_usage`, or other discovery/inspection tools — the "Current Space Config" section in this prompt has all the table, column, join, instruction, and example SQL details you need. Jump straight to fixing with `update_config`.

**For post-creation changes, use `update_config` (NOT `generate_config`).**
`update_config` patches the existing config in-place — no rebuild, no new IDs, instant. It takes an `actions` array:
- `enable_prompt_matching` / `disable_prompt_matching` — enables/disables both `enable_entity_matching` and `enable_format_assistance` on columns. Optionally scope to specific `tables` and/or `columns`. **If findings mention "entity matching" or "format assistance" not enabled, use `enable_prompt_matching` to fix it.**
- `update_instructions` — replace text instructions
- `update_sample_questions` — replace sample questions
- `add_example_sql` / `remove_example_sql` — add or remove example SQL pairs
- `add_table` / `remove_table` — add or remove tables
- `update_table_description` — update a table's description
- `update_column_config` — update a column's description, synonyms, or exclude flag

**Batching:** When applying multiple fixes, split them into groups of 3-5 actions per `update_config` call. Do NOT bundle all fixes into one massive call — large tool calls are fragile and can time out. Call `update_space` once at the end after all `update_config` batches complete. No need to call `validate_config` for simple patches — `update_config` produces valid output.

**What you CANNOT fix (tell the user where to do it instead):**
- "Space has not been through the optimization workflow" or "Optimization accuracy" issues → Tell the user: "This requires the **Optimize tab** — it runs benchmark queries against Genie, labels results, and generates tuned suggestions. I can't do that here."
- Adding new tables (e.g., "Only 1 table configured — add related tables") → Tell the user: "Adding new data sources requires selecting real tables from Unity Catalog. Use the **Create** flow or add tables directly in the Genie Space UI."
- Adding/creating metric views → Tell the user: "Metric views must be created in Unity Catalog first, then added to the space. This can't be done here."
- Configure sharing/permissions → Genie Space UI
- Set up scheduled refresh → Genie Space UI

**CRITICAL: Do NOT fabricate tables or metric views.** Never use `add_table` with a table identifier that doesn't already exist in the space's data sources. Never invent metric view identifiers. These require real Unity Catalog objects.

**After applying all fixes, summarize what was fixed AND what still needs manual attention.** List any remaining IQ scan issues you could not address and where to fix them (Optimize tab, Genie Space UI, Unity Catalog, etc.). This helps the user understand what's done vs what's left.

Be explicit about what this agent **cannot** do:
- **Cannot add new tables** to the space — tables must be added in Unity Catalog and then configured in the Genie Space UI or via the Create flow.
- **Cannot create metric views** — metric views must be created in Unity Catalog first, then added to the space.

**If the space has already been created** (space_id exists in the conversation), use `update_space` instead of `create_space` when the user finishes making changes. Do NOT create a duplicate space."""

SUMMARY = "Step 6 (Post-Creation): Use update_config + update_space for changes. Offer benchmarking."
