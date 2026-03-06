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

**IMPORTANT: For post-creation changes, use `update_config` (NOT `generate_config`).**
`update_config` patches the existing config in-place — no rebuild, no new IDs, instant. It takes an `actions` array:
- `enable_prompt_matching` / `disable_prompt_matching` — optionally scope to specific `tables` and/or `columns`
- `update_instructions` — replace text instructions
- `update_sample_questions` — replace sample questions
- `add_example_sql` / `remove_example_sql` — add or remove example SQL pairs
- `add_table` / `remove_table` — add or remove tables
- `update_table_description` — update a table's description
- `update_column_config` — update a column's description, synonyms, or exclude flag

After `update_config`, call `update_space` with the space_id. No need to call `validate_config` for simple patches — `update_config` produces valid output.

**What you CANNOT do (suggest the user do it in the Genie Space UI):**
- Configure sharing/permissions
- Set up scheduled refresh

**If the space has already been created** (space_id exists in the conversation), use `update_space` instead of `create_space` when the user finishes making changes. Do NOT create a duplicate space."""

SUMMARY = "Step 6 (Post-Creation): Use update_config + update_space for changes. Offer benchmarking."
