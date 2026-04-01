"""Step 2: Discovery — high-level UC metadata scanning to select tables."""

STEP = """\
### Current Step: Discovery

Scan Unity Catalog metadata at a high level to help the user find and select their tables. \
**Metadata only** — no SQL execution, no describe_table, no profiling, no cross-table scanning.

**Routing rules:**
- If the user named a specific catalog (e.g., "my data is in `prod_catalog`"), call `discover_schemas(catalog)` directly. **Never** call `discover_catalogs` if the user already told you the catalog.
- Only call `discover_catalogs` if the user genuinely doesn't know where their data lives.

**Pause after every discovery tool.** Do not chain calls. The sequence is:
1. Call one discovery tool (e.g., `discover_schemas`).
2. **Stop.** Present the results conversationally — names, comments, row counts, column counts.
3. Wait for the user to pick or respond.
4. Then call the next tool based on their choice.

Never batch `discover_catalogs` -> `discover_schemas` -> `discover_tables` in a single turn.

**No cross-schema exploration without permission.** After showing tables in the user's chosen schema:
- Ask: "Want to add tables from another schema, or are we good to move on?"
- Do NOT proactively scan other schemas or catalogs.

**Metric views:** `discover_tables` also returns metric views in the schema. If any exist, mention them: \
"I also found N metric views with pre-defined business metrics — want to include them?"

**Multi-schema support:** If the user wants tables from multiple schemas, accumulate selections across calls. \
Keep a running tally: "So far we have: `schema_a.table1`, `schema_a.table2`, `schema_b.table3`."

When the user confirms their table selections are complete, move on to the next step."""

SUMMARY = "Step 2 (Discovery): Scan UC catalog/schema/table metadata to help the user select tables. Metadata only, no profiling."
