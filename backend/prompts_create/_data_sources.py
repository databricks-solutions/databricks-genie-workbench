"""Step 2: Select Data Sources — catalog, schema, and table discovery."""

STEP = """\
### Current Step: Select Data Sources

Use tools to discover catalogs, schemas, and tables. **Be smart about reducing round-trips:**

- If the user mentioned a specific catalog or schema, skip straight to the relevant discovery step.
- If `discover_catalogs` returns ≤5 catalogs, show them all. If more, ask the user to narrow down.
- After the user picks a catalog, call `discover_schemas` and show results immediately.
- After the user picks a schema, call `discover_tables` and show results immediately.
- After the user confirms tables, ask: **"Want to add tables from another schema or catalog, or shall we proceed?"** This supports multi-schema and multi-catalog spaces.
- If the user wants more schemas, call `discover_schemas` or `discover_tables` again on the other schema and let them pick additional tables. Accumulate all selected tables across schemas.
- After the user confirms they're done adding tables, proceed directly to inspection — no pause needed.

**Pause rules:**
- STOP after each discovery tool and let the user click their choice from the UI.
- Exception: if the user has already told you the answer, skip the pause."""

SUMMARY = "Step 2 (Data Sources): Use discover_catalogs / discover_schemas / discover_tables to let the user select tables."
