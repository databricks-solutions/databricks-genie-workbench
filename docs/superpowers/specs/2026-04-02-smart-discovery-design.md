# Smart Table Discovery — Design Spec

**Branch:** `improve-create-agent-sz`
**Parent spec:** `docs/superpowers/specs/2026-04-01-improve-create-agent-design.md`

## Problem

The current discovery flow forces users through a rigid catalog → schema → table hierarchy. This causes:
- Users who don't know their data path get lost
- The agent explores wrong catalogs/schemas (#51)
- Multiple tool calls just to find the right tables (3+ round trips)
- No way to search by intent ("I need healthcare claims data")

## Solution

Replace the hierarchical drill-down with a **search-first discovery** approach. The agent uses the user's business requirements to generate search terms (exact words, synonyms, abbreviations, domain patterns), runs a single broad SQL query against `system.information_schema`, and ranks results by relevance to the user's questions.

**Two paths:**
1. **Smart search** (default): Agent infers what data is needed, searches, recommends tables with explanations
2. **Direct path** (fallback): User specifies a catalog/schema/table directly, agent goes straight there

---

## New Tool: `search_tables`

### Tool Definition

```python
search_tables(
    keywords: list[str],       # LLM-generated search terms
    catalogs: list[str] = [],  # Optional: scope to specific catalogs. Empty = all accessible.
    max_results: int = 50      # Cap to keep LLM context manageable
)
```

### SQL Query Strategy

Queries `system.information_schema.tables` joined with `system.information_schema.columns`, matching keywords against table names, column names, table comments, and column comments via OR + LIKE:

```sql
SELECT DISTINCT
    t.table_catalog, t.table_schema, t.table_name,
    t.comment AS table_comment, t.table_type,
    t.last_altered,
    collect_set(
        CASE WHEN lower(c.column_name) LIKE '%keyword%'
              OR lower(c.comment) LIKE '%keyword%'
        THEN c.column_name END
    ) AS matching_columns,
    count(DISTINCT c.column_name) AS total_columns
FROM system.information_schema.tables t
LEFT JOIN system.information_schema.columns c
    USING (table_catalog, table_schema, table_name)
WHERE (
    lower(t.table_name) LIKE '%keyword1%' OR lower(t.comment) LIKE '%keyword1%'
    OR lower(c.column_name) LIKE '%keyword1%' OR lower(c.comment) LIKE '%keyword1%'
    OR lower(t.table_name) LIKE '%keyword2%' ...
)
AND t.table_catalog IN (...)  -- if catalogs specified
GROUP BY 1,2,3,4,5,6
ORDER BY t.last_altered DESC
LIMIT {max_results}
```

### Return Format

```json
{
  "tables": [
    {
      "full_name": "prod.healthcare.medical_claims",
      "comment": "Medical insurance claims with diagnosis and procedure codes",
      "table_type": "MANAGED",
      "total_columns": 24,
      "matching_columns": ["diagnosis_code", "procedure_code", "claim_amount"],
      "matched_keywords": ["claims", "diagnosis"]
    }
  ],
  "search_terms_used": ["claims", "clm", "diagnosis", "dx", "patient", "member"],
  "catalogs_searched": ["prod", "samples"],
  "total_matches": 30
}
```

---

## Search Algorithm (LLM-Driven Keyword Generation)

The agent generates 10-20 search terms before calling `search_tables`. The prompt instructs the agent to think through these categories:

| Category | Example: "healthcare claims cost analysis" |
|----------|-------------------------------------------|
| Exact terms | `claims`, `cost`, `healthcare` |
| Synonyms | `medical`, `insurance`, `expense`, `spend`, `payment` |
| Abbreviations | `clm`, `med`, `hc`, `rx`, `dx`, `px` |
| Related entities | `patient`, `member`, `subscriber`, `provider`, `facility` |
| Common table prefixes | `fact_claims`, `dim_patient`, `raw_claims`, `gold_claims` |
| Metric-related | `amount`, `total`, `count`, `rate`, `avg` |

**Principle:** Over-retrieve, then rank. Better to return 30 tables with 5 great matches than to miss the right table because the search was too narrow.

**Result ranking:** The agent evaluates each result against the user's business questions:
- Explains why each recommended table is relevant
- Flags dimension tables that could enrich the analysis
- Warns about tables that look like staging/ETL artifacts

---

## Discovery Prompt Rewrite

The `_discovery.py` prompt changes from hierarchical navigation instructions to:

1. **Analyze requirements** — extract what data structures the user needs
2. **Generate search terms** — exact words, synonyms, abbreviations, domain conventions, related concepts
3. **Call `search_tables`** — one call with the generated keywords
4. **Rank and recommend** — present top 3-5 tables with explanations tied to business questions
5. **Direct path shortcut** — if the user named specific catalog/schema/tables, skip search and use `discover_tables` or `describe_table` directly

**Handling changes:** The user can change their mind at any point:
- "Also look for provider data" → run `search_tables` again with new keywords, merge results
- "Remove that table" → drop from `session.selected_tables`
- "Use this table instead" → swap in selection
- "That's not what I need" → re-search with different terms

The agent never makes the user restart the entire flow.

---

## Tool Availability (Discovery Step)

```python
STEP_TOOLS["discovery"] = {
    "search_tables",          # Primary: smart search
    "discover_catalogs",      # Fallback: hierarchical browsing
    "discover_schemas",       # Fallback
    "discover_tables",        # Fallback
}
```

---

## Files Changed

| File | Change |
|------|--------|
| `backend/services/create_agent_tools.py` | Add `search_tables` tool definition + `_search_tables()` handler |
| `backend/services/uc_client.py` | Add `search_tables_sql()` function — builds and executes the information_schema query |
| `backend/prompts_create/_discovery.py` | Rewrite for search-first flow with keyword generation instructions |
| `backend/prompts_create/_tools.py` | Add `search_tables` to tool sequence guidelines |
| `backend/services/create_agent.py` | Add `search_tables` to `STEP_TOOLS["discovery"]` set |

**Not changed:** Frontend (agent presents results conversationally), existing discovery tools (kept as fallback), session state, other steps.

---

## Error Handling

- **Warehouse not running:** `search_tables` auto-starts the configured SQL warehouse before executing the query. The app already has `SQL_WAREHOUSE_ID` configured — use the existing `sql_executor` to start it if stopped.
- **Query timeout:** If information_schema query takes >30s, cancel and fall back to `list_summaries` API (faster but name-only, single catalog)
- **No results:** Agent tells the user no matches were found, asks for more context or a different description, suggests trying the direct path
- **Too many results:** Capped at `max_results` (default 50). Agent filters to the most relevant in its response.
