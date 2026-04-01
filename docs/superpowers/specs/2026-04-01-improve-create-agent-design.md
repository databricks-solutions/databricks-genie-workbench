# Improve Create Agent — Design Spec

**Branch:** `improve-create-agent-sz`
**Issues:** #44, #50, #51, #52, #53, #54, #25

## Problem

The create agent rushes through tool calls without enough conversation, doesn't respect user choices, and doesn't give users visibility or control over what it's doing. Specific complaints from CUJ feedback:

- Never asks for real business questions upfront (#54)
- Ignores user's specified catalog, searches across multiple catalogs/schemas (#51)
- Immediately chains tool calls after catalog selection instead of pausing (#50)
- Data selection workflow (catalog/schema/table) is unclear (#44)
- Cross-table scanning during inspection is confusing (#52)
- No step to review/edit AI-generated column and table descriptions (#53)
- General: needs to be more conversational (#25)

Root cause: the agent's prompts prioritize speed/autonomy over user understanding. There are no structural guardrails to enforce conversational flow — the LLM can chain tools freely and skip past user checkpoints.

## Approach

Restructure the agent step flow, rewrite prompts to be conversational, add structural guardrails (session state gating + tool filtering per step), and add a two-phase plan review UI for table/column metadata. Remove auto-pilot mode entirely.

---

## New Step Structure

**Current:**
```
requirements -> data_sources -> inspection -> plan -> config_create -> post_creation
```

**Proposed:**
```
requirements -> discovery -> feasibility -> inspection -> plan -> config_create -> post_creation
```

### Step Details

#### 1. `requirements` (improved)

Gather purpose, audience, title. Explicitly ask for real business questions the user wants Genie to answer. Collect business context as it comes up naturally (fiscal year, KPIs, terminology). Don't force a checklist — if the user gives rich context upfront, move on. Conversational, as many or few exchanges as needed.

**Available tools:** None (pure conversation).

#### 2. `discovery` (replaces `data_sources`)

Based on requirements, scan UC metadata at a high level. No SQL profiling, no `describe_table`, no cross-table scanning. Just catalog/schema/table metadata (names, comments, row counts, column counts).

Rules:
- If the user named a catalog, call `discover_schemas(catalog)` directly — never `discover_catalogs`. Only call `discover_catalogs` if the user genuinely doesn't know where their data is.
- Pause after every discovery tool. No chaining `discover_catalogs` -> `discover_schemas` -> `discover_tables` in one batch.
- No cross-schema exploration without permission. After showing tables in the user's chosen schema, ask "Want to add tables from another schema?" — don't proactively scan other schemas.
- Metadata only. No SQL execution, no profiling.

**Available tools:** `discover_catalogs`, `discover_schemas`, `discover_tables`.

**Session state populated:** `selected_catalogs`, `selected_schemas`, `selected_tables`.

#### 3. `feasibility` (new)

LLM-only assessment — no new tools. The agent reasons over the user's business questions (from requirements) and the selected tables' UC metadata (from discovery) to assess whether the data can support the intended Genie Space.

The agent presents its confidence conversationally: "These tables look well-suited for your revenue tracking questions. I notice there's no customer demographics table though — the 'top customers by region' question might be hard without one. Want to add a table for that, adjust your questions, or proceed as-is?"

User decides: proceed to deep inspection, go back to add tables, or adjust requirements.

**Available tools:** None (LLM reasoning over existing context).

**Session state populated:** `feasibility_confirmed`.

#### 4. `inspection` (narrowed)

Deep profiling only on confirmed tables. Three tools per table:
- `describe_table` — schema, columns, sample rows
- `profile_columns` — distinct values, null rates
- `assess_data_quality` — quality issues

**Dropped:** `profile_table_usage` (cross-table UC scanning). This was confusing to users (#52) and slow (#38). The information it provides (query history, lineage, frequently-used columns) is not essential for plan generation.

**Column cap:** Limit to first 50 columns per table for both `describe_table` and `profile_columns`. If a table has more, note "showing 50 of N columns" and let the user request specific columns.

**Agent behavior:** Summarize findings conversationally after inspection. Don't dump raw tool results — lead with insights. "orders has 12 columns, looks clean. customers has a 72% null rate on email — we might want to exclude that column." Ask targeted business-logic questions based on findings.

If UC metadata lacks table or column descriptions, the agent generates them during this step.

**Available tools:** `describe_table`, `profile_columns`, `assess_data_quality`, `test_sql`.

#### 5. `plan` (improved)

Generate the plan via `generate_plan` (parallel LLM calls, same as today). Present for user review in a **two-phase review UI** (see Frontend Changes below).

**Plan generation adjustments:**
- Bump example SQL generation from 5 to 10 (IQ scan requires 8+, target with margin)
- Ensure table/column descriptions are always populated (generate if UC metadata is missing)
- Text instructions: target 50-2000 char range (IQ scan thresholds)

**Available tools:** `generate_plan`, `present_plan`, `test_sql`.

#### 6. `config_create` (same)

Generate config, validate, create space. Fast-path still works (user clicks "Approve & Create" -> deterministic create, no LLM needed). Auto-chain still works (generate_config -> validate -> create).

**Available tools:** `discover_warehouses`, `generate_config`, `validate_config`, `create_space`.

#### 7. `post_creation` (same)

Update/patch space via `update_config`, `update_space`.

**Available tools:** `update_config`, `validate_config`, `update_space`.

---

## Session State Additions

Add to `AgentSession`:

```python
selected_catalogs: list[str] = []   # Populated during discovery
selected_schemas: list[str] = []    # Populated during discovery
selected_tables: list[str] = []     # Populated during discovery
feasibility_confirmed: bool = False # Set when user proceeds past feasibility
```

These fields:
1. Gate `detect_step()` transitions (e.g., can't enter inspection without `feasibility_confirmed`)
2. Ground the assembled prompt ("User has selected catalog X — do not explore other catalogs")
3. Must be persisted to Lakebase alongside existing session fields

---

## `detect_step()` Changes

Replace tool-call-history inference with session-state gating:

| Step | Entry condition |
|------|----------------|
| `requirements` | Default start state |
| `discovery` | Business requirements gathered (heuristic: user has provided a purpose statement and at least one business question; the `requirements` prompt tells the agent when to transition) |
| `feasibility` | `session.selected_tables` is populated |
| `inspection` | `session.feasibility_confirmed` is True |
| `plan` | `describe_table` tool calls exist for the selected tables |
| `config_create` | `generate_plan` or `present_plan` result exists in history |
| `post_creation` | `session.space_id` is set |

---

## Tool Filtering Per Step

Each step only exposes its allowed tools to the LLM via `TOOL_DEFINITIONS`. This is the strongest structural guardrail — the LLM literally cannot call `describe_table` during discovery because it's not in the tool list.

Tool-to-step mapping defined above in Step Details.

**Skipping steps:** Users can skip steps via natural conversation ("just skip inspection"). The agent advances session state without calling tools. No special UI buttons — the user just says it. This replaces the old auto-pilot per-step skip mechanism.

---

## Auto-Pilot Removal

Strip entirely from:
- `backend/prompts_create/_tools.py` — delete "Auto-Pilot and Step Skipping" section
- `backend/prompts_create/__init__.py` — remove auto-pilot references in `detect_step()` / `assemble_system_prompt()`
- `backend/services/create_agent.py` — remove selections handling for `auto_pilot` / `skip_step` keys
- `frontend/src/components/CreateAgentChat.tsx` — remove auto-pilot toggle UI, skip buttons, `auto_pilot` from selections payloads

---

## `_validate_config` Alignment with IQ Scanner

Add IQ scanner checks 1-10 as **warnings** (not errors) in `_validate_config`. The scanner (`scanner.py`) is not modified. This lets the agent catch quality gaps before creation.

New warnings to add:

| # | Check | Warning condition |
|---|-------|-------------------|
| 2 | Table descriptions | <80% of tables have descriptions; also warn at 80-99% ("aim for 100%") |
| 3 | Column descriptions | <50% of columns have descriptions; also warn at 50-79% ("aim for 80%+"); also warn if no column synonyms |
| 4 | Text instructions | Missing or <=50 chars; also warn if >2000 chars; also warn if SQL patterns found in text |
| 5 | Join specs | No join specs when >1 table |
| 6 | Table count | Warn at 9-12 ("consider splitting"); current >5 warning threshold should be removed to align with scanner |
| 7 | Example SQLs | <8 example SQLs; also warn at 8-14 ("10-15 is the sweet spot"); also warn if >50% lack usage_guidance |
| 8 | SQL snippets | No functions/expressions/measures/filters; also warn if missing filters or measures specifically |
| 9 | Entity/format matching | No columns with entity matching or format assistance; also warn if >100 entity columns (approaching 120 limit) |
| 10 | Benchmarks | <10 benchmark questions |

Existing structural validations (sorting, IDs, format rules) remain as errors.

---

## Frontend Changes

### Two-Phase Plan Review

Split the plan review card into two tabs:

**Tab 1: "Data Schema" (shown first)**
- Summary card per table showing: table name, editable description, included/excluded column counts, column name chips
- "Edit columns" drill-in per table opens column list with:
  - Column name (read-only)
  - Description (editable text field)
  - Type hint badge (read-only)
  - Include/exclude toggle
  - Auto-excluded ETL columns visually dimmed with explanation
- Back button to return to summary view

**Tab 2: "Instructions & SQL"**
- Existing plan card: sample questions, text instructions, joins, measures, filters, expressions, benchmarks — all editable as today

**Data flow:** `present_plan` / `generate_plan` tool result already returns `tables` with column configs. Currently the frontend ignores this for the plan card. Surface it in Tab 1. On "Approve & Create", frontend sends both edited table metadata AND edited instructions/SQL in `selections.edited_plan`.

Data Schema tab shown first — it's the foundation, and #53 specifically calls out that users have no chance to review descriptions.

### Auto-pilot UI Removal

Remove auto-pilot toggle, skip-step buttons, and `auto_pilot` / `skip_step` from selections payloads.

---

## Future Work (Not In Scope)

### Large-Scale Discovery

When catalogs/schemas/tables number in the hundreds or thousands, the current discovery UX breaks (UI overwhelm, LLM context bloat). Proposed approach for later:

- Add optional `filter` parameter to `discover_catalogs`, `discover_schemas`, `discover_tables` for server-side LIKE filtering
- Threshold-based behavior: if count <= threshold, show picker as today. If count > threshold, ask the user to specify by name or keyword, then filter server-side
- Suggested thresholds: catalogs <=10, schemas <=15, tables <=30
- The agent asks conversationally: "You have 300 tables in this schema. What kind of data are you looking for?" instead of dumping a 300-item list

### Catalog Scoping Structural Guardrail

Currently catalog/schema scoping is prompt-level enforcement only. A future enhancement could have the tool handler block or warn when the agent calls `discover_schemas` on a catalog not in `session.selected_catalogs`.

---

## Files Changed

### Backend (prompts)
- `backend/prompts_create/__init__.py` — update `STEP_ORDER`, `detect_step()`, `assemble_system_prompt()` for new steps
- `backend/prompts_create/_requirements.py` — rewrite for conversational business question gathering
- `backend/prompts_create/_data_sources.py` — **delete** (replaced by `_discovery.py`)
- **New:** `backend/prompts_create/_discovery.py` — high-level UC scan with pause rules
- **New:** `backend/prompts_create/_feasibility.py` — LLM-only data fitness assessment
- `backend/prompts_create/_inspection.py` — narrow to describe/profile/quality only, drop profile_table_usage
- `backend/prompts_create/_plan.py` — update for two-phase review
- `backend/prompts_create/_tools.py` — remove auto-pilot section, update tool sequence guidelines

### Backend (services)
- `backend/services/create_agent.py` — new STEP_ORDER/STEP_LABELS/STEP_THINKING, tool filtering per step, session state gating, remove auto-pilot selections handling
- `backend/services/create_agent_session.py` — add `selected_catalogs`, `selected_schemas`, `selected_tables`, `feasibility_confirmed` fields + Lakebase persistence
- `backend/services/create_agent_tools.py` — `_validate_config` IQ scanner alignment, column cap (50) in describe_table/profile_columns
- `backend/services/plan_builder.py` — bump example SQLs from 5 to 10, ensure description generation

### Backend (schema)
- `sql/setup_lakebase.sql` — add `selected_catalogs`, `selected_schemas`, `selected_tables`, `feasibility_confirmed` columns to sessions table (or handle via JSON in existing column)

### Frontend
- `frontend/src/components/CreateAgentChat.tsx` — two-phase plan review UI (Data Schema tab + Instructions tab), remove auto-pilot toggle/skip buttons, update `STEPS` array (add `discovery`/`feasibility`, remove `data`), update `currentStep()` function
- `frontend/src/types/index.ts` — update types for new plan review structure if needed

### Tests
- `tests/test_inspection_loop.py` — update for new step order, remove auto-pilot references
- `tests/test_compaction_400.py` — update for new step order, remove auto-pilot references
- `backend/tests/test_dynamic_prompts.py` — update for new step order, `detect_step()` changes
- `tests/test_full_schema.py` — may need updates for new `_validate_config` warnings

### Dependency notes
- `plan_builder.py` `_build_shared_context()` and `_summarize_usage()` handle missing `profile_table_usage` data gracefully (check for presence) — no structural change needed, just less data available
- Scanner (`scanner.py`) is NOT modified
- Fix agent, optimizer, spaces router, admin router are NOT affected — blast radius is contained to the create agent flow
