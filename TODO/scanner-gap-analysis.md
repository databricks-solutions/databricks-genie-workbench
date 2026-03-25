# Genie Workbench Gap Analysis: Best Practices from Enterprise Implementation Guide

## Context

The "Genie Spaces Enterprise Implementation Guide" (v2.0, March 2026) is a 30-page field engineering document capturing best practices from dozens of enterprise deployments. We compared its recommendations against what Genie Workbench currently checks, scores, and enforces — specifically in the IQ Scanner (`scanner.py`), Fix Agent (`fix_agent.py`), Create Agent (`create_agent.py` + `plan_builder.py`), and Auto-Optimize pipeline.

The goal is to identify actionable gaps where adding checks or improving existing ones would meaningfully improve the quality guidance Genie Workbench provides to users.

---

## HIGH Priority Gaps

### Gap 1: Column Description Coverage is Binary (Scanner)

- **PDF says:** Column descriptions should "disambiguate everything — specify units, sign conventions, aggregation guidance." Every queryable column needs a description. This is the #1 accuracy driver.
- **Current state:** Scanner check #3 passes if *any single column across all tables* has a description. A space with 1/200 columns described passes.
- **Fix:** `backend/services/scanner.py` — Measure coverage percentage. Report "Only 12/87 columns (14%) have descriptions — target 50%+ for reliable accuracy." Keep binary check as baseline, add coverage finding.
- **Impact:** Directly affects Retriever + SQL Generator agents. The guide links this to 30% → 45% accuracy jump.

### Gap 2: Table Description Coverage is Binary (Scanner)

- **PDF says:** Table descriptions should be "mission statement format" — purpose, use cases, granularity, update frequency. Every table needs one.
- **Current state:** Scanner check #2 passes if *any* table has *any* non-empty description. 1/8 tables described = pass.
- **Fix:** `backend/services/scanner.py` — Require all (or 80%+) tables to have descriptions. Report which tables are missing. Add soft check for minimum length (~20 chars).
- **Impact:** The Intent Agent uses table descriptions to route questions. Wrong-table = first entry in the curation decision flowchart.

### Gap 3: PK/FK Constraints Not Surfaced (Create Agent)

- **PDF says:** PK/FK constraints are "critical for automatic join path inference" — priority #2 in curation hierarchy.
- **Current state:** `_describe_table` in `create_agent_tools.py` calls `client.tables.get()` but never reads `table_constraints`. The SDK's `TableInfo` has this field, but it's ignored.
- **Fix:** `backend/services/create_agent_tools.py` — Extract `table_info.table_constraints` in `_describe_table` and include PK/FK in the result. The create agent can then auto-generate higher-quality join specs.
- **Impact:** Better join inference = fewer wrong-join errors, which the guide identifies as a key failure mode.

### Gap 4: Example SQL Count Too Low (Scanner + Create Agent)

- **PDF says:** Adding 10-15 example queries = "largest single accuracy jump" (+20 percentage points). This is the biggest ROI after naming/metadata.
- **Current state:** Scanner check #7 requires only 5+ examples. Create agent hard-codes exactly 5 in `plan_builder.py` (`_gen_example_sqls`: "Generate EXACTLY 5 question+SQL pairs").
- **Fix:**
  - `backend/services/scanner.py` — Pass at 5, but add finding recommending 10-15 for "largest accuracy improvement"
  - `backend/services/plan_builder.py` — Change `_gen_example_sqls` to generate 8-10 instead of exactly 5
- **Impact:** The guide's accuracy progression shows this as the single biggest jump: 55% → 77%.

---

## MEDIUM Priority Gaps

### Gap 5: Entity Matching Limit & RLS Gotcha Not Detected (Scanner)

- **PDF says:** Max 120 entity matching columns/space. Entity matching is **silently disabled** for tables with row filters or column masks.
- **Current state:** Scanner check #9 only checks if *at least one* column has entity matching. No count check. No RLS detection. Zero mentions of `row_filter` or `column_mask` in backend code.
- **Fix:**
  - `backend/services/scanner.py` — Count entity matching columns, warn at 100+, error at 120
  - `backend/services/create_agent_tools.py` — Check `table_info.row_filter` / `table_info.column_mask` in `_describe_table`, warn that entity matching will be silently disabled

### Gap 6: Text Instruction Length & Quality (Scanner)

- **PDF says:** Keep text instructions under 2000 chars, <20% of curation effort. Use structured template (PURPOSE, KEY ENTITIES, BUSINESS RULES, CLARIFICATIONS). Text instructions consume LLM context tokens and can push out higher-value SQL context.
- **Current state:** Scanner check #4 only requires >50 chars. No upper bound check. No SQL-in-text detection.
- **Fix:** `backend/services/scanner.py` — Warn if >2000 chars. Detect SQL patterns (SELECT/WHERE/JOIN/GROUP BY) in text instructions and suggest moving to SQL expressions/examples.

### Gap 7: Column Synonyms Not Checked (Scanner)

- **PDF says:** Column synonyms are priority #8 in curation hierarchy. The decision flowchart says: "Wrong column → fix column descriptions + synonyms."
- **Current state:** Scanner has zero checks for column synonyms. The create agent supports them, but users get no signal that they're important.
- **Fix:** `backend/services/scanner.py` — Add soft check/finding: "No column synonyms defined. Add synonyms for columns with abbreviated or technical names."

### Gap 8: SQL Expression Type Balance (Scanner)

- **PDF says:** Three types of SQL expressions: Measures, Filters, Dimensions. Each needs Name, SQL, Synonyms, Instruction.
- **Current state:** Scanner check #8 passes if *any* of SQL functions/expressions/measures/filters exist. No breakdown by type.
- **Fix:** `backend/services/scanner.py` — Report breakdown: "3 measures, 0 filters, 0 expressions — add filters and expressions for better coverage." Flag types that are missing.

### Gap 9: Example SQL Missing Usage Guidance (Scanner)

- **PDF says:** Every example SQL needs a title phrased as users would ask + usage guidance explaining when to apply it.
- **Current state:** Scanner counts example SQLs but doesn't check if `usage_guidance` fields are populated.
- **Fix:** `backend/services/scanner.py` — Add finding if example SQLs lack usage_guidance: "3 of 5 example SQLs have no usage guidance — add descriptions of when each should be applied."

---

## LOW Priority Gaps

### Gap 10: Table Count Threshold Messaging (Scanner)

- **PDF says:** >12 tables = accuracy degrades, >30 = hard limit. Recommend multi-room architecture.
- **Current state:** Scanner uses 1-10 range (more conservative than guide's 12). No mention of multi-room architecture.
- **Fix:** Adjust threshold to 12, add multi-room architecture suggestion for >12 tables.

---

## Key Files to Modify

| File | Gaps |
|------|------|
| `backend/services/scanner.py` | 1, 2, 4, 5, 6, 7, 8, 9, 10 |
| `backend/services/create_agent_tools.py` | 3, 5 |
| `backend/services/plan_builder.py` | 4 |
| `backend/models.py` | If new checks change scoring model |

## Verification

- Run `python tests/test_full_schema.py` after scanner changes
- Deploy via `./scripts/deploy.sh` and test IQ scan on a known space
- Compare IQ scores before/after to ensure no regressions
- Test create agent flow end-to-end to verify PK/FK extraction works
