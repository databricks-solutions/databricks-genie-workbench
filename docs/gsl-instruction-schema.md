# GSL Instruction Schema

## Why a schema

Three agents write `text_instructions`:

- **Create Agent** — generates the initial block when a user builds a
  new space.
- **Fix Agent** — patches the block in response to IQ Scanner findings.
- **Optimizer (GSO)** — rewrites the block as part of
  benchmark-driven optimization.

Historically each used a different authoring convention, so a Create
Agent space with `## Terminology` headers would be overwritten by a
Fix Agent patch that emitted loose prose, or stripped by an
optimizer that expected `PURPOSE:` ALL-CAPS headers. This doc is the
shared vocabulary Create and Fix both target so the output is
coherent end-to-end. The optimizer migrates to this schema in
Workbench 0.1 (#91, #173).

## Section vocabulary

Five canonical sections. Present them in this order; **omit empty
sections** — do not leave an empty header.

| # | Header | What goes here |
|---|---|---|
| 1 | `## PURPOSE` | One or two bullets stating the space's scope and audience. |
| 2 | `## DISAMBIGUATION` | Clarification-question triggers: "When the user asks about X without specifying Y, ask them to clarify Y." Also, term-resolution rules: "'Q1' means calendar Q1 unless the user says 'fiscal Q1'." |
| 3 | `## DATA QUALITY NOTES` | Caveats about the data the model needs to know: NULL handling, known bad rows, column semantics that aren't in the column description. |
| 4 | `## CONSTRAINTS` | Hard guardrails: what never to show (PII columns, secrets), what not to do (cross-join, ignore a required filter). |
| 5 | `## Instructions you must follow when providing summaries` | Summary-customization behavior: rounding rules, mandatory caveats, date-range statements. This header is Databricks's verbatim blessed string — do not paraphrase it. |

## Format rules

- **Markdown `## Header`** for each section.
- **ALL-CAPS (`## PURPOSE`) or title/sentence case (`## Purpose`) is acceptable.**
  Prompts and future validators treat them interchangeably. The summary-behavior
  section stays in sentence case because Databricks docs call that exact string out.
- **Dash bullets** (`- …`) for each rule. Keep one idea per bullet.
- **Blank line between sections.**
- **No SQL inside bullets.** SQL goes in `sql_snippets` (reusable
  expressions and measures) or `example_question_sqls` (full query
  patterns). This is the scanner's rule — see `_SQL_IN_TEXT_RE` in
  `backend/services/scanner.py`.
- **Keep total content ≤ 2,500 characters** — the IQ Scanner's soft
  threshold in check #4 (text-instructions length). Longer blocks push
  out higher-value SQL context in the Genie prompt window.
- **Each bullet should reference a concrete asset** (table, column,
  user phrase) or be a specific behavioral rule. Vague guidance ("be
  helpful", "follow best practices") is an anti-pattern per Databricks.

## Verbatim example

```markdown
## PURPOSE
- Answer questions about order revenue for FY2024 US retail orders.
- Users are merchandising managers — assume retail/e-commerce fluency.

## DISAMBIGUATION
- When the user asks about "customer performance" without a time range, ask them to clarify the period.
- "Q1" means calendar Q1 unless the user says "fiscal Q1".

## DATA QUALITY NOTES
- orders.order_amount is NULL for cancelled rows — filter with is_cancelled = false.
- Returns appear in dim_returns one day after the sale — allow for T+1 reconciliation when joining.

## CONSTRAINTS
- Never show PII columns (customer_email, customer_phone).
- Do not project raw payment tokens.

## Instructions you must follow when providing summaries
- Round percentages to two decimal places.
- Always state the date range used in the summary.
```

## What does NOT go in `text_instructions`

Per <https://docs.databricks.com/aws/en/genie/best-practices>,
`text_instructions` is a last resort. The following content belongs in
other config layers:

| Content | Target config layer |
|---|---|
| Metric / filter / expression definitions (e.g. `revenue = SUM(orders.order_amount)`) | `instructions.sql_snippets` (expressions / measures / filters) |
| Full example queries and multi-step query patterns | `instructions.example_question_sqls` |
| Join conditions | `instructions.join_specs` |
| Table / column documentation | table `description` / `column_configs[].description` / `synonyms` |

Keep `text_instructions` focused on natural-language guidance that
Genie cannot infer from the structured config.

## Agent-specific behavior

### Create Agent

Emits the section vocabulary above when generating a new space. Each
section is one or more bullets. Output shape stays
`content: list[str]` during this near-term pass; migration to the
canonical single-item `[full_text]` shape is tracked in #177
(Workbench 0.1).

### Fix Agent

**Must preserve existing section headers when patching.**
`instructions.text_instructions[N].content` is the only patchable path
into this block (`backend/prompts.py::_VALID_FIELD_PATHS_BLOCK`). When
the Fix Agent proposes a patch, it must:

1. Identify the Markdown `## Section` headers already in the content.
2. Preserve each header in its `new_value`; edit only bullets within
   a section, or add a new section at the correct position in the
   order above.
3. If the only way to address a finding is to delete a canonical
   section, decline the patch by returning
   `{"decline": true, "rationale": "..."}` instead.

### Optimizer (GSO) — NOT YET MIGRATED

GSO currently emits a **wider section vocabulary in ALL-CAPS plain
text** (see
`packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py`
`INSTRUCTION_SECTION_ORDER` / `INSTRUCTION_FORMAT_RULES`). The Fix
Agent's header-preservation rule handles GSO-authored content
correctly because "preserve existing headers" applies whether they
are Markdown or ALL-CAPS plain text.

Full GSO alignment (Markdown format, narrower vocabulary, content
routing to `sql_snippets` / `join_specs` / `example_question_sqls`,
shared Python module) is tracked in epic #173.

## References

- Databricks best practices: <https://docs.databricks.com/aws/en/genie/best-practices>
- Genie Space serialized schema: <https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field>
- Near-term epic: #87 (this doc + #89 Create Agent + #90 Fix Agent)
- Full unification epic: #173 (Workbench 0.1)
- IQ Scanner check this schema supports: `backend/services/scanner.py` check #4 (text-instructions length + SQL-in-text)
