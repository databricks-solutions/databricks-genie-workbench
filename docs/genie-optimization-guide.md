# Best Practices: Patching a Genie Agent After Poor Benchmark Results

## Phase 1: Diagnosis — Understand What Failed and Why

### 1.1 Pull and classify benchmark results

Start by reading the benchmark run results. Every evaluation falls into one of three buckets:

* **Passed** (score = 100) — no action needed
* **Failed** (score = 0) — Genie produced clearly wrong SQL
* **Needs review** (score = 50) — Genie produced partially correct SQL or the judge couldn't decide; this is NOT passing

Report the summary verbatim (e.g. "8/15 passed, 4 failed, 3 need review") and work on all non-passing evals together.

### 1.2 Cluster failures into themes (3–6 themes)

Use two axes to cluster:

| Axis | What to look for |
| --- | --- |
| **Primary: score_reasons** | Group evals sharing the same dominant reason (wrong filter, wrong aggregation, wrong table/field, missing business logic, misinterpretation) |
| **Secondary: applied-context signals** | Refine by shared columns in `filter_columns`/`join_columns`/`select_columns`, same cited instruction line numbers, same missing entity in `qn_entities`, or same expected-vs-generated SQL diff pattern |

Don't force every failure into a theme — set aside one-offs with low pattern signal.

### 1.3 Rank themes by impact

For each theme compute: failure count, fix confidence (High/Medium/Low), and fix scope (local vs broad). Rank by `failure_count × fix_confidence`. Attack the highest-impact themes first.

### 1.4 Resolve cited instructions

Before proposing fixes, read the full instruction set and cross-reference anything cited in the failing evals. This reveals whether instructions are poorly worded, conflicting, or simply absent.

---

## Phase 2: Failure Mode Taxonomy — Map Each Theme to a Root Cause

Each cluster of failures typically maps to one of these root causes:

### A. Incorrect SQL Logic
Genie's SQL is mechanically wrong — e.g. `COUNT(*)` instead of `COUNT(DISTINCT id)`.

### B. Inconsistency / Conflicting Definitions
A business term has multiple competing definitions across text instructions, SQL snippets, or column descriptions.

### C. Misapplied Semantics
Genie picks a plausible-but-wrong column, table, or filter — e.g. filtering on `open_date` when the user asked about `close_date`.

### D. Ignored Instructions
An author-written instruction exists but Genie doesn't follow it — often because of conflicting examples, vague phrasing, or context noise.

### E. Overconfidence
Genie answers a question it shouldn't have, misusing available columns to approximate something outside the space's domain.

### F. Time-based / Timezone Errors
Wrong day boundaries, wrong timezone, or off-by-one date math.

### G. Out-of-scope
The question requires capabilities Genie doesn't support (ML forecasting, Python logic, etc.). No context fix helps — document the limitation.

---

## Phase 3: Fix Strategies — Concrete Actions Per Root Cause

### For Incorrect SQL Logic:
1. **Add a SQL measure or derived expression** — e.g. a `sql_measure` snippet named "total_messages_sent" with content `COUNT(DISTINCT message_id)`. Measures get picked up across all matching questions.
2. **Add synonyms to the snippet** — so varied phrasings ("total messages", "message count") all route to the same expression.
3. **Add an example SQL pair** — a complete question/SQL pair demonstrating the correct usage for complex multi-step logic.

### For Inconsistency / Conflicting Definitions:
1. **Resolve conflicting instructions** — identify the canonical definition, update or delete the contradicting item.
2. **Remove duplicate snippets** — if two expressions define the same term differently, keep one.
3. **Hide overlapping columns** — if similar columns confuse Genie, hide the non-canonical one.
4. **Add a single authoritative SQL expression** — the canonical definition as a reusable building block.

### For Misapplied Semantics:
1. **Update column descriptions** — make similar columns clearly distinguishable. Use "NOT to be confused with…" phrasing.
2. **Add column synonyms** — so "close date" maps to `cls_date`, not `opp_date`.
3. **Enable entity matching (value indexing)** — on low-cardinality categorical columns whose values users reference by name.
4. **Add a SQL filter snippet** — for standardized filtering logic (e.g. "Active customers" → `status = 'active'`).
5. **Add or fix JOIN snippets** — if Genie joins on the wrong key, add a `join` knowledge snippet with the correct ON condition and relationship type.
6. **Hide noise columns** — internal IDs, audit fields, and debug columns that create ambiguity.

### For Ignored Instructions:
1. **Rephrase instructions more directly** — vague instructions ("be careful", "use the right column") are ignored. Be specific.
2. **Resolve instruction conflicts** — an example SQL may contradict a text instruction; remove or align the conflicting item.
3. **Move logic from text to SQL expressions** — SQL expressions are applied more reliably than prose. Convert "always use COUNT DISTINCT for messages" into an actual `sql_measure`.
4. **Reduce text instruction volume** — too many text instructions dilute each other. Consolidate related rules.
5. **Add an example SQL pair** — examples teach more reliably than text for complex behaviors.

### For Overconfidence:
1. **Add scope-limiting text instructions** — "This space cannot answer questions about delivery logistics. Decline such questions."
2. **Add column descriptions with negative constraints** — describe what a column does NOT represent.
3. **Register certified answers** — for mission-critical questions where only one verified response should be returned.

### For Time-based / Timezone Errors:
1. **Add an explicit timezone instruction** — name the source timezone, target timezone, and the exact conversion function.
2. **Document day/week/month boundary expressions** — e.g. "Use `date_trunc('day', convert_timezone('UTC', 'America/Los_Angeles', ts))` for daily aggregations."

---

## Phase 4: Instruction Placement Rules — Put Knowledge in the Right Surface

| Content type | Correct surface | Wrong surface |
| --- | --- | --- |
| Business term definition (natural language) | Text instruction | SQL example |
| Reusable aggregate metric (`SUM(revenue)`) | `sql_measure` snippet | Text instruction or SQL example |
| Calculated column (`revenue - cost`) | `sql_derived` snippet | Text instruction |
| Standard filter (`status = 'active'`) | `sql_filter` snippet or entity matching | Text instruction |
| Table JOIN relationship | `join` snippet | Text instruction or SQL expression |
| Complete Q&A pair (complex multi-step query) | SQL example (title = question, content = SQL) | Text instruction |
| Column disambiguation | Column descriptions + synonyms | Text instruction |
| Categorical value recognition | Entity matching (value indexing) | Text instruction or filter |

**Key principle:** SQL expressions (measures, derived columns, filters) are composable building blocks that Genie can reuse across many questions. SQL examples only fire on close text-match to their title. Text instructions are the weakest signal — use them only for guidance that can't be expressed as SQL.

---

## Phase 5: Metadata Quality — The Foundation That Enables Everything

Before adding instructions, ensure your metadata baseline is solid:

1. **Table descriptions** — every table should have a plain-language description of what it represents.
2. **Column descriptions** — every user-facing column should have a description that disambiguates it from similar columns. "A string" or "an ID" is not a description.
3. **Column synonyms** — add alternative names users might type (e.g. "close date", "closed date", "cls date" all map to `cls_date`).
4. **Column visibility** — hide internal columns (audit IDs, ETL timestamps, debug flags) that add noise.
5. **Entity matching** — enable on low-cardinality categorical columns (status, region, tier, product_type) whose values users reference by name.
6. **JOIN relationships** — add `join` snippets for every FK relationship between tables in the space. These are the most reliably applied context type.

---

## Phase 6: Authoring SQL Expressions — Getting the Content Right

### 6.1 Choosing the right type

| Type | When to use | Content format | Example |
| --- | --- | --- | --- |
| `sql_measure` | Aggregate metrics (SUM, COUNT, AVG, MAX, MIN) | Bare aggregate expression — no `SELECT`, no `FROM` | `COUNT(DISTINCT message_id)` |
| `sql_derived` | Row-level calculated columns | Bare expression — Genie inserts it into the SELECT list | `revenue - cost` |
| `sql_filter` | Standard WHERE clause conditions | Bare condition — no `WHERE` keyword | `status = 'active' AND is_deleted = false` |

**Rule of thumb:** if the expression contains an aggregate function, it's a `sql_measure`. If it's a row-level calculation, it's a `sql_derived`. If it's a boolean condition meant for filtering, it's a `sql_filter`.

### 6.2 Naming and title

The title/name is what Genie matches against when deciding whether to apply the expression:

* Use a descriptive, human-readable name: `total_messages_sent`, `net_revenue`, `active_customer_count`
* The name should read like the business concept it represents — not the SQL mechanics
* Avoid generic names like `count_1`, `metric_a`, or `custom_calc`

### 6.3 Synonyms — expanding the match surface

Synonyms dramatically increase how often Genie applies the expression. Add every phrasing a user might type:

* For `total_messages_sent`: "total messages", "message count", "messages sent", "number of messages", "how many messages"
* For `net_revenue`: "net rev", "revenue after costs", "profit margin", "net income"
* For `active_customer_count`: "active customers", "active accounts", "current customers"

**More synonyms = broader applicability.** This is the single highest-leverage field on a SQL expression.

### 6.4 Content format rules

* **Bare expression only** — never include `SELECT`, `FROM`, `WHERE`, or full query structure. Genie composes the expression into the query it's building.
* **Reference column names as they appear in the table** — use the actual column name (e.g. `msg_id`), not a synonym or display name.
* **Keep it self-contained** — the expression should work when dropped into any valid query context against that table.

### 6.5 When to use a SQL expression vs. an example SQL pair

| Use a SQL expression when... | Use an example SQL pair when... |
| --- | --- |
| The logic is a single expression (one aggregate, one calculation, one filter) | The logic requires multiple steps, CTEs, subqueries, or CASE statements spanning many lines |
| The concept applies across many possible questions | The concept only makes sense in one specific query shape |
| You want broad, automatic applicability | You want to demonstrate a specific multi-table orchestration |

**Default to SQL expressions.** They're composable, broadly applied, and don't depend on title-matching. Only use example SQL pairs for complex multi-step logic that can't be captured in a single expression.

---

## Phase 7: Space Configuration — Global Settings That Affect All Answers

The `description` field on the space acts as a global natural-language instruction to Genie. It should define:

* The space's domain and purpose (e.g. "This space answers questions about sales pipeline performance for the Revenue Operations team")
* The intended audience and their terminology
* Behavioral guardrails (e.g. "Always ask which date field to use if the question is ambiguous")

A vague or empty description forces Genie to infer scope from tables alone — leading to overconfidence and misapplied semantics.

---

## Phase 8: Knowledge Mining Sources — Where to Find Fix Content

Before writing fixes from scratch, mine these sources for pre-existing knowledge:

* **Declared primary keys and foreign keys** → free JOIN snippets with correct ON conditions
* **`topJoins`** from UC table metadata → observed JOIN patterns from actual query history
* **Column comments** already in UC → candidate column descriptions (copy into the space if missing)

---

## Phase 9: Common Anti-Patterns — What Makes Fixes Fail

### 9.1 Instruction placement errors (reverse audit)

| What you see | Why it fails | What to do instead |
| --- | --- | --- |
| Text instruction containing SQL keywords (`SELECT`, `WHERE`, `CASE WHEN`) | Text instructions are weakest signal; SQL logic buried in prose gets ignored | Convert to `sql_measure`, `sql_filter`, or `sql_derived` snippet |
| SQL example whose title is a single business term and body is one aggregate | Examples only fire on close title text-match; a measure applies broadly | Convert to a `sql_measure` snippet with synonyms |
| SQL example whose body is a single derived expression (`revenue - cost`) | Same issue — too narrow a match surface | Convert to a `sql_derived` snippet |
| Vague text instructions ("be careful", "consider context", "use the right column") | No actionable signal for Genie to follow | Rewrite with specifics or delete entirely |
| Vague example-SQL titles (≤4 generic words like "Get sales", "Show numbers") | Never match real user questions | Rewrite title as the full natural-language question the example answers |

### 9.2 Benchmark overfitting

Fixes should generalize beyond the specific benchmark set:

* Benchmark questions are not exhaustive — extract patterns that help many question shapes, not just the ones that failed
* If a fix only helps one benchmark but wouldn't help a rephrased version of the same question, it's too narrow (e.g. adding an example SQL whose title is the exact benchmark question verbatim, with no synonyms)
* Prefer `sql_measure`/`sql_derived` snippets (broad applicability) over example SQL pairs (narrow title-match) when the logic is a single expression

### 9.3 Raw/bronze table attachment

Attaching raw or bronze-layer tables degrades performance because:

* Column names are cryptic (e.g. `col_1`, `fld_xyz`, `raw_ts_utc_v2`)
* Complex nested types (arrays of structs) confuse SQL generation
* No clear domain semantics for Genie to reason about

**Prefer:** pre-modeled views, silver/gold-layer tables, or metric views. If a well-formed metric view exists on the same data, attach that instead of the raw source.

### 9.4 SQL context gaps

A space is almost guaranteed to produce poor benchmark results if it has:

* **0 SQL expressions** (`sql_measure`/`sql_derived`/`sql_filter`/`join` snippets) but multiple tables and benchmark questions
* **0 example SQL** but the benchmark set covers ≥3 distinct question shapes
* Columns heavily referenced in benchmarks or popular queries but **no corresponding `sql_measure`/`sql_derived` snippet** giving them a business semantic

---

## Quick Reference: Fix Priority Order

When time is limited, apply fixes in this priority order (highest impact per effort):

1. **Add missing JOIN snippets** — most reliably applied, unblocks multi-table questions
2. **Add SQL measures/derived expressions** — composable across many questions
3. **Fix column descriptions and synonyms** — resolves misapplied semantics at the root
4. **Enable entity matching on categorical columns** — resolves value-recognition failures
5. **Add SQL filter snippets** — standardizes common WHERE clauses
6. **Add example SQL pairs** — teaches complex multi-step logic
7. **Rewrite or remove conflicting text instructions** — resolves ignored-instruction failures
8. **Hide noise columns** — reduces ambient confusion
