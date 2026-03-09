# Optimize Feature: Medium-Term Roadmap

Reference doc for improvements beyond the quick wins (QW1-QW5). These items require more significant effort and are tracked here for future implementation.

## MT1: Multi-Judge Evaluation Pipeline

**Goal:** Replace the single LLM call with a pipeline of specialized judges, each evaluating a different aspect of Genie's response.

**Judges to implement:**
- **SQL Correctness Judge** — Compares generated SQL against expected SQL for semantic equivalence (not just string matching)
- **Schema Alignment Judge** — Checks if Genie used the right tables and columns for the question
- **Filter/Aggregation Judge** — Validates WHERE clauses, GROUP BY, and aggregation functions
- **Join Judge** — Checks if the right joins were used with correct cardinality
- **Result Quality Judge** — Compares actual query results for data accuracy

**Architecture:**
- Each judge runs independently (parallelizable)
- Results feed into a meta-judge that synthesizes findings
- Meta-judge produces the final optimization suggestions

**Effort:** High — requires new judge framework, prompt engineering per judge, result aggregation logic

---

## MT2: Iterative Optimization Loop

**Goal:** Run optimization → apply → re-benchmark → optimize again until convergence or max iterations.

**How it works:**
1. Generate optimization suggestions (current behavior)
2. Apply accepted suggestions to create a new space
3. Re-run benchmarks against the new space
4. Compare scores before/after
5. If accuracy improved but not perfect, generate a new round of suggestions
6. Repeat until convergence (no improvement) or max iterations (3-5)

**Key challenges:**
- Need to track optimization history across iterations
- Must avoid oscillating changes (undo then redo)
- Need a scoring function to measure improvement

**Effort:** High — requires session persistence, history tracking, convergence detection

---

## MT3: Benchmark Generation from Query History

**Goal:** Auto-generate benchmark questions from actual Genie query logs, so optimization is grounded in real user behavior.

**Approach:**
- Pull recent queries from the Genie conversation history API
- Cluster similar queries to identify common patterns
- For each cluster, generate a representative benchmark question
- Use the actual SQL that Genie generated as the expected answer (verified by human)
- Weight benchmarks by query frequency (popular questions matter more)

**Effort:** Medium — requires Genie conversation API integration, clustering logic

---

## MT4: A/B Testing Framework

**Goal:** Create two copies of a space (original + optimized) and route traffic to compare accuracy in production.

**Components:**
- Space cloning with applied optimizations
- Traffic splitting (by user or by question)
- Result collection and comparison dashboard
- Statistical significance testing before recommending the winner

**Effort:** High — requires infrastructure for traffic routing, result collection, and statistical analysis

---

## MT5: Lever-Specific Optimization Strategies

**Goal:** Instead of one generic optimization prompt, use lever-specific strategies that understand the nuances of each optimization lever.

**Levers:**
1. **Data Model** — Table/column descriptions, synonyms, entity matching
   - Strategy: Analyze which columns are referenced in failing queries, improve their descriptions
   - Can use column profiling to suggest entity matching enablement

2. **Joins** — Join specifications between tables
   - Strategy: Analyze query patterns for implicit joins, detect missing or wrong joins
   - Use foreign key detection heuristics and query history

3. **SQL Assets** — Filters, expressions, measures, example SQLs
   - Strategy: Generate new SQL assets from failing query patterns
   - Use the delta between generated and expected SQL to identify missing assets

4. **Instructions** — Text instructions for business rules
   - Strategy: Extract implicit rules from correct answers that are missing from instructions
   - Detect conflicting instructions

**Effort:** Medium-High — requires per-lever prompt engineering and evaluation

---

## Priority Order

1. **MT3** (Benchmark Generation) — Highest ROI, grounds optimization in real usage
2. **MT5** (Lever-Specific Strategies) — Builds on QW2-QW5, improves suggestion quality
3. **MT1** (Multi-Judge Pipeline) — Most impactful for accuracy, highest effort
4. **MT2** (Iterative Loop) — Automation win, depends on MT1 for reliable scoring
5. **MT4** (A/B Testing) — Production validation, depends on MT2 for automation
