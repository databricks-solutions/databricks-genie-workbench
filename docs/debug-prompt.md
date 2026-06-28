# Genie Space Optimizer Run Debugger

## Purpose

This prompt guides an agent through a systematic investigation of a Genie Space Optimizer run, producing an end-to-end narrative of what happened across all pipeline stages: preflight, baseline eval, proactive enrichment, post-enrichment eval, lever loop, and finalization.

## Inputs Required

This prompt ships with no workspace, space, or run baked in. Supply your own values for the placeholders below and substitute every occurrence into the SQL queries that follow.

- **`<WORKSPACE_URL>`**: your Databricks workspace host, e.g. `https://your-workspace.cloud.databricks.com`. The agent connects here — via the Databricks CLI/SDK or a SQL warehouse — to run the queries below.
- **`<SPACE_ID>`**: the Genie Space that was optimized — a 32-character hex string (e.g. `01f17108…a028`).
- **`<RUN_ID>`**: the optimizer run to investigate — a UUID (e.g. `0c43c0ed-…-5f91ef0e69b8`).
- **`<CATALOG>`**: the Unity Catalog catalog holding the `genie_space_optimizer` schema (e.g. `main`). The fully-qualified schema is `<CATALOG>.genie_space_optimizer`.

## Step 1: Get Run Metadata

Query the runs table to understand the run's overall configuration and outcome.

```sql
SELECT *
FROM <CATALOG>.genie_space_optimizer.genie_opt_runs
WHERE space_id = '<SPACE_ID>'
AND run_id = '<RUN_ID>'
```

**What to extract:**
- `status` — terminal status (e.g. MAX_ITERATIONS, CONVERGED, FAILED)
- `started_at` / `completed_at` — total wall-clock duration
- `max_iterations` — iteration budget
- `levers` — which levers were enabled (JSON array of ints 1-6)
- `apply_mode` — how patches are applied (e.g. `genie_config`)
- `llm_model` — which LLM was used
- `best_iteration` — which iteration was promoted as champion
- `best_accuracy` / `best_repeatability` — final best scores
- `convergence_reason` — why the run stopped
- `max_benchmark_count` — target benchmark ceiling

## Step 2: Get All Stages (Chronological)

This is the core timeline. Query stages ordered by timestamp to reconstruct the full execution flow.

```sql
SELECT stage, status, started_at, completed_at, duration_seconds, iteration, detail_json, error_message
FROM <CATALOG>.genie_space_optimizer.genie_opt_stages
WHERE run_id = '<RUN_ID>'
ORDER BY started_at
```

**Reasoning:** Each row is a lifecycle event. Stages with status `STARTED` mark beginnings; `COMPLETE` marks endings (with `detail_json` containing the outcome); `SKIPPED` means the stage was attempted but bypassed (check `detail_json` for `reason_code`).

### Stage Sequence to Expect

1. **PREFLIGHT_STARTED** → PREFLIGHT_METADATA_COLLECTION → DATA_PROFILING → GENIE_BENCHMARK_EXTRACTION → BENCHMARK_GENERATION → PREFLIGHT_SEMANTIC_ALIGNMENT → PREFLIGHT_PREDICATE_VALIDATION → PREFLIGHT_GT_EXECUTION_CHECK → PREFLIGHT_BENCHMARK_PUSH → PREFLIGHT_BENCHMARK_WINDOW → PREFLIGHT_STARTED (COMPLETE)
2. **BASELINE_EVAL_STARTED** (STARTED → COMPLETE)
3. **ENRICHMENT_STARTED** → PROMPT_MATCHING_SETUP → DESCRIPTION_ENRICHMENT → JOIN_DISCOVERY → SPACE_METADATA_ENRICHMENT → PROACTIVE_INSTRUCTION_SEEDING → SQL_EXPRESSION_SEEDING → POST_ENRICHMENT_EVAL_STARTED (optional) → ENRICHMENT_COMPLETE
4. **LEVER_LOOP_STARTED** → [AG_AG1_STARTED → AG_AG1_NO_APPLIED_PATCHES or AG_AG1_EVAL_COMPLETE] × N iterations → LEVER_LOOP_STARTED (COMPLETE)
5. **FINALIZE_STARTED** → REPEATABILITY_TEST → FINALIZE_HEARTBEAT(s) → BENCHMARK_PUBLISH → FINALIZE_TERMINAL → FINALIZE_STARTED (COMPLETE)

### Key Fields in `detail_json` to Parse

| Stage | Key Fields |
|-------|-----------|
| PREFLIGHT_METADATA_COLLECTION | `columns_collected`, `tags_collected`, `routines_collected` |
| DATA_PROFILING | `tables_profiled`, `columns_profiled`, `low_cardinality_columns`, `metric_view_profile_outcomes` |
| GENIE_BENCHMARK_EXTRACTION | `genie_space_benchmarks`, `with_sql`, `question_only` |
| BENCHMARK_GENERATION | `total_count`, `curated_count`, `synthetic_count`, `auto_corrected_count`, `valid_count` |
| PREFLIGHT_SEMANTIC_ALIGNMENT | `checked`, `misaligned`, `remaining` |
| PREFLIGHT_PREDICATE_VALIDATION | `checked`, `mismatched`, `auto_corrected`, `remaining` |
| PREFLIGHT_GT_EXECUTION_CHECK | `checked`, `empty_results`, `remaining` |
| PREFLIGHT_BENCHMARK_PUSH | `added`, `dedup_skipped`, `merged_total`, `existing_count`, `window_status` |
| PREFLIGHT_BENCHMARK_WINDOW | `count`, `window`, `status` |
| BASELINE_EVAL_STARTED (COMPLETE) | `overall_accuracy`, `both_correct_rate`, `thresholds_met` |
| PROMPT_MATCHING_SETUP | `format_assistance_enabled`, `entity_matching_enabled`, `total_changes` |
| DESCRIPTION_ENRICHMENT | `total_eligible`, `total_patches_generated`, `total_enriched`, `total_skipped` |
| JOIN_DISCOVERY | `existing_specs`, `candidates_found`, `total_applied` |
| PROACTIVE_INSTRUCTION_SEEDING | `instructions_seeded`, `instructions_expanded`, `seed_outcome`, `expand_outcome`, `seeded_sections` |
| SQL_EXPRESSION_SEEDING | `total_candidates`, `total_seeded`, `total_rejected` |
| POST_ENRICHMENT_EVAL_STARTED | `accuracy`, `evaluated_count`, `thresholds_met` |
| ENRICHMENT_COMPLETE | `total_enrichments`, `enrichment_skipped`, `post_enrichment_accuracy` |
| AG_AG1_STARTED | `cluster_id`, `impact_score`, `question_count`, `root_cause`, `affected_questions`, `cluster_signature`, `rationale`, `instruction_rewrite_preview` |
| AG_AG1_NO_APPLIED_PATCHES | `reason_code` |
| LEVER_LOOP_STARTED (COMPLETE) | `levers_attempted`, `levers_accepted`, `reflection_buffer` (array of per-iteration outcomes) |
| REPEATABILITY_TEST | `average_pct`, `per_run_pcts`, `total_questions` |
| FINALIZE_STARTED (COMPLETE) | `status`, `promoted_model`, `repeatability_pct`, `terminal_reason` |

## Step 3: Get Iterations (Eval Results)

```sql
SELECT iteration, eval_scope, overall_accuracy, total_questions, correct_count,
       both_correct_rate, thresholds_met, is_champion, repeatability_pct,
       evaluated_count, excluded_count, num_needs_review, failures_json, remaining_failures
FROM <CATALOG>.genie_space_optimizer.genie_opt_iterations
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, eval_scope
```

**Reasoning:**
- `eval_scope = 'full'` is the baseline eval (iteration 0)
- `eval_scope = 'enrichment'` is the post-enrichment eval (iteration 0)
- Higher iterations are lever-loop eval results (if any patches were applied and re-evaluated)
- `failures_json` lists the question IDs that failed
- `is_champion = true` indicates which iteration was promoted

## Step 4: Check Lever Loop Decisions

```sql
SELECT iteration, decision_order, gate_name, decision, reason_code, reason_detail, proposal_ids_json
FROM <CATALOG>.genie_space_optimizer.genie_eval_lever_loop_decisions
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, decision_order
```

**Reasoning:** This table shows which proposals passed which gates. If proposals pass `proposal_grounding` but the overall iteration still ends in `no_applied_patches`, it means proposals were generated and grounded but failed a downstream application step.

## Step 5: Check Patches

```sql
SELECT lever, scope, iteration, COUNT(*) as cnt
FROM <CATALOG>.genie_space_optimizer.genie_opt_patches
WHERE run_id = '<RUN_ID>'
GROUP BY lever, scope, iteration
ORDER BY iteration, lever
```

**Reasoning:**
- `lever = 0, iteration = 0` patches are enrichment-phase patches (descriptions, format_assistance, instructions)
- `lever > 0, iteration > 0` patches are lever-loop patches
- If all patches are at iteration 0, the lever loop generated no persisted patches

## Step 6: Check Tables With No Data

```sql
SELECT '<TABLE_NAME>' as table_name, COUNT(*) as cnt
FROM <CATALOG>.genie_space_optimizer.<TABLE_NAME>
WHERE run_id = '<RUN_ID>'
-- UNION ALL for each table...
```

Tables to check (all have `run_id` column):
- `genie_eval_asi_results`
- `genie_eval_gt_correction_candidates`
- `genie_eval_human_required`
- `genie_eval_lever_loop_decisions`
- `genie_eval_proactive_corpus_profile`
- `genie_eval_proactive_patches`
- `genie_eval_question_regressions`
- `genie_opt_benchmark_mutations`
- `genie_opt_patches`
- `genie_opt_provenance`
- `genie_opt_suggestions`
- `genie_opt_finalize_attestation_matrix`

Note: `genie_opt_data_access_grants` does NOT have a `run_id` column — check it separately with a simple `SELECT COUNT(*)`.

## Step 7: Interpret the Lever Loop Failure Pattern

When the lever loop produces `no_applied_patches` repeatedly, look at:

1. **`root_cause` in AG_AG1_STARTED detail_json** — the classified failure type for the cluster
2. **`reflection_buffer` in LEVER_LOOP_STARTED COMPLETE detail_json** — per-iteration rollback reasons
3. **`instruction_rewrite_preview`** — what the system wanted to write but couldn't apply

Common `root_cause` values and their meaning:
- `unverifiable_no_expected_sql` — benchmarks lack gold-standard SQL; the optimizer cannot diff generated vs. expected SQL to validate a fix
- `measure_swap` — Genie is using wrong measure columns
- `missing_groupby_col` — GROUP BY doesn't match the question's requested dimensions

When `root_cause = unverifiable_no_expected_sql`:
- The system CAN identify the problem pattern (e.g. "extra HAVING clauses", "missing GROUP BY")
- The system CAN generate proposal instructions
- But it CANNOT validate that applying those instructions actually fixes the issue because there's no expected SQL to compare against
- This creates a deadlock: proposals pass grounding but can't be applied without verification

## Step 8: Cross-Run Comparison (Optional)

If comparing two runs on the same space, note:
- Whether the second run inherits enrichments from the first (check if DESCRIPTION_ENRICHMENT shows 0 eligible)
- Benchmark count differences (curated vs. synthetic)
- Whether baseline accuracy improved due to prior enrichments
- Whether the failing question set shrank (check `affected_questions` in AG stages)
- Different LLM models and their impact on baseline accuracy

## Output Format

Produce a narrative covering:
1. **Preflight**: What data was collected, how benchmarks were sourced/generated, any pruning
2. **Baseline Eval**: Accuracy, number of failures, whether thresholds were met
3. **Enrichment**: What was enriched vs. skipped, why; post-enrichment accuracy if evaluated
4. **Lever Loop**: Root cause classification, what was attempted, why it failed
5. **Finalization**: Repeatability score, what was promoted, terminal reason
6. **Empty Tables**: Which tables have no data and why that's expected given the run outcome
7. **Notable Observations**: Model used, duration, comparison with prior runs if applicable
