# Genie Space Optimizer Run Debugger

## Purpose

Use this prompt to reconstruct a Genie Space Optimizer run from the live Delta logs. The current job is a 4-task DAG:
`intake_and_snapshot`, `benchmark_qc_and_repair`, `optimize`, and `publish_and_audit`.
The main sources of truth live under `<LOG_CATALOG>.<LOG_SCHEMA>`.

| Task | Primary tables | What to look for |
|---|---|---|
| `INTAKE_AND_SNAPSHOT` | `genie_opt_runs`, `genie_opt_artifacts` (`run_manifest`), `genie_opt_stages` | original snapshot, job/run metadata, warehouse id, config contract |
| `BENCHMARK_QC_AND_REPAIR` | `genie_opt_artifacts` (`benchmark_qc`), `genie_opt_benchmark_mutations`, `genie_opt_stages` | benchmark validation, repair/prune attempts, live benchmark push |
| `OPTIMIZE` | `genie_opt_iterations`, `genie_opt_patches`, `genie_eval_lever_loop_decisions`, `genie_opt_provenance`, `genie_opt_artifacts` (`space_quality_enrichment`), `genie_opt_stages` | baseline / enrichment evals, lever-loop decisions, root cause, description metadata, terminal reason |
| `PUBLISH_AND_AUDIT` | `genie_opt_artifacts` (`publish_record`), `genie_opt_runs`, `genie_opt_stages` | publish decision, audit summary, final status, concerns |

Older 6-notebook tables are retired. Only consult them for legacy installs.

## Inputs Required

This prompt ships with no workspace, space, or run baked in. Supply your own values for the placeholders below and substitute every occurrence into the SQL queries that follow.

- **`<WORKSPACE_ID>`**: Databricks workspace id. Use this when opening workspace links (`?o=<WORKSPACE_ID>`).
- **`<WORKSPACE_HOST>`**: optional Databricks workspace host, e.g. `https://your-workspace.cloud.databricks.com`, if you want API/UI links.
- **`<SPACE_ID>`**: the Genie Agent that was optimized.
- **`<RUN_ID>`**: the optimizer run to investigate.
- **`<LOG_CATALOG>`**: Unity Catalog catalog that holds the GSO log schema.
- **`<LOG_SCHEMA>`**: schema that holds the GSO log tables. Do not assume it is named `genie_space_optimizer`.

All SQL below should be fully qualified as `<LOG_CATALOG>.<LOG_SCHEMA>.<table>`.

## Step 0: Optional Consolidated View

If the Workbench API is available, fetch `GET /api/auto-optimize/runs/{run_id}` first. It already stitches together the current four-step view, scores, levers, and workspace links. If you only have SQL access, start with the queries below.

## Step 1: Get Run Metadata

Query the runs table to understand the run's overall configuration and outcome.

```sql
SELECT
  run_id,
  space_id,
  domain,
  catalog,
  uc_schema,
  status,
  started_at,
  completed_at,
  job_run_id,
  job_id,
  max_iterations,
  levers,
  apply_mode,
  llm_model,
  deploy_target,
  benchmarks_generated,
  best_iteration,
  best_accuracy,
  best_repeatability,
  convergence_reason,
  config_snapshot,
  triggered_by,
  warehouse_id,
  human_corrections_json,
  max_benchmark_count,
  updated_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_runs
WHERE space_id = '<SPACE_ID>'
  AND run_id = '<RUN_ID>'
```

**What to extract:**
- `status` - terminal status.
- `started_at` / `completed_at` - total wall-clock duration.
- `catalog` / `uc_schema` - the actual log location for this run.
- `job_run_id` / `job_id` - Databricks job identifiers.
- `warehouse_id` - the warehouse resolved at preflight.
- `levers`, `apply_mode`, `llm_model`, `deploy_target` - run configuration.
- `benchmarks_generated`, `max_iterations`, `max_benchmark_count` - capacity and benchmark behavior.
- `best_iteration`, `best_accuracy`, `best_repeatability` - the final champion state.
- `convergence_reason` - the final stop reason recorded on the run row.
- `config_snapshot` - the rollback anchor captured at trigger time.
- `human_corrections_json` - carry-forward human feedback, if any.

If `config_snapshot` is missing, the run violated the trigger-time snapshot contract. Baseline revert and discard fail closed because there is no trustworthy rollback anchor.

## Step 2: Get The Current Task Timeline

Query the stage timeline and the task-level artifact blobs.

```sql
SELECT
  task_key,
  stage,
  status,
  started_at,
  completed_at,
  duration_seconds,
  lever,
  iteration,
  detail_json,
  error_message
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_stages
WHERE run_id = '<RUN_ID>'
ORDER BY started_at
```

```sql
SELECT
  artifact_kind,
  stage_name,
  iteration,
  artifact_json,
  content_hash,
  source_notebook,
  created_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_artifacts
WHERE run_id = '<RUN_ID>'
ORDER BY created_at
```

**Reasoning:** `genie_opt_stages` is the chronological timeline, while `genie_opt_artifacts` stores the run manifest, benchmark QC payload, post-enrichment Space description metadata, and publish record.

**Current labels to expect:**
- Top-level task labels: `INTAKE_AND_SNAPSHOT`, `BENCHMARK_QC_AND_REPAIR`, `OPTIMIZE`, `PUBLISH_AND_AUDIT`.
- Common nested stage labels: `PREFLIGHT_STARTED`, `PREFLIGHT_METADATA_COLLECTION`, `DATA_PROFILING`, `GENIE_BENCHMARK_EXTRACTION`, `BENCHMARK_GENERATION`, `PREFLIGHT_SEMANTIC_ALIGNMENT`, `PREFLIGHT_PREDICATE_VALIDATION`, `PREFLIGHT_GT_EXECUTION_CHECK`, `PREFLIGHT_BENCHMARK_PUSH`, `PREFLIGHT_BENCHMARK_WINDOW`, `BASELINE_EVAL_STARTED`, `DESCRIPTION_ENRICHMENT`, `JOIN_DISCOVERY`, `SPACE_METADATA_ENRICHMENT`, `SQL_EXPRESSION_SEEDING`, `LEVER_LOOP_STARTED`, `PUBLISH_AND_AUDIT`.
- The post-enrichment evaluation is usually represented by `genie_opt_iterations` with `eval_scope = 'enrichment'`, not by a separate wrapper stage.

Do not rely on older wrapper labels such as `ENRICHMENT_STARTED`, `PROMPT_MATCHING_SETUP`, `FINALIZE_STARTED`, `FINALIZE_TERMINAL`, or `AG_AG1_*`. They are legacy noise, not the primary debugging surface for current runs.

### Key Fields in `detail_json` to Parse

| Stage | Key Fields |
|-------|-----------|
| `INTAKE_AND_SNAPSHOT` | `run_id`, `space_id`, `catalog`, `schema`, `baseline_config_hash`, `warehouse_id` |
| `BENCHMARK_QC_AND_REPAIR` | `valid_count`, `repair_tries_used`, `repair_max_tries`, `window_status`, `final_validity`, `terminal_reason` |
| `PREFLIGHT_METADATA_COLLECTION` | `columns_collected`, `tags_collected`, `routines_collected` |
| `DATA_PROFILING` | `tables_profiled`, `columns_profiled`, `low_cardinality_columns`, `metric_view_profile_outcomes` |
| `GENIE_BENCHMARK_EXTRACTION` | `genie_space_benchmarks`, `with_sql`, `question_only` |
| `BENCHMARK_GENERATION` | `total_count`, `curated_count`, `synthetic_count`, `auto_corrected_count`, `valid_count` |
| `PREFLIGHT_SEMANTIC_ALIGNMENT` | `checked`, `misaligned`, `remaining` |
| `PREFLIGHT_PREDICATE_VALIDATION` | `checked`, `mismatched`, `auto_corrected`, `remaining` |
| `PREFLIGHT_GT_EXECUTION_CHECK` | `checked`, `empty_results`, `remaining` |
| `PREFLIGHT_BENCHMARK_PUSH` | `added`, `dedup_skipped`, `merged_total`, `existing_count`, `window_status` |
| `PREFLIGHT_BENCHMARK_WINDOW` | `count`, `window`, `status` |
| `BASELINE_EVAL_STARTED` | `overall_accuracy`, `both_correct_rate`, `thresholds_met` |
| `DESCRIPTION_ENRICHMENT` | `total_eligible`, `total_patches_generated`, `total_enriched`, `total_skipped`, `total_failed_llm` |
| `JOIN_DISCOVERY` | `existing_specs`, `candidates_found`, `total_applied` |
| `SPACE_METADATA_ENRICHMENT` | `description_generated`, `questions_count` |
| `SQL_EXPRESSION_SEEDING` | `total_candidates`, `total_seeded`, `total_rejected` |
| `LEVER_LOOP_STARTED` | `levers_attempted`, `levers_accepted`, `reflection_buffer`, `terminal_reason` |
| `PUBLISH_AND_AUDIT` | `terminal_reason`, `final_status`, `published`, `publish_outcome`, `champion_iteration`, `champion_accuracy`, `audit_summary_generated`, `concerns` |

## Step 3: Get Iterations (Eval Results)

```sql
SELECT
  iteration,
  attempt_no,
  attempt_mode,
  eval_scope,
  overall_accuracy,
  both_correct_rate,
  total_questions,
  correct_count,
  evaluated_count,
  excluded_count,
  thresholds_met,
  is_champion,
  terminal_reason,
  decision,
  decision_reason,
  repeatability_pct,
  best_accuracy,
  best_config_version_id,
  current_hypothesis,
  do_not_repeat,
  next_hypothesis,
  surgical_attempts_used,
  target_accuracy,
  max_attempts,
  failures_json,
  remaining_failures,
  num_needs_review,
  eval_run_id,
  eval_run_status,
  config_json
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_iterations
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, eval_scope
```

**Reasoning:**
- `iteration = 0` and `eval_scope = 'full'` is the baseline eval.
- `iteration = 0` and `eval_scope = 'enrichment'` is the post-enrichment eval when it exists.
- Higher iterations are lever-loop eval results.
- `is_champion = true` marks the promoted row.
- `terminal_reason` on the champion row is authoritative and is copied to `genie_opt_runs.convergence_reason` by publish.
- `config_json` is the full effective Genie Agent config for that attempt.
- `best_config_version_id`, `current_hypothesis`, `do_not_repeat`, and `next_hypothesis` are the loop-state breadcrumbs that explain why the run kept going or stopped.
- `eval_run_id` / `eval_run_status` appear when the native eval runner was used.

## Step 4: Trace Decisions and Provenance

```sql
SELECT
  iteration,
  decision_order,
  stage_letter,
  gate_name,
  decision,
  reason_code,
  reason_detail,
  affected_qids_json,
  source_cluster_ids_json,
  proposal_ids_json,
  proposal_to_patch_map_json,
  metrics_json,
  created_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_eval_lever_loop_decisions
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, decision_order
```

```sql
SELECT
  iteration,
  lever,
  question_id,
  cluster_id,
  proposal_id,
  patch_type,
  gate_type,
  gate_result,
  resolved_root_cause,
  resolution_method,
  judge,
  judge_verdict,
  arbiter_verdict,
  wrong_clause,
  counterfactual_fix,
  rationale_snippet,
  logged_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_provenance
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, lever, question_id
```

**Reasoning:** `genie_eval_lever_loop_decisions` is the gate audit trail. `genie_opt_provenance` is the end-to-end trace from judge verdicts to patches and gate outcomes. If you need the exact patch linkage, join through `proposal_ids_json` or `proposal_to_patch_map_json`.

## Step 5: Inspect Patches and Benchmark Changes

```sql
SELECT
  iteration,
  lever,
  patch_index,
  patch_type,
  applied_patch_type,
  scope,
  risk_level,
  target_object,
  applied,
  rolled_back,
  rollback_reason,
  proposal_id,
  cluster_id,
  provenance_json,
  applied_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_patches
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, lever, patch_index
```

```sql
SELECT
  question_id,
  op,
  reason,
  before,
  after,
  logged_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_benchmark_mutations
WHERE run_id = '<RUN_ID>'
ORDER BY logged_at
```

**Reasoning:** `genie_opt_patches` shows the patch lifecycle, including applied vs. rolled back patches. `genie_opt_benchmark_mutations` shows the live-space benchmark changes from benchmark QC and repair.

## Step 6: Legacy Tables Only If Needed

If you are debugging an older install, you may still see retired tables such as:
- `genie_eval_asi_results`
- `genie_eval_human_required`
- `genie_eval_proactive_corpus_profile`
- `genie_eval_proactive_patches`
- `genie_eval_gt_correction_candidates`
- `genie_eval_question_regressions`
- `genie_opt_finalize_attestation_matrix`
- `genie_opt_suggestions`
- `genie_opt_data_access_grants`

Fresh installs do not create these tables. Do not treat them as the primary source of truth for current runs.

## Step 7: Interpret the Failure Pattern

When the run stalls or no patch is applied, inspect:

1. `terminal_reason` on the champion `genie_opt_iterations` row.
2. `reason_code` and `decision` in `genie_eval_lever_loop_decisions`.
3. `resolved_root_cause`, `resolution_method`, `counterfactual_fix`, `wrong_clause`, and `rationale_snippet` in `genie_opt_provenance`.
4. `rollback_reason` and `applied` / `rolled_back` in `genie_opt_patches`.

Common root causes and their meaning:
- `unverifiable_no_expected_sql` - the benchmarks do not have a gold SQL answer, so the optimizer cannot prove that a candidate fix is correct.
- `measure_swap` - Genie is using the wrong measure columns.
- `missing_groupby_col` - the GROUP BY does not match the question's requested dimensions.

When `resolved_root_cause = unverifiable_no_expected_sql`:
- The system can identify the problem pattern.
- The system can generate proposal instructions.
- It cannot validate that applying those instructions actually fixes the issue because there is no expected SQL to compare against.
- That can leave the run with grounded proposals that still fail to apply or get rolled back.

If you need the published summary, inspect the `publish_record` artifact in `genie_opt_artifacts` rather than looking for a `FINALIZE_TERMINAL` stage. The current publish task is `PUBLISH_AND_AUDIT`.

## Step 8: Cross-Run Comparison (Optional)

If comparing two runs on the same space, note:
- Whether the second run reused the same `config_snapshot` and benchmark mutation history.
- Benchmark count differences and the `genie_opt_benchmark_mutations` deltas.
- Whether baseline accuracy improved, using the `iteration = 0`, `eval_scope = 'full'` row in `genie_opt_iterations`.
- Whether the failing cluster or provenance set shrank, using `genie_opt_provenance` and `genie_eval_lever_loop_decisions`.
- Different `llm_model` values and their impact on baseline and optimized scores.

## Output Format

Produce a narrative covering:
1. **Intake and Snapshot**: What was captured at trigger time, plus the run manifest and rollback anchor.
2. **Benchmark QC and Repair**: Benchmark validity, repair attempts, and what changed in the live space.
3. **Optimize**: Baseline accuracy, enrichment, lever-loop decisions, provenance, and terminal reason.
4. **Publish and Audit**: Publish outcome, audit summary, final status, and concerns.
5. **Legacy Tables**: Only if the run predates the current 4-task DAG.
6. **Notable Observations**: Model used, duration, workspace context, and comparison with prior runs if applicable.
