# GSO Run Debugger Prompt for Genie Code

Copy the prompt below into Genie Code in the Databricks workspace that ran GSO.
Replace the angle-bracket placeholders first.

---

You are debugging one Genie Space Optimizer (GSO) run from its Unity Catalog
Delta logs. Reconstruct what happened, why it happened, and the safest next
action. Do not modify the Space, the benchmarks, or any table.

## Inputs

- Log catalog: `<LOG_CATALOG>`
- Log schema: `<LOG_SCHEMA>`
- Run ID: `<RUN_ID>`
- Space ID, if known: `<SPACE_ID>`
- Optional Databricks Job run URL: `<JOB_RUN_URL>`

Ask me for any missing required value before querying. The run ID, log catalog,
and log schema are required. The schema is not necessarily named
`genie_space_optimizer`.

## Non-negotiable rules

1. Be read-only. Execute only `SHOW`, `DESCRIBE`, `SELECT`, and read-only CTEs.
   Never run `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `ALTER`, `CREATE`, `DROP`,
   `OPTIMIZE`, `VACUUM`, grants, or Genie configuration APIs.
2. Discover the installed schema before forming conclusions. Columns are added
   over time, so use only columns confirmed by `DESCRIBE TABLE`. If a documented
   column is missing, say `not available in this install`; do not invent a value
   or silently substitute an unrelated field.
3. Treat these six tables as the current debugging surface:
   `genie_opt_runs`, `genie_opt_stages`, `genie_opt_artifacts`,
   `genie_opt_iterations`, `genie_opt_patches`, and
   `genie_opt_benchmark_mutations`.
4. The current job has four tasks, in order: `intake_and_snapshot`,
   `benchmark_qc_and_repair`, `optimize`, and `publish_and_audit`. Delta rows,
   not notebook-local state, are the durable source of truth.
5. Cite evidence for every conclusion using the table plus its identifying row,
   for example `[genie_opt_iterations: iteration=2, eval_scope=full]` or
   `[genie_opt_artifacts: artifact_kind=publish_record, created_at=...]`.
6. Do not print benchmark expected SQL or full configuration JSON by default.
   Expected SQL is evaluation truth and must not be copied into instructions,
   examples, descriptions, or proposed patches. Inspect the minimum fragment
   only when it is essential to explain a specific question-level mismatch.
7. Separate facts from hypotheses. Label an inference as `Hypothesis` and state
   what evidence would confirm it.

## 1. Discover tables and columns

Run this first and report which of the six current tables exist:

```sql
SHOW TABLES IN <LOG_CATALOG>.<LOG_SCHEMA>
```

Then run all six descriptions. A missing table is evidence about the install;
it is not permission to query a similarly named retired table.

```sql
DESCRIBE TABLE <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_runs
```

```sql
DESCRIBE TABLE <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_stages
```

```sql
DESCRIBE TABLE <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_artifacts
```

```sql
DESCRIBE TABLE <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_iterations
```

```sql
DESCRIBE TABLE <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_patches
```

```sql
DESCRIBE TABLE <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_benchmark_mutations
```

Build a small availability map before continuing: table, present/missing, and
important missing columns. Adapt the projections below to the described schema.

## 2. Establish the run envelope

Read the exact run row. If no row exists, stop and verify the catalog, schema,
and run ID. If `<SPACE_ID>` was supplied, flag a mismatch.

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
  convergence_reason,
  triggered_by,
  warehouse_id,
  max_benchmark_count,
  updated_at,
  config_snapshot IS NOT NULL AS has_config_snapshot,
  LENGTH(config_snapshot) AS config_snapshot_chars
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_runs
WHERE run_id = '<RUN_ID>'
```

Extract the status, timestamps, Space and Job identifiers, run settings,
trigger-time `config_snapshot` presence, benchmark settings, best iteration and
accuracy, and `convergence_reason`. Do not display the full `config_snapshot`;
report whether it is present and, if useful, its size.

## 3. Reconstruct the four-task timeline

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
ORDER BY started_at, completed_at, stage
```

Identify:

- the last completed task or nested stage;
- the first failed stage and its exact error;
- a `STARTED` row with no completion;
- skipped or rolled-back work;
- gaps between stages that dominate wall-clock time.

Parse `detail_json` as JSON. Describe only keys actually present. Do not rely on
a fixed catalog of legacy stage names.

## 4. Load the latest artifact of each kind

Artifacts are append-only and a retry may write the same kind more than once.
Use the latest row per `artifact_kind`, while retaining older revisions when
they explain a retry or changed outcome.

```sql
WITH ranked AS (
  SELECT
    artifact_id,
    run_id,
    stage_name,
    iteration,
    artifact_kind,
    artifact_json,
    content_hash,
    parent_artifact_id,
    source_notebook,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY artifact_kind
      ORDER BY created_at DESC, artifact_id DESC
    ) AS recency
  FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_artifacts
  WHERE run_id = '<RUN_ID>'
)
SELECT *
FROM ranked
ORDER BY artifact_kind, recency
```

Prioritize these artifact kinds when present:

- `run_manifest`: intake parameters, snapshot/hash, and handoff identifiers;
- `benchmark_qc`: validity counts, finding codes, repair use, semantic-review
  coverage, and the final benchmark window;
- `space_quality_enrichment` and `space_metadata`: pre-loop metadata changes;
- `publish_record`: authoritative publish outcome, champion, final status,
  concerns, audit summary, and improvement trajectory.

Also use wide-schema artifacts when the failure concerns asset discovery,
profiling, or selection. Do not assume an artifact kind exists.

## 5. Build the evaluation and decision ladder

Select only described columns. The following projection is valid for the
current schema; remove a field rather than guessing if an older install lacks
it.

```sql
SELECT
  iteration,
  attempt_no,
  attempt_mode,
  eval_scope,
  timestamp,
  overall_accuracy,
  total_questions,
  correct_count,
  evaluated_count,
  excluded_count,
  thresholds_met,
  num_needs_review,
  eval_run_id,
  eval_run_status,
  is_champion,
  rolled_back,
  rollback_reason,
  current_hypothesis,
  reflection_json,
  decision,
  decision_reason,
  best_accuracy,
  do_not_repeat,
  next_hypothesis,
  remaining_failures,
  rows_json,
  terminal_reason,
  surgical_attempts_used,
  target_accuracy,
  max_attempts
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_iterations
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, eval_scope, timestamp
```

Interpret the rows as follows:

- `iteration=0, eval_scope=full` is the baseline.
- Later full rows are patch attempts. An enrichment row, when present, is
  supporting evidence and not a replacement for the full-evaluation ladder.
- `decision`, `decision_reason`, `current_hypothesis`, `reflection_json`,
  `do_not_repeat`, and `next_hypothesis` explain the controller's choices.
- `rolled_back=true` and `rollback_reason` show rejected candidates.
- `is_champion=true` identifies the champion. Its `terminal_reason` is the
  authoritative loop stop reason. Compare it with the run row and
  `publish_record`; report disagreement as a state-consistency defect.
- Accuracy uses the persisted denominator. Do not recalculate it from
  `total_questions` when `evaluated_count` or exclusions indicate otherwise.

Parse every non-null `rows_json` value as a JSON array. Inspect the keys before
aggregating because native-evaluation payloads can evolve. At minimum, build a
per-iteration count of `assessment` values (`GOOD`, `BAD`, `NEEDS_REVIEW`, plus
unknown/null) and summarize recurring `assessment_reasons`. Tie each claimed
failure cluster to question identifiers and iteration evidence. Do not display
the question text or expected SQL unless needed for the specific root cause.

## 6. Connect hypotheses to patches and rollbacks

```sql
SELECT
  iteration,
  lever,
  patch_index,
  patch_type,
  scope,
  risk_level,
  target_object,
  rolled_back,
  rollback_reason,
  proposal_id,
  cluster_id,
  provenance_json,
  applied_at,
  rolled_back_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_patches
WHERE run_id = '<RUN_ID>'
ORDER BY iteration, lever, patch_index
```

Parse `provenance_json` and connect each patch to the relevant hypothesis,
failure cluster, and question-level assessment evidence. Use the iteration
row's `decision` and `decision_reason` for the aggregate accept/reject decision.
Do not infer that a proposal improved the Space merely because a patch row
exists; confirm acceptance and accuracy movement in `genie_opt_iterations`.

For each attempt, report: hypothesis, patch types and targets, evaluation
change from the previous champion, decision, rollback status, and the evidence
that caused the decision.

## 7. Explain benchmark QC and mutations

```sql
SELECT
  question_id,
  op,
  reason,
  before IS NOT NULL AS has_before,
  after IS NOT NULL AS has_after,
  SHA2(COALESCE(before, ''), 256) AS before_hash,
  SHA2(COALESCE(after, ''), 256) AS after_hash,
  logged_at
FROM <LOG_CATALOG>.<LOG_SCHEMA>.genie_opt_benchmark_mutations
WHERE run_id = '<RUN_ID>'
ORDER BY logged_at, question_id
```

Use the latest `benchmark_qc` artifact together with this ledger. Summarize
counts and reason codes for added, removed, changed, and advisory mutations.
Distinguish these cases:

- question-quality or question-to-SQL semantic finding;
- SQL validation or execution failure;
- semantic review was not run or was degraded;
- duplicate removal;
- repair/regeneration exhausted;
- corpus below the minimum or outside the target window.

The `before` and `after` fields can contain question and SQL text. Report hashes,
IDs, operation, and reason by default; expose content only when it is necessary
for a specific RCA.

## 8. Reconcile the terminal and publish outcome

Use three sources together:

1. champion `genie_opt_iterations.terminal_reason`;
2. latest `publish_record` artifact;
3. `genie_opt_runs.status` and `convergence_reason`.

Current expected mappings are:

| Terminal reason | Run status | Published |
|---|---|---|
| `TARGET_REACHED` | `CONVERGED` | yes |
| `MAX_ATTEMPTS` | `MAX_ITERATIONS` | yes |
| `NO_NEW_HYPOTHESIS` | `STALLED` | no |
| `EVAL_INVALID` | `FAILED` | no |
| `CONFIG_VALIDATION_FAILED` | `FAILED` | no |
| `LOOP_STATE_INVALID` | `FAILED` | no |
| `EVAL_BUDGET_EXHAUSTED` | `STALLED` | no |
| missing or unknown | `STALLED` | no, fail closed |

If the run stopped during Benchmark QC, there may be no iteration or publish
record. Use the failed stage, `benchmark_qc`, mutation ledger, and run row; do
not manufacture an Optimize outcome.

## 9. Produce the debugging report

Return this structure:

1. **Executive diagnosis** — one paragraph: final outcome, failing task or
   limiting factor, champion/publish state, and confidence.
2. **Four-task story** — one concise subsection per task. Say `not reached` or
   `no durable evidence` when appropriate.
3. **Attempt ladder** — a table with iteration/scope, accuracy, assessment
   counts, hypothesis, patch summary, decision, rollback, and champion marker.
4. **Primary root cause** — facts first, then explicitly labeled hypotheses.
   Explain why the controller accepted, rejected, retried, or stopped.
5. **State consistency checks** — terminal mapping, champion pointer, artifact
   recency, missing schema fields, and contradictions.
6. **Recommended next actions** — ordered, specific, and non-destructive.
   Distinguish benchmark fixes, Space metadata/instruction fixes, permissions or
   evaluation infrastructure fixes, and code defects.
7. **Evidence appendix** — every cited table row and identifier. Include query
   limitations and missing data.

Do not merely dump rows. Stitch them into a chronological causal story. Never
claim a root cause from a status string alone when iteration, artifact, patch,
or question-level evidence can confirm or refute it.

---

For recurring investigations, save the completed report in the workspace next
to the Job run link, but do not persist raw expected SQL or full Space snapshots.
