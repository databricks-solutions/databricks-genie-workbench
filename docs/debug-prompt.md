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
`genie_space_optimizer`. Below, `<S>` means `<LOG_CATALOG>.<LOG_SCHEMA>`.

## Non-negotiable rules

1. **Read-only.** Only `SHOW`, `DESCRIBE`, `SELECT`, and read-only CTEs. Never
   `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `ALTER`, `CREATE`, `DROP`, `OPTIMIZE`,
   `VACUUM`, grants, or Genie configuration APIs.
2. **Discover the schema before concluding.** Columns are added over time. Use
   only columns confirmed by `DESCRIBE TABLE`. If a column named below is
   missing, say `not available in this install` — never invent a value or
   substitute an unrelated field.
3. **Six current tables** are the debugging surface: `genie_opt_runs`,
   `genie_opt_stages`, `genie_opt_artifacts`, `genie_opt_iterations`,
   `genie_opt_patches`, `genie_opt_benchmark_mutations`. A missing table is
   evidence about the install, not permission to query a retired one.
4. **Four tasks, in order:** `intake_and_snapshot`, `benchmark_qc_and_repair`,
   `optimize`, `publish_and_audit`. Delta rows, not notebook-local state, are
   the durable truth.
5. **Cite every conclusion** as table + identifying row, e.g.
   `[genie_opt_iterations: iteration=2, eval_scope=full]`.
6. **Never print benchmark expected SQL or full config JSON by default.**
   Expected SQL is evaluation truth and must not reach instructions, examples,
   descriptions, or patches. Quote the minimum fragment only when it is the only
   way to explain a specific question-level mismatch.
7. **Separate facts from inference.** Label inferences `Hypothesis` and state
   what evidence would confirm them.

## Step 1 — Discover tables and columns

```sql
SHOW TABLES IN <S>
```

Then `DESCRIBE TABLE <S>.<t>` for each of the six tables in rule 3. Build an
availability map (table, present/missing, notable missing columns) and adapt
every projection below to what you actually find.

## Step 2 — Establish the run envelope

Read the run row. If none exists, stop and re-verify catalog, schema, and run
ID. If `<SPACE_ID>` was given and differs, flag the mismatch.

`config_snapshot` is excluded from the projection on purpose — it holds the full
Space config JSON, which rule 6 forbids printing. Only its presence and size are
selected.

```sql
SELECT * EXCEPT (config_snapshot),
       config_snapshot IS NOT NULL AS has_config_snapshot,
       LENGTH(config_snapshot) AS config_snapshot_chars
FROM <S>.genie_opt_runs WHERE run_id = '<RUN_ID>'
```

Report: `status`, `started_at`/`completed_at`, `space_id`, `job_id`/`job_run_id`,
run settings (`max_iterations`, `levers`, `apply_mode`, `llm_model`,
`max_benchmark_count`), `benchmarks_generated`, `best_iteration`,
`best_accuracy`, `convergence_reason`, and `triggered_by`. State whether the
trigger-time `config_snapshot` is present and its size — never print its body.

Check `run_kind` first (available on newer installs; if the column is missing,
treat the run as `optimization`). A `run_kind = 'mv_advice'` row with
`status = 'MV_ADVICE'` is **not** an optimization run: it is a born-terminal
sentinel for a standalone metric-view advice request (the IQ Scan "suggest"
surface, MV-D23). It runs no eval, so `max_iterations = 0`, `levers = []`, and
there are no stages, iterations, patches, or publish record to find. Report it
as an advice run, read its proposals from `genie_opt_mv_candidates`
(`run_id = '<RUN_ID>'`) if that table is present, and skip Steps 3–8 rather than
reporting the absent optimization ladder as a failure. Everything below assumes
`run_kind = 'optimization'`.

## Step 3 — Reconstruct the four-task timeline

```sql
SELECT task_key, stage, status, started_at, completed_at, duration_seconds,
       lever, iteration, detail_json, error_message
FROM <S>.genie_opt_stages WHERE run_id = '<RUN_ID>'
ORDER BY started_at, completed_at, stage
```

Identify the last completed stage; the first failure and its exact error; any
`STARTED` row with no completion; skipped or rolled-back work; and gaps that
dominate wall-clock time. Parse `detail_json` as JSON and describe only keys
actually present.

## Step 4 — Load the latest artifact per kind

Artifacts are append-only; a retry can write the same kind twice. Take the
latest per kind, but keep older revisions that explain a retry.

Inventory the artifacts first — `artifact_json` bodies can be large, so fetch
them per kind afterwards rather than all at once:

```sql
WITH ranked AS (
  SELECT artifact_id, stage_name, iteration, artifact_kind, content_hash,
         parent_artifact_id, source_notebook, created_at,
         LENGTH(artifact_json) AS json_chars,
         ROW_NUMBER() OVER (PARTITION BY artifact_kind
           ORDER BY created_at DESC, artifact_id DESC) AS recency
  FROM <S>.genie_opt_artifacts WHERE run_id = '<RUN_ID>'
)
SELECT * FROM ranked ORDER BY artifact_kind, recency
```

```sql
SELECT artifact_kind, artifact_json FROM <S>.genie_opt_artifacts
WHERE artifact_id = '<ARTIFACT_ID>'
```

Prioritize when present: `run_manifest` (intake parameters, snapshot hash,
handoff IDs); `benchmark_qc` (validity counts, finding codes, repair use,
semantic-review coverage, final window); `space_quality_enrichment` and
`space_metadata` (pre-loop metadata changes); `publish_record` (authoritative
publish outcome, champion, final status, concerns, audit summary). The
`wide_schema_*` kinds matter when the failure concerns asset discovery,
profiling, or column selection. Do not assume a kind exists.

## Step 5 — Build the evaluation and decision ladder

Read the ladder first without `rows_json`, which can be large and contains
question text and expected SQL:

```sql
SELECT * EXCEPT (rows_json) FROM <S>.genie_opt_iterations
WHERE run_id = '<RUN_ID>' ORDER BY iteration, eval_scope, timestamp
```

Then pull `rows_json` only for the iterations you actually need to explain:

```sql
SELECT iteration, eval_scope, rows_json FROM <S>.genie_opt_iterations
WHERE run_id = '<RUN_ID>' AND iteration IN (<ITERATIONS_OF_INTEREST>)
```

Read the ladder as follows:

- `iteration=0, eval_scope=full` is the baseline; later full rows are patch
  attempts. An enrichment row is supporting evidence, not a substitute for the
  full-evaluation ladder.
- `decision`, `decision_reason`, `current_hypothesis`, `reflection_json`,
  `do_not_repeat`, and `next_hypothesis` explain the controller's choices.
- `rolled_back=true` plus `rollback_reason` marks rejected candidates.
- `is_champion=true` identifies the champion; its `terminal_reason` is the
  authoritative stop reason. Compare it against the run row and
  `publish_record` — report disagreement as a state-consistency defect.
- Accuracy uses the persisted denominator. Do not recompute from
  `total_questions` when `evaluated_count` or `excluded_count` disagree.
- A transient eval status (`EVALUATION_TIMEOUT`, `EVALUATION_CANCELLED`) is
  retried up to twice before failing, and terminal stamping falls back to the
  best persisted iteration. Check for retries before calling an eval blip fatal.

Parse each non-null `rows_json` as a JSON array; inspect its keys before
aggregating, since native-evaluation payloads evolve. At minimum, count
`assessment` values per iteration (`GOOD`, `BAD`, `NEEDS_REVIEW`, unknown/null)
and summarize recurring `assessment_reasons`. Tie every claimed failure cluster
to question IDs. Do not print question text or expected SQL unless it is
essential to one root cause.

## Step 6 — Connect hypotheses to patches

```sql
SELECT iteration, lever, patch_index, patch_type, scope, risk_level,
       target_object, rolled_back, rollback_reason, proposal_id, cluster_id,
       provenance_json, applied_at, rolled_back_at
FROM <S>.genie_opt_patches WHERE run_id = '<RUN_ID>'
ORDER BY iteration, lever, patch_index
```

Parse `provenance_json` to link each patch to its hypothesis, failure cluster,
and question-level evidence. A patch row does **not** mean the Space improved —
confirm acceptance and accuracy movement in `genie_opt_iterations`. Per attempt,
report: hypothesis, patch types and targets, accuracy change vs. the previous
champion, decision, rollback status, and the evidence behind the decision.

## Step 7 — Explain benchmark QC and mutations

```sql
SELECT question_id, op, reason, logged_at,
       before IS NOT NULL AS has_before, after IS NOT NULL AS has_after,
       SHA2(COALESCE(before, ''), 256) AS before_hash,
       SHA2(COALESCE(after, ''), 256) AS after_hash
FROM <S>.genie_opt_benchmark_mutations WHERE run_id = '<RUN_ID>'
ORDER BY logged_at, question_id
```

Combine with the latest `benchmark_qc` artifact. Summarize counts and reason
codes for added, removed, changed, and advisory mutations, distinguishing:
question-quality or question-to-SQL semantic findings; SQL validation or
execution failure; semantic review not run or degraded; duplicate removal;
repair/regeneration exhausted; and corpus below the minimum or outside the
target window. `before`/`after` hold question and SQL text — report hashes, IDs,
op, and reason by default; expose content only for a specific RCA.

## Step 8 — Reconcile the terminal and publish outcome

Cross-check three sources: champion `genie_opt_iterations.terminal_reason`; the
latest `publish_record` artifact; and `genie_opt_runs.status` plus
`convergence_reason`. Expected mappings:

| Terminal reason | Run status | Published |
|---|---|---|
| `TARGET_REACHED` | `CONVERGED` | yes |
| `MAX_ATTEMPTS` | `MAX_ITERATIONS` | yes |
| `NO_NEW_HYPOTHESIS` | `STALLED` | no |
| `EVAL_BUDGET_EXHAUSTED` | `STALLED` | no |
| `EVAL_INVALID` | `FAILED` | no |
| `CONFIG_VALIDATION_FAILED` | `FAILED` | no |
| `LOOP_STATE_INVALID` | `FAILED` | no |
| `INSUFFICIENT_VALID_BENCHMARKS` | `SKIPPED` | no; Optimize never ran |
| missing or unknown | `STALLED` | no, fail closed |

A `status = 'MV_ADVICE'` run has no terminal reason and no publish record by
design (see Step 2) — it is an advice run, not a stalled or failed optimization.
Do not force it into the mapping above.

If the run stopped during Benchmark QC there may be no iteration or publish
record. Use the failed stage, `benchmark_qc`, the mutation ledger, and the run
row — do not manufacture an Optimize outcome.

## Step 9 — Produce the report

1. **Executive diagnosis** — one paragraph: outcome, failing task or limiting
   factor, champion/publish state, confidence.
2. **Four-task story** — one short subsection per task; say `not reached` or
   `no durable evidence` where true.
3. **Attempt ladder** — table: iteration/scope, accuracy, assessment counts,
   hypothesis, patch summary, decision, rollback, champion marker.
4. **Primary root cause** — facts first, then labeled hypotheses. Explain why
   the controller accepted, rejected, retried, or stopped.
5. **State consistency checks** — terminal mapping, champion pointer, artifact
   recency, missing schema fields, contradictions.
6. **Recommended next actions** — ordered, specific, non-destructive. Separate
   benchmark fixes, Space metadata/instruction fixes, permission or eval
   infrastructure fixes, and code defects.
7. **Evidence appendix** — every cited row and identifier, plus query
   limitations and missing data.

Stitch the rows into a chronological causal story rather than dumping them.
Never claim a root cause from a status string alone when iteration, artifact,
patch, or question-level evidence can confirm or refute it.

---

For recurring investigations, save the completed report in the workspace next
to the Job run link, but do not persist raw expected SQL or full Space snapshots.
