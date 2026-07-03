# GSO Simplification Plan

**Status:** Structural loop rewrite implemented on `gso/optimizer-v2`
**Scope:** `packages/genie-space-optimizer/` — dead code, duplicated state, and over-specified contracts surfaced during code review.

**Implemented before the structural cutover:** Changes A/B, I, O, P, plus the
benchmark `asset_fingerprint` loader prerequisite for Change F.

**Implemented in the structural cutover:** Changes J/K/L/M/N, with the user's
confirmed decisions:

- Task 02 was removed and folded into the optimize task.
- Job task keys and notebook filenames are unnumbered:
  `intake_and_snapshot -> benchmark_qc_and_repair -> optimize -> publish_and_audit`.
- The active optimize path is `optimization/unified_loop.py`; it starts with
  baseline iteration 0, then repeats `LLM proposes patch -> apply -> native
  full benchmark eval -> accept or rollback`.
- The native Genie Benchmark API is the only active evaluation path. There is
  no legacy eval fallback in the job path.
- Slice/P0 gates and the coverage/enrichment phase are removed from the active
  path. Enrichment is just another LLM-proposed patch in the unified loop.
- No `GSO_UNIFIED_LOOP` flag was added; the new workflow is the only active
  job workflow.

Historical findings below still reference the old numbered files where they
describe the pre-cutover state. The Proposed Changes and Recommended
Sequencing sections record which items have now landed.

## Context

This is a living document. Each section captures a specific simplification
target: what's dead/duplicated, the evidence, the proposed change, and the
risk. New findings get appended as new sections. Changes are sequenced in the
Recommended Sequencing section at the bottom.

---

## Finding 1 — `space_snapshot` artifact is write-only

**Where:** `jobs/run_00_intake_and_snapshot.py`, `optimization/state.py`

**Evidence**

- `write_artifact(spark, run_id, "space_snapshot", …)` is called exactly once,
  in Task 00.
- `load_artifacts` (the reader for `genie_opt_artifacts`) has **no runtime
  callers**. The only other `download_artifacts` references are in
  `tools/evidence_bundle.py` / `tools/_mlflow_runner.py` and concern MLflow
  artifact downloads — a different concept.
- The discard path (`integration/discard.py`) reads
  `genie_opt_runs.config_snapshot` (the run-row **column**), not the artifact:
  ```python
  run_data = wh_load_run(ws, config.warehouse_id, run_id, ...)
  original_snapshot = run_data.get("config_snapshot")  # run-row column
  ```
- The lever loop (`_run_lever_loop`) seeds its working `metadata_snapshot`
  from a **live** `fetch_space_config(w, space_id)` each iteration — not from
  the artifact.
- The gates path receives `space_snapshot={}` (an empty placeholder).

**Conclusion:** the `space_snapshot` artifact is currently a write-only audit
record. No code path reads `serialized_space`, `config`, or `captured_at`
back from it.

## Finding 2 — `serialized_space` vs `config` distinction is academic

**Where:** `jobs/run_00_intake_and_snapshot.py` (the `space_snapshot` payload)

The payload carries both:

- `serialized_space` — raw export.
- `config` — parsed/enriched version with `_parsed_space`, `_tables`,
  `_metric_views`, `_prefetched_uc_metadata` projections.

Since nothing reads either field back (see Finding 1), the distinction is
unused. The `config_hash` field is also write-only — every reference in the
codebase is a write site in `run_00_intake_and_snapshot.py`; there are zero
readers. (Task 01's benchmark-reuse gate 2 is intended to compare
`asset_fingerprint` read from benchmark *rows* in `genie_benchmarks_<domain>`,
not `config_hash` from any artifact. See Finding 8 for the current loader bug:
persisted rows carry the fingerprint in `expectations`, but the loader does not
copy it back into the benchmark dict, so the gate is currently ineffective on
loaded rows.)

## Finding 3 — `data_access_grants: []` is a dead vestige

**Where:** `jobs/run_00_intake_and_snapshot.py`

- Inline comment admits it: `# folded here (no runtime grant writer in v2)`.
- `ddl.py` lists `genie_opt_data_access_grants` in `RETIRED_TABLES`; the table
  is renamed to `*_deprecated` on existing installs and never created on new
  ones.
- A grep for `data_access_grants` across the package returns only the DDL
  constant, the retired-table DDL, and this single write site. **No reader.**

## Finding 4 — `preflight_fetch_config` API fallback is dead on the trigger path

**Where:** `optimization/preflight.py`, `integration/trigger.py`

`_capture_config_snapshot` in `integration/trigger.py` hard-raises if both
the SP and OBO fetch fail:

```python
if not space_snapshot:
    raise RuntimeError(f"Cannot export non-empty Genie Space config for {space_id}. ...")
```

So for any run launched through `trigger_optimization`, `config_snapshot` is
guaranteed to be non-null on the run row before the job starts. The
`fetch_space_config` fallback inside `preflight_fetch_config` only fires for
out-of-band run-row inserts (manual/test) or pre-v2 rows that predate the
column. `preflight_fetch_config` is also called by both Task 00 and Task 01, so
the fallback is not scoped to the intake task alone. It's defensive code, not a
normal path — but it's also cheap to keep, so this finding is informational, not
a removal target.

## Finding 5 — `run_manifest` duplicates run-row columns

**Where:** `jobs/run_00_intake_and_snapshot.py` (the `run_manifest` payload)

Several `run_manifest` fields (`run_id`, `space_id`, `domain`, `catalog`,
`schema`, `apply_mode`, `levers`, `warehouse_id`, `triggered_by`) already
exist as typed columns on `genie_opt_runs`. The manifest is a
human-readable, auditable blob of the *complete* parameter envelope, including
values without dedicated columns (`benchmark_target`, `benchmark_max`,
`max_iterations`, `benchmark_repair_max_tries`, `target_accuracy`,
`max_attempts`). It does have a real consumer — the backend reads
`target_accuracy` and `max_attempts` from it via
`_load_latest_artifact(run_id, "run_manifest")` in
`backend/routers/auto_optimize.py` — so it should be kept. (The
`baseline_config_hash` field within it, like `config_hash` on `space_snapshot`,
is write-only — see Finding 2.) The duplication of run-row columns is worth
noting but is intentional audit redundancy.

## Finding 6 — UC metadata duplicates `serialized_space` and the merge is expensive

**Where:** `optimization/preflight.py` (`preflight_collect_uc_metadata`),
`common/uc_metadata.py`

`serialized_space` already carries what the space *author configured*:
table names (`data_sources.tables[].identifier`), column names
(`column_configs[].column_name`), inlined column comments, and function
declarations (`sql_functions[]`). `preflight_collect_uc_metadata` re-fetches
the same information from Unity Catalog via a three-tier fallback
(prefetched → REST → Spark) and then spends effort reconciling the two.

UC metadata adds four things the space config genuinely lacks:

- **Column data types** — not in `column_configs`.
- **Table tags** — not in `serialized_space`.
- **Foreign keys** — read from `information_schema.key_column_usage`.
- **Ground-truth routine signatures** — return types, params.

So the fetch is justified, but the **reconciliation/merge** is
over-engineered. The lever loop mutates `column_configs` to apply patches,
which is why the code keeps the inlined comments alongside UC comments —
but UC is the source of truth for what actually exists, and the inlined
comments can drift.

**Conclusion:** the fetch should stay; the merge logic is a candidate for
simplification. A future design could treat UC as the single source of truth
for column metadata and drop the inlined `column_configs.comment` from the
working config (applying patches only to the live Genie Space, not the
in-memory mirror).

## Finding 7 — Benchmarks have two load sources and the reuse path is opaque

**Where:** `optimization/preflight.py` (`_load_or_generate_benchmarks`),
`optimization/evaluation.py` (`extract_genie_space_benchmarks`,
`generate_benchmarks`)

`_load_or_generate_benchmarks` reads benchmarks from two sources:

1. **`serialized_space`** — via `extract_genie_space_benchmarks`, which pulls
   `benchmarks.questions[].answer[]` (user-authored, with SQL) and
   `config.sample_questions[]` (question-only, no SQL). This is the *seed* set.
2. **`genie_benchmarks_<domain>` Delta table** — via
   `load_benchmarks_from_dataset`, which reads benchmarks persisted by a
   *prior run* of the same space. This is the *bulk* of the 30–40 target.

The dual-source design is intentional: most spaces ship 0–5 user-authored
benchmarks, so the 30–40 target is met by LLM synthesis persisted across
runs. Without the Delta table, every run would regenerate from scratch and
burn tokens re-deriving the same benchmarks.

**Conclusion:** the dual sources are justified, but the reuse-vs-regenerate
decision logic (see Finding 8) is the real problem. The two-source split
itself is fine.

## Finding 8 — Benchmark reuse/regenerate cascade is over-complicated; an LLM triage could collapse several gates

**Where:** `optimization/preflight.py` (`_load_or_generate_benchmarks`)

The reuse path is a cascade of five gates:

1. ≥5 valid existing benchmarks? (count gate)
2. Schema fingerprint matches? (asset set changed?) — *intended*
   deterministic O(1) gate
3. Question-SQL alignment still holds? (LLM-based, expensive)
4. All curated questions present? (set difference)
5. Count ≥ 30? (count gate)

Only if all pass → reuse. Otherwise → regenerate.

Gates 1, 2, and 4 are cheap and deterministic and catch decay modes an LLM
can't reliably see: schema drift (fingerprint), broken SQL (EXPLAIN),
missing curated questions (set difference). These should stay.

**Current bug:** gate 2 is not reliably active today. `create_evaluation_dataset`
writes `asset_fingerprint` into each row's `expectations`, but
`load_benchmarks_from_dataset` reconstructs benchmark dicts from
`inputs`/`expectations` without copying `expectations["asset_fingerprint"]`
back into the returned benchmark dict. `_load_or_generate_benchmarks` then
checks `_bm.get("asset_fingerprint")`, which is usually empty for loaded rows.
Fix the loader before treating fingerprint reuse as a real deterministic gate.

Gates 3 and 5, plus the implicit assumption that quantity ⇒ quality, are
where the design falls down. The code never assesses **diversity or
difficulty** — semantic properties no deterministic check can measure. A
space with 35 near-identical single-filter benchmarks would pass all five
gates and be reused indefinitely.

**Conclusion:** first restore gate 2 by preserving `asset_fingerprint` when
loading benchmark rows. Then replace gates 3 + 5 (and the missing
diversity/difficulty assessment) with a single LLM triage call. The LLM
receives the current schema + candidate benchmark set + a rubric (diversity,
quantity 30–40, difficulty spread, schema alignment) and returns either
`{verdict: "good_enough"}` or `{verdict: "regenerate", reason: ...}`.

**Tradeoff:** adds one LLM call to every run (even the reuse path) and the
verdict isn't replay-safe (same inputs may yield different verdicts across
runs, complicating the audit trail). The deterministic gates are idempotent
and free. A hybrid is the right answer: fingerprint + EXPLAIN
deterministically first; if those pass, one LLM call for the semantic
judgment; regenerate only if the LLM says to.

## Finding 9 — Task 02 builds dead legacy eval scaffolding on the production path

**Where:** `jobs/run_02_baseline_eval_and_triage.py`,
`optimization/harness.py` (`baseline_setup_scorers`,
`_build_predict_and_scorers`, `baseline_run_evaluation`),
`optimization/scorers/__init__.py` (`make_all_scorers`),
`optimization/eval_runner.py` (`OfficialBenchmarkRunner`)

The eval is mid-migration from an in-process MLflow-based evaluator to the
native Genie Benchmark API (`genie_create_eval_run` /
`genie_get_eval_run`). The native path is the **default**
(`USE_OFFICIAL_BENCHMARK_RUNNER=true`), and on that path Task 02's Step 02b
builds a pile of machinery that is never invoked:

- **`predict_fn`** — `make_predict_fn` builds a Genie conversation API
  caller. On the native path the server runs the questions itself, so
  `predict_fn` is built by `_build_predict_and_scorers` and then never
  called. Pure dead allocation.
- **8 of the 9 judges** — `make_all_scorers` builds syntax_validity,
  schema_accuracy, logical_accuracy, semantic_equivalence, completeness,
  response_quality, asset_routing, result_correctness, and the arbiter. On
  the native path, `build_eval_output_from_official` populates
  `per_judge = {"result_correctness": frac}` directly from the server's
  `num_correct`/`num_done` — none of the 9 locally-built scorers run. The
  comment in `eval_runner.py` is explicit: per-judge thresholds are derived
  only from `result_correctness`; "Reworking acceptance / per-judge
  thresholds is Phase 3."
- **`scorers` list** — passed to `run_evaluation`, referenced for
  `scorer_count` telemetry and the precheck phase, then never invoked when
  the official runner takes over.

The legacy in-process path only fires when: (a) the feature switch is off,
(b) `w` is not a real `WorkspaceClient` (unit/integration tests with
`MagicMock`), or (c) `resolve_space_benchmark_qids` returns `None` pre-creation
(a Phase 2 gap). None of these hold in production.

**Conclusion:** on the production path, Task 02 Step 02b's setup work is dead
scaffolding. The task's actual job on the production path is: call
`genie_create_eval_run`, poll, persist iteration 0, stamp `best_accuracy`.
Everything else is retained for the legacy fallback and a Phase 3 that hasn't
landed.

## Finding 10 — Task 02 fetches the space config twice and re-collects UC metadata a third time

**Where:** `jobs/run_02_baseline_eval_and_triage.py` (Step 02b),
`optimization/harness.py` (`_build_predict_and_scorers`),
`optimization/preflight.py` (`preflight_collect_uc_metadata`)

Step 02b makes **two** live `fetch_space_config` calls:

1. Inside `_build_predict_and_scorers` — to extract `text_instructions` for
   the scorers' instruction context (only used on the legacy path).
2. In the notebook body — to feed `preflight_collect_uc_metadata` and
   `baseline_persist_state(config_snapshot=...)`.

Neither reads the `config_snapshot` already on `genie_opt_runs` (the trigger-
time snapshot). The justification for a live fetch is that Task 01 pushed
benchmarks into the live space, so the config has drifted since trigger time
— defensible. But the *second* fetch plus a *third* `preflight_collect_uc_metadata`
run (Task 01 already did one, `_build_predict_and_scorers` re-fetches the
config for instruction text, and the notebook body re-fetches + re-collects
UC) is pure redundancy.

The root cause: UC metadata is attached to an in-memory dict that dies with
the task's process, so each task re-derives it. The trigger-time
`config_snapshot` exists in Delta but is treated as a rollback anchor, not a
working input.

**Conclusion:** collapse the two in-task fetches to one. The broader UC
re-collection across tasks is covered by Change E.

## Finding 11 — Task 02 is entirely eliminable; the loop should start with the eval

**Where:** `jobs/run_02_baseline_eval_and_triage.py`,
`jobs/run_03_optimize.py`, `optimization/harness.py` (`_run_lever_loop`),
`integration/discard.py`, `optimization/applier.py` (`rollback`,
`apply_patch_set`)

Task 02's runtime job, stripped of legacy scaffolding, is: run a native
eval, persist iteration 0, stamp `best_accuracy`. But none of the rollback
anchors in the system depend on Task 02's eval result:

1. **User discard** (`discard.py`): reads `genie_opt_runs.config_snapshot` —
   the trigger-time snapshot, written before the job starts. This is the
   rollback to original. No Task 02 dependency.
2. **Coverage pass rollback** (lever loop): uses `_coverage_pre_snapshot`, a
   `copy.deepcopy(metadata_snapshot)` captured at loop entry — an in-memory
   snapshot of the config before enrichment. No Task 02 dependency.
3. **Surgical attempt rollback** (`apply_patch_set`): each attempt captures
   `pre_snapshot = copy.deepcopy(metadata_snapshot)` before applying patches,
   and rolls back to that if the attempt regresses. No Task 02 dependency.

Task 02's baseline eval result is used for exactly one thing: the
`_frozen_baseline_accuracy` that the coverage pass compares against (the "bar
to beat"). But that value is just "the accuracy of the space before
optimization started" — which is iteration 0 of the loop by definition.

**The simplification:** Task 03 should start by running the baseline eval
itself (iteration 0). The trigger-time `config_snapshot` is already the
rollback anchor. This is an intentional behavior change: the optimization loop
always begins with a native Genie Benchmark API run, and the result decides
whether any LLM repair work is needed. The loop becomes:

```
iteration 0:  run eval → accuracy = X
              persist iteration-0 eval row
              if X >= target → CONVERGED, move to publish/audit
              if X <  target → enter LLM analyze-patch loop

iteration N:  LLM analyzes failures from iteration N-1
              LLM suggests a patch (lever selection + rationale)
              apply patch → run eval
              persist iteration-N eval row
              if accuracy improved and >= target → CONVERGED
              if accuracy improved and <  target → continue
              if accuracy regressed → rollback to iteration N-1 config
              if max attempts reached → MAX_ITERATIONS
```

Every eval-analyze-patch cycle builds on the previous iteration's success.
The LLM's reasoning job — analyze why the space failed specific benchmarks,
suggest a targeted patch — is the core of the loop. The current 11-stage RCA
spine, coverage/surgical two-mode controller, and slice/p0/full eval gates are
over-engineered scaffolding around what is fundamentally an LLM-driven
hypothesis-test loop.

**What this eliminates:**
- Task 02 entirely (one fewer DAG task, no `triage` artifact, no separate
  baseline eval stage bookkeeping).
- `get_baseline_eval_state` cross-task handoff (the baseline is in-memory
  iteration 0 of the same loop).
- The `_frozen_baseline_accuracy` / `best_accuracy` stamping dance — the
  loop just compares each iteration's accuracy to `target_accuracy`.
- The double config fetch + UC re-collection (Finding 10) — the loop already
  has the config in memory from its own setup.

**What must be preserved:**
- The trigger-time `config_snapshot` on `genie_opt_runs` — this is the
  rollback anchor for user-initiated discard. Already exists, no change.
- Per-iteration `pre_snapshot` capture in `apply_patch_set` — each patch
  application must still capture a pre-snapshot for in-loop rollback.
- The `genie_opt_iterations` Delta table — iteration 0 is still written, just
  by the loop prologue instead of a separate task.
- The iteration-0 row shape that publish/UI/history code already consume. The
  producer moves from Task 02 to the Task 03 prologue, but the durable Delta
  contract should not change.
- A single internal accuracy scale. `run_03_optimize.py` currently normalizes
  `0.90` to `90.0`; the new prologue and loop must compare against the user
  target on one consistent scale.

**Accepted tradeoff:** folding reduces Repair-Run granularity. Today an operator can
re-run Task 02 alone (cheap — one eval) without re-running the whole loop.
After folding, a Repair Run of the merged task re-runs the baseline eval +
loop together. This is acceptable: the baseline eval is a single
`genie_create_eval_run` call (cheap relative to the loop), and the
config_snapshot rollback anchor is in Delta regardless.

## Finding 12 — The `triage` artifact is write-only

**Where:** `jobs/run_02_baseline_eval_and_triage.py` (Step 02c),
`optimization/state.py` (`ARTIFACT_KINDS`)

`write_artifact(spark, run_id, "triage", ...)` is called in Task 02, but
`load_artifacts` has no runtime callers and no backend route loads it. Task 03
reads baseline state via `get_baseline_eval_state` (Delta read of
`genie_opt_iterations` row at iteration=0) and `load_run` (Delta read of
`genie_opt_runs`) — not the artifact.

The artifact's fields are either derivable from the iteration-0 row
(`baseline_accuracy`, `thresholds_met`, `baseline_failures`) or are `None`
placeholders that Task 03 populates internally (`selected_cluster`,
`root_cause`, `allowed_patch_family`). The `regression_questions` field is
always `[]` at baseline time by definition.

The Task 03 notebook header previously claimed `**Reads** | triage artifact`,
but the code made no `load_artifacts` call. That stale header has been fixed.

**Conclusion:** `triage` is the second write-only artifact (alongside
`space_snapshot`). Safe to delete the write; the iteration-0 Delta row is the
contract. (Note: `benchmark_qc` is *not* write-only — the backend's
`_load_latest_artifact(run_id, "benchmark_qc")` and the frontend's
`BenchmarkChangesPanel` consume it. Only the `gt_correction_candidates: []`
field within `benchmark_qc` is vestigial.)

## Finding 13 — The slice/P0/full 3-tier eval gate is obsolete on the native eval path

**Where:** `optimization/harness.py` (`_run_gate_checks`, lines 9379–10000+),
`common/config.py` (`ENABLE_SLICE_GATE`, `GSO_FULL_BENCHMARK_ONLY_EVAL`)

Each surgical attempt runs up to **three** evals in sequence:

1. **Slice gate** — runs only benchmarks affected by the patched objects.
   Fast early-warning; rollback immediately if the slice regresses.
2. **P0 gate** — runs a priority-0 subset. Another fast early-warning.
3. **Full eval** — runs all benchmarks. The authoritative gate.

This tiered design was built for the earlier evaluator path where a subset run
was a cheap early warning and a full run was the expensive authoritative check.
With the native Genie Benchmark API (`genie_create_eval_run`), slice/P0 gates
are still separate eval-run submissions when opted in; they submit fewer
questions, but they also add extra server-side runs before the authoritative
full eval. The cleanup argument is not "subset and full cost exactly the same";
it is that the subset gates are now a legacy, default-disabled control path
that adds branching, tolerance tuning, and extra round-trips around a full-eval
acceptance policy.

The `GSO_FULL_BENCHMARK_ONLY_EVAL` flag already defaults to `true`, which
disables the slice/P0 gates. But the code paths, config flags, tolerance
tuning (`SLICE_GATE_MIN_REDUCTION`, `SLICE_GATE_SMALL_CORPUS_ROWS`,
`SLICE_GATE_TOLERANCE_SMALL_CORPUS`), and the `_run_gate_checks` function
(~600 lines) are all still present.

**Conclusion:** delete the slice/P0 gate code paths entirely once operators no
longer need the opt-in legacy path. Each surgical attempt runs exactly one full
eval. This eliminates `_run_gate_checks` and its associated
tolerance/bypass/broadness logic.

## Finding 14 — The proactive enrichment pass is a separate "coverage mode" that doesn't fit the unified loop

**Where:** `optimization/harness.py` (`_run_lever_loop` Phase 1, lines
11626–11955), `_run_description_enrichment`, `_run_proactive_join_discovery`,
`_run_space_metadata_enrichment`, `_finalize_coverage_decision`

Before the surgical loop begins, `_run_lever_loop` runs a "coverage pass" —
three broad, non-targeted enrichment sub-passes:

1. `_run_description_enrichment` — LLM-generates table/column descriptions.
2. `_run_proactive_join_discovery` — discovers and adds join hints.
3. `_run_space_metadata_enrichment` — generates space description + sample
   questions.

Each sub-pass re-fetches the config fresh (`fetch_space_config`) after
mutating the live space (3 API calls in Phase 1 alone). After all three, a
full eval measures whether enrichment helped; if it regressed, the whole
pass rolls back as one unit via `_finalize_coverage_decision`.

This coverage/surgical two-mode controller adds enormous complexity:
- A separate `_coverage_pre_snapshot` deepcopy and its own rollback protocol.
- The `allow_cluster_agnostic` strategist escape hatch (coverage allows
  multi-cluster AGs; surgical enforces one-cluster-per-AG).
- `_finalize_coverage_decision` with its `LOOP_STATE_INVALID` terminal reason.
- Attempt-number threading (`_attempt_no` = coverage=1, surgical=2..N).

In the user's unified loop vision, enrichment is just another patch the LLM
might propose — not a separate pre-loop phase with its own eval/rollback
protocol. The LLM sees that table descriptions are missing and proposes a
`update_table_description` patch; the loop evals it and accepts/rolls back
like any other patch.

**Conclusion:** the coverage mode should be absorbed into the unified
surgical loop. The three enrichment functions become tools the LLM can
invoke (or patch types it can propose), not separate pre-loop phases. This
eliminates the two-mode controller, the coverage pre-snapshot protocol, and
the `allow_cluster_agnostic` branching.

## Finding 15 — `_run_lever_loop` is 11,000 lines with 60+ feature flags; unmaintainable

**Where:** `optimization/harness.py` (22,627 lines total),
`common/config.py` (137 `GSO_` flags),
`optimization/optimizer.py` (15,818 lines)

`_run_lever_loop` spans lines 11101–22137 (11,036 lines). The iteration
body alone (the surgical while loop) is ~9,000 lines. The file has 158
functions. `optimizer.py` (the strategist) is another 15,818 lines.

The code bears the marks of years of incremental development layered behind
feature flags for "byte-stability" (replay-identical behavior with the flag
off). Comments reference "Cycle 5 T1", "Tier 4", "Phase 8", "T2.16", "P3
task 4", etc. — each a incremental change that was never cleaned up after
landing. There are **137 `GSO_` flags** in `config.py` and **61 flag checks**
in `harness.py` alone.

Examples of accumulated complexity in a single iteration body:
- Per-iteration journey ledger accumulators (`_journey_events`,
  `_journey_emit`, `_render_current_journey`).
- Decision-record emitters (`_decision_emit`, 17 append sites in the body).
- Patch-survival ledger (`PatchSurvivalSnapshot`, `build_patch_survival_table`).
- Structural-synthesis buffer (`_structural_synthesis_buffer`, forced
  synthesis at lever-5 gate drops).
- Strategist memoization (`strategist_memo_cache`, `_memo_key`).
- Collision guard (`_compute_forbidden_ag_set`, `_ag_collision_key`).
- Intent collision detection (`_intent_collisions`).
- RCA terminal decision state (`_rca_terminal_state`,
  `RcaTerminalDecision`, `RcaTerminalStatus`).
- Eval budget guard (`_eval_budget`, `_estimate_full_benchmark_seconds`).
- Per-question journey ledger (Task 13).
- Cycle 5 emit-dedup sets (`_iter_emitted_keys`, `_emit_idempotency_key`).

Each of these is a feature that was added incrementally, gated by a flag,
and never folded back into the main path. The result is that the core loop
— eval → analyze → patch → eval — is buried under ~9,000 lines of
observability, accounting, and gating scaffolding.

**Conclusion:** this file cannot be incrementally simplified. The right path
is Change K — rewrite the loop as a clean eval → analyze → patch → eval
cycle, porting only the essential logic (strategist prompt, patch apply,
rollback, reflection buffer) and leaving the observability/accounting
scaffolding behind.

## Finding 16 — The strategist context payload is enormous and grows every iteration

**Where:** `optimization/optimizer.py` (`_call_llm_for_adaptive_strategy`,
lines 9470–9810)

The strategist LLM call receives a massive context payload that accumulates
across iterations:

- Hard failure clusters (with RCA cards, blame sets, question lists)
- Soft signal clusters
- Full metadata snapshot (the entire space config)
- Reflection buffer (grows every iteration — every prior attempt + outcome)
- Priority ranking (clusters sorted by impact)
- Persistence summary (what's already been applied — grows every iteration)
- Proven patterns (patches that worked in prior runs)
- Human reviewer suggestions
- IQ scan findings
- RCA themes + conflicts
- Identifier allowlist
- Intent collisions
- Prior iteration's dropped causal patches
- Attempt number + mode

The prompt is truncated to a token budget (`_truncate_context_to_budget`),
but the accumulation + truncation logic is itself complex. The reflection
buffer and persistence summary grow unboundedly across iterations.

**Conclusion:** the strategist prompt should be simplified to: (a) the
failing benchmarks from the last eval, (b) the current space config, (c) a
short reflection of what was tried in the last 1–2 iterations. The LLM's
job is to analyze failures and propose a patch — it doesn't need 15 context
fields or unbounded history.

## Finding 17 — `fetch_space_config` is called 3+ times in Phase 1 alone

**Where:** `optimization/harness.py` (`_run_lever_loop` Phase 1, lines
11660–11695)

Phase 1's three enrichment sub-passes each mutate the live space and then
re-fetch the config to get the updated state:

```python
enrichment_result = _run_description_enrichment(...)
if enrichment_result.get("total_enriched", 0) > 0:
    config = fetch_space_config(w, space_id)  # re-fetch #1

join_result = _run_proactive_join_discovery(...)
if join_result.get("total_applied", 0) > 0:
    config = fetch_space_config(w, space_id)  # re-fetch #2

meta_result = _run_space_metadata_enrichment(...)
if meta_result.get("description_generated"):
    config = fetch_space_config(w, space_id)  # re-fetch #3
```

Each re-fetch is a live Genie API call. The re-fetch is needed because the
enrichment functions patch the space via the Genie API, and the in-memory
`config` dict is stale after the patch. But the enrichment functions could
return the updated config (or the patched portions) instead of forcing the
caller to re-fetch.

**Conclusion:** in the unified loop (Change K), this is moot — each
iteration's `apply_patch_set` returns `post_snapshot`, which becomes the
working config for the next iteration. No re-fetch needed.

## Finding 18 — `publish_and_audit` duplicates champion selection between two modules

**Where:** `optimization/publish.py` (`resolve_champion_row`),
`optimization/models.py` (`promote_best_model`)

The publish task calls both:
1. `resolve_champion_row(scored_iters)` — picks the champion for the audit
   context (checks `is_champion` flag first, else highest-accuracy
   non-rolled-back row with baseline as floor).
2. `promote_best_model(spark, run_id, ...)` — re-implements the same
   "pick highest-accuracy non-rolled-back full/enrichment row" logic,
   stamps `is_champion`, and updates `best_iteration`/`best_accuracy`.

Both functions independently implement the candidate universe filter
(`eval_scope IN ('full', 'enrichment')`), the rolled-back exclusion (with
baseline-as-floor), and the max-accuracy selection. The selection should be
extracted into a small shared helper that both modules can call, or
`publish_and_audit` should pass the selected champion row into the stamping
path. Do **not** make `models.py` import `publish.py` directly: `publish.py`
already imports `promote_best_model`, so that direction would create an import
cycle.

**Conclusion:** unify the selection logic, but do it cycle-aware. Low-risk
cleanup if the shared selector lives outside `publish.py`/`models.py` or the
call direction is adjusted deliberately.

## Finding 19 — `publish_and_audit` loads `run_row` but barely uses it

**Where:** `optimization/publish.py` (`publish_and_audit`, `as_audit_context`)

`run_row = load_run(spark, run_id, catalog, schema)` is loaded at the top of
`publish_and_audit`, then passed to `as_audit_context`, which only reads
`run_row.get("run_id")` and `run_row.get("space_id")` — both already
available as function parameters to `publish_and_audit`. The `load_run` call
is a Delta query with no real consumer.

**Conclusion:** drop the `load_run` call; pass `run_id` and `space_id`
directly to `as_audit_context`. Keep the later post-`promote_best_model`
`load_run` refresh, which is used to read the stamped `best_accuracy`.

## Finding 20 — Champion selection's `enrichment` scope is a two-mode-controller vestige

**Where:** `optimization/publish.py` (`resolve_champion_row`,
`build_improvement_trajectory`), `optimization/models.py`
(`promote_best_model`), `optimization/state.py` (`load_all_scored_iterations`)

`load_all_scored_iterations` filters to `eval_scope IN ('full', 'enrichment')`
so an accepted coverage rung (`eval_scope='enrichment'`) can be the champion.
`resolve_champion_row` and `promote_best_model` both specially handle this
scope. `build_improvement_trajectory` sorts the coverage rung as "attempt 1"
before surgical rungs.

The `enrichment` scope only exists because of the coverage/surgical two-mode
controller (Finding 14). If Change M (absorb coverage into the unified loop)
lands, there's no separate enrichment scope — every iteration is just an
iteration with `eval_scope='full'`. The candidate filter collapses to
`eval_scope='full'`, the special-casing disappears, and the trajectory is a
flat staircase of iterations 0..N.

**Conclusion:** downstream cleanup that follows naturally from Change M.
No dedicated change needed — the publish logic simplifies itself when the
two-mode controller goes away.

## Finding 21 — `LOOP_STATE_INVALID` terminal reason is a coverage-rollback vestige

**Where:** `optimization/publish.py` (`_TERMINAL_REASON_TO_RUN_STATUS`,
`_TERMINAL_REASON_CONCERN`), `optimization/harness.py`
(`_finalize_coverage_decision`)

The `LOOP_STATE_INVALID` terminal reason is only set by
`_finalize_coverage_decision` when the coverage pass can't prove its rollback
(too many consecutive rollbacks, unprovable state). It maps to run status
`FAILED` and carries a concern: "the loop state was inconsistent." If Change
M lands, the coverage rollback protocol is gone, so `LOOP_STATE_INVALID`
can never be set. The mapping and concern in `publish.py` become dead code.

**Conclusion:** downstream cleanup that follows from Change M. Remove the
`LOOP_STATE_INVALID` entries from `_TERMINAL_REASON_TO_RUN_STATUS` and
`_TERMINAL_REASON_CONCERN` once the coverage pass is absorbed.

## Finding 22 — Audit summary prompt references two-mode-controller terminology

**Where:** `common/config.py` (`AUDIT_SUMMARY_PROMPT`)

The prompt instructs the LLM to cover "what the broad COVERAGE attempt
(attempt 1) did, then the SURGICAL attempts (2..N)." This terminology only
makes sense under the coverage/surgical two-mode controller. If the unified
loop (Change K) lands, the prompt should say "iterations 1..N" instead.

**Conclusion:** minor prompt update that follows from Change K. No
dedicated change needed — update the prompt text when the two-mode
controller is removed.

## Finding 23 — Redundant non-`src/` files: orphaned bundle, broken ledgers, one-shot notebooks

**Where:** `packages/genie-space-optimizer/` (files outside `src/`)

A sweep of the package root turned up files that are orphaned, one-shot, or
broken. One initial candidate was a false positive:
`packages/genie-space-optimizer/databricks.yml` is still an active validated
source of truth for the 4-task job shape. The root `databricks.yml`,
`scripts/deploy_lib/gso_job.py`, docs, and `tests/unit/test_phase7_job_dag.py`
explicitly reference or mirror it. Do **not** delete the package
`databricks.yml` unless that source-of-truth contract is intentionally moved.

The remaining candidates:

- **`deploy.sh`** — package-local standalone deploy wrapper for the old
  standalone `genie-space-optimizer` Databricks App path. It is not called by
  the root `install.sh`, root `deploy.sh`, or active notebook install path. If
  deleted, update package backend error messages/docs that still say "run
  deploy.sh" in the standalone context.
- **`databricks.yml`** — explicitly preserved, not a deletion candidate. It is
  the package bundle source mirrored by the Workbench root bundle and deploy
  library tests.
- **`docs/2026-05-05-optimizer-iteration-ledger.md`** — a behavioral-hardening
  cycle ledger whose 18 sibling planning-doc links are all missing
  (gitignored under `docs/*`).
- **`docs/runid_analysis/cycle_11_falsification_probe.md`** — an incomplete
  one-off probe (result table is `<fill>` placeholders).
- **`docs/optimizer-process-design/burn-down-ledger.md`** — unreferenced by
  `00-index.md` or any other doc.
- **`notebooks/reconstruct_cycle7_fixture.py`** — a self-described one-shot
  operator notebook; the module it calls is tested separately.
- **`scripts/create_instruction_quality_dataset.py` +
  `scripts/run_instruction_quality_eval.py`** — zero references anywhere.
- **Tier 2 (judgment call):** `08-slide-outline.md`, `appendices/B-visual-prompts.md`,
  `interactive-optimizer-visualization.html` — SA pitch material referenced by
  `00-index.md` but not engineering docs.

See Change P for the deletion list and the explicitly-preserved set.

---

## Proposed Changes

### Change A — Delete the `space_snapshot` artifact write

**Files:** `jobs/run_00_intake_and_snapshot.py`, `optimization/state.py`

Remove the `write_artifact(spark, run_id, "space_snapshot", …)` call in
Task 00. The canonical rollback anchor is already
`genie_opt_runs.config_snapshot`, which is what `discard.py` reads.

**Risk:** Low. No runtime reader of this artifact exists. The only theoretical
consumer is a future audit/UI reader; if one is added later, it can read
`genie_opt_runs.config_snapshot` directly (the same data).

**Follow-up:** Remove `"space_snapshot"` from `ARTIFACT_KINDS` in
`optimization/state.py` once the write site is gone, so the allowlist stays
honest. (Keep it if you want the artifact kind reserved for a future
re-introduction.)

### Change B — Drop `data_access_grants` from the payload

**File:** `jobs/run_00_intake_and_snapshot.py`

If Change A is rejected (artifact kept), at minimum remove the
`"data_access_grants": []` field from the `space_snapshot` payload. It's a
v1 vestige with no reader.

**Risk:** Zero.

### Change C — Collapse `serialized_space`/`config` if artifact is kept

**File:** `jobs/run_00_intake_and_snapshot.py`

If Change A is rejected, collapse the payload to a single `serialized_space`
field (drop `config`). The enriched projections (`_parsed_space`, etc.) are
regenerable from `serialized_space` and are not read back from the artifact
anyway. Drop `config_hash` too unless it is intentionally being reserved for a
new consumer; current code has no reader.

**Risk:** Low. No reader distinguishes the two fields today.

### Change D — ~~(Optional) Move `config_hash` onto the run row~~

**Status: Withdrawn.** This change was predicated on `config_hash` having a
reader that needed migrating before the `space_snapshot` artifact could be
deleted. But verification shows `config_hash` / `baseline_config_hash` has
**zero readers** anywhere — every reference is a write site in
`run_00_intake_and_snapshot.py`. Task 01's benchmark-reuse gate 2 is intended
to compare `asset_fingerprint` read from benchmark *rows* in
`genie_benchmarks_<domain>`, not `config_hash` from any artifact. That
fingerprint gate has its own loader bug (Finding 8), but there is still no
`config_hash` migration needed. Change A can proceed directly without this
change.

### Change E — Collapse UC metadata merge into a single source-of-truth read

**Files:** `optimization/preflight.py` (`preflight_collect_uc_metadata`),
lever-loop mutation sites in `optimization/harness.py` / `applier.py`

Keep the UC fetch (data types, tags, FKs, routine signatures are genuinely
needed), but stop reconciling inlined `column_configs.comment` against UC
comments. Treat UC as the single source of truth for column metadata in the
working config; apply patch mutations only to the live Genie Space via the
API, not to the in-memory mirror.

**Risk:** High — touches the lever-loop mutation path. The current merge
exists because levers 1–3 mutate `column_configs` in place before PATCHing.
Removing the in-memory mirror requires reworking how patches are rendered.
Defer until the lever loop is otherwise stabilized.

### Change F — Replace benchmark reuse gates 3+5 with one LLM triage call

**Files:** `optimization/preflight.py` (`_load_or_generate_benchmarks`)

First fix the persisted-row loader so `load_benchmarks_from_dataset` copies
`expectations["asset_fingerprint"]` back into each benchmark dict. Then keep
deterministic gates 1 (count ≥5), 2 (fingerprint match), and 4 (curated
coverage) as-is — they're cheap and catch decay the LLM can't see. Replace gate
3 (alignment LLM call) + gate 5 (count ≥30) + the missing diversity/difficulty
assessment with a single LLM triage call invoked only after gates 1, 2, 4 pass.
The LLM returns a structured verdict; regenerate only on
`{verdict: "regenerate"}`.

**Risk:** Medium. Adds one LLM call per run on the reuse path. Verdict is not
replay-safe — mitigate by logging the prompt + response + model id to the
`benchmark_qc` artifact so the audit trail records *why* reuse was
accepted/rejected. Net cost should be *lower* than today because the current
gate 3 already makes an LLM alignment call; this consolidates it. The
fingerprint-loader fix is a prerequisite, not part of the LLM-triage behavior
change.

### Change G — Rip out the legacy in-process eval scaffolding from Task 02

**Files:** `jobs/run_02_baseline_eval_and_triage.py`,
`optimization/harness.py` (`baseline_setup_scorers`,
`baseline_run_evaluation`, `_build_predict_and_scorers`),
`optimization/scorers/__init__.py` (`make_all_scorers`),
`optimization/eval_runner.py`

On the production path (`USE_OFFICIAL_BENCHMARK_RUNNER=true`), Task 02's
Step 02b builds `predict_fn` + 9 scorers + runs `baseline_setup_scorers` /
`baseline_run_evaluation`, none of which are consulted — the native
`OfficialBenchmarkRunner` returns `per_judge = {"result_correctness": frac}`
directly from the server. Strip Task 02 down to:

1. `OfficialBenchmarkRunner.run(space_id, qids)` — one native eval call.
2. `build_eval_output_from_official(result)` — map to the legacy dict shape.
3. `baseline_persist_state(...)` — write iteration 0 + stamp
   `best_accuracy`.

Delete `baseline_setup_scorers` / `_build_predict_and_scorers` /
`make_all_scorers` from the Task 02 call path (keep them as a private
fallback behind the feature switch only if the legacy path is still
exercised by tests; otherwise delete outright). Stop calling
`make_predict_fn` on the native path.

**Risk:** Medium — touches the eval entry point. Tests that pass
`MagicMock` workspaces currently fall back to the legacy path; they'd need
either a fake `OfficialBenchmarkRunner` or to be migrated to assert against
`build_eval_output_from_official` directly. The fail-closed contract (D1)
must be preserved: qid-resolution failure must still map to a failed gate,
not a silent pass.

**Follow-up:** Phase 3 (rewire acceptance / per-judge thresholds to the
native shape) unblocks deleting `build_eval_output_from_official`'s legacy
dict mimicry — at that point the 9-judge `scores` dict faking can go too.

### Change H — Collapse Task 02's double config fetch + UC re-collection

**Files:** `jobs/run_02_baseline_eval_and_triage.py`,
`optimization/harness.py` (`_build_predict_and_scorers`)

Task 02 Step 02b calls `fetch_space_config` twice (once inside
`_build_predict_and_scorers` for instruction text, once in the notebook
body for UC metadata + persist) and re-runs `preflight_collect_uc_metadata`
a third time overall. Collapse to one fetch in the notebook body; pass the
result into the (slimmed) setup function instead of letting it re-fetch.

**Risk:** Low — pure plumbing. The instruction-text extraction moves to the
caller. Behavior is identical on both eval paths.

### Change I — Delete the `triage` artifact write

**File:** `jobs/run_02_baseline_eval_and_triage.py`

Remove the `write_artifact(spark, run_id, "triage", ...)` call. No runtime
reader exists (Finding 12); Task 03 reads baseline state from
`genie_opt_iterations` and `genie_opt_runs`. Drop `"triage"` from
`ARTIFACT_KINDS` in `optimization/state.py` once the write site is gone.

**Risk:** Zero. Same pattern as Change A / Change B.

### Change J — Eliminate Task 02; fold the baseline eval into `optimize`'s loop prologue

**Status: Implemented in structural cutover.**

**Files:** `jobs/run_02_baseline_eval_and_triage.py` (delete),
`jobs/run_optimize.py`, `optimization/unified_loop.py`, the Databricks Job DAG
definition, `jobs/_handoff.py`
(`get_baseline_eval_state` — delete or inline)

`optimize` now starts with the baseline eval as iteration 0 of the loop, then
enters the analyze-patch cycle:

```
optimize (new shape):
  1. Load benchmarks from genie_benchmarks_<domain>
  2. Run native eval (genie_create_eval_run) → iteration 0
  3. Persist iteration 0 in genie_opt_iterations
  4. If accuracy >= target_accuracy → CONVERGED, write terminal state, exit
  5. If accuracy <  target_accuracy → enter loop:
       a. LLM analyzes iteration N-1 failures (which benchmarks failed, why)
       b. LLM suggests a patch (selects a lever, writes the patch JSON,
          provides rationale)
       c. apply_patch_set (captures pre_snapshot for in-loop rollback)
       d. Run native eval → iteration N and persist the eval row
       e. If accuracy >= target → CONVERGED
          If accuracy improved but < target → continue to N+1
          If accuracy regressed → rollback to pre_snapshot, continue
          If max_attempts reached → MAX_ITERATIONS
```

The trigger-time `config_snapshot` on `genie_opt_runs` remains the
user-discard rollback anchor (unchanged). Per-iteration `pre_snapshot` in
`apply_patch_set` remains the in-loop rollback anchor (unchanged).

**What gets deleted:**
- `run_02_baseline_eval_and_triage.py` (the entire notebook).
- `get_baseline_eval_state` cross-task handoff (baseline is in-memory iter 0).
- The `triage` artifact (Change I, now subsumed).
- `baseline_setup_scorers` / `baseline_run_evaluation` / `baseline_persist_state`
  as a separate Task-02 call path (the loop prologue writes the same iteration-0
  durable contract directly or through a renamed helper).
- The double config fetch + UC re-collection (Finding 10 — the loop has the
  config in memory from its own setup).
- The `_frozen_baseline_accuracy` / `best_accuracy` stamping dance — the
  loop compares each iteration to `target_accuracy`, not to a stamped
  baseline.

**Prerequisites:**
- Change G must land first (rip out legacy eval scaffolding) so the loop's
  eval call is the slim native path, not `baseline_run_evaluation`.
- The Task 03 prologue must preserve the iteration-0 Delta shape that
  publish/UI/history already consume.
- The target-accuracy comparison must use one internal scale throughout the
  prologue and loop.
- The 11-stage RCA spine / coverage-surgical two-mode controller is the
  current implementation of "analyze → suggest patch." This change does not
  require rewriting that spine in the same PR — the spine can be preserved
  as the analyze-patch implementation behind the new prologue. A follow-up
  change (Change K, below) can simplify the spine itself.

**Result:** the DAG is now four unnumbered tasks:
`intake_and_snapshot -> benchmark_qc_and_repair -> optimize -> publish_and_audit`.
Repair-run granularity changes as expected: rerunning optimize reruns the
baseline eval plus loop together.

### Change K — Rewrite the lever loop as a clean eval → analyze → patch → eval cycle

**Status: Implemented in structural cutover.**

**Files:** `optimization/unified_loop.py`, `jobs/run_optimize.py`,
`common/config.py`

Rewrite `_run_lever_loop` as the unified loop the user envisions:

```
iteration 0:  run eval → accuracy = X
              if X >= target → CONVERGED
              if X <  target → enter loop

iteration N:  LLM analyzes iteration N-1 failures
              LLM suggests a patch (lever + rationale)
              apply_patch_set (captures pre_snapshot)
              run eval → iteration N
              if accuracy >= target → CONVERGED
              if accuracy improved but < target → continue
              if accuracy regressed → rollback to pre_snapshot, continue
              if max_attempts reached → MAX_ITERATIONS
```

This rewrite subsumes several other changes:
- **Change L** (delete slice/P0 gate) — the unified loop runs one full eval
  per attempt.
- **Change M** (absorb coverage mode) — enrichment becomes a patch type the
  LLM can propose, not a separate pre-loop phase.
- **Change N** (simplify strategist context) — the LLM receives only failing
  benchmarks + current config + short reflection.
- **Finding 17** (Phase 1 re-fetches) — moot; `apply_patch_set` returns
  `post_snapshot`, which becomes the next iteration's config.

**What to port from the current implementation:**
- `_call_llm_for_adaptive_strategy` (simplified) — the strategist prompt +
  JSON response parsing.
- `apply_patch_set` — the patch applier with leakage firewall and pre/post
  snapshot capture.
- `rollback` — the per-attempt rollback to `pre_snapshot`.
- Reflection buffer (capped at last 2–3 iterations, not unbounded).
- `genie_opt_iterations` / `genie_opt_patches` Delta writes for audit.

**What to leave behind:**
- The 3-tier eval gate (`_run_gate_checks`, slice/P0/full).
- The coverage/surgical two-mode controller.
- The 60+ feature flags and their gating branches.
- The journey ledger, patch-survival ledger, decision-record emitters,
  structural-synthesis buffer, collision guard, intent collision detection,
  RCA terminal decision state, emit-dedup sets.
- `strategist_memo_cache` (the LLM shouldn't see identical inputs twice if
  the loop is working correctly).
- The tolerance tuning / small-corpus bypass logic.

**Result:** the active job path imports `run_unified_optimization_loop`.
No `GSO_UNIFIED_LOOP` feature flag was added per user decision; the old
`harness._run_lever_loop` is not in the active job path.

### Change L — Delete the slice/P0 eval gate code paths

**Status: Implemented for the active path.**

**Files:** `optimization/harness.py` (`_run_gate_checks`),
`common/config.py` (`ENABLE_SLICE_GATE`, `SLICE_GATE_*` flags)

Delete the slice and P0 gate code paths entirely. Each surgical attempt
runs exactly one full eval. This eliminates `_run_gate_checks` (~600 lines),
the slice/P0 tolerance tuning (`SLICE_GATE_MIN_REDUCTION`,
`SLICE_GATE_SMALL_CORPUS_ROWS`, `SLICE_GATE_TOLERANCE_SMALL_CORPUS`,
`SLICE_GATE_TOLERANCE`), and the small-corpus bypass logic.

**Result:** the unified loop runs one full native Benchmark API eval per
candidate. `GSO_ENABLE_LEGACY_SLICE_P0_GATES` no longer enables a runtime
fallback.

### Change M — Absorb the coverage/enrichment pass into the unified loop

**Status: Implemented in structural cutover.**

**Files:** `optimization/harness.py` (`_run_lever_loop` Phase 1,
`_finalize_coverage_decision`, `_run_description_enrichment`,
`_run_proactive_join_discovery`, `_run_space_metadata_enrichment`),
`optimization/optimizer.py` (`allow_cluster_agnostic` branching)

In the unified loop, enrichment is just another patch the LLM can propose.
The three enrichment functions become tools/patch types available to the
strategist:
- `update_table_description` (already a lever-1 patch type)
- `add_join_spec` (already a lever-4 patch type)
- `update_space_metadata` (new patch type or reuse lever-5)

The LLM sees that descriptions are missing (from the metadata snapshot) and
proposes the appropriate patch. The loop evals it and accepts/rolls back
like any other patch.

**What gets deleted:**
- The separate Phase 1 enrichment block (3 sub-passes + 3 config re-fetches).
- `_finalize_coverage_decision` and the coverage rollback protocol.
- `_coverage_pre_snapshot` and the `LOOP_STATE_INVALID` terminal reason.
- The `allow_cluster_agnostic` strategist escape hatch and attempt-number
  threading.
- Phase 1.5 (instruction restructuring) and Phase 1.6 (instruction
  snapshot) — these become part of the LLM's analysis, not separate phases.

**Result:** there is no separate coverage/enrichment phase in the active
workflow, and `LOOP_STATE_INVALID` is no longer a terminal reason emitted by
the active path.

### Change N — Simplify the strategist context payload to failing benchmarks + config + short reflection

**Status: Implemented in structural cutover.**

**Files:** `optimization/optimizer.py` (`_call_llm_for_adaptive_strategy`,
`_build_context_data`, `_truncate_context_to_budget`)

Reduce the strategist's context payload to three fields:

1. **Failing benchmarks from the last eval** — the question, the model's
   (wrong) SQL, the expected SQL, and the judge verdict.
2. **Current space config** — the metadata snapshot (tables, instructions,
   examples, join specs).
3. **Short reflection** — the last 1–2 iterations' patch + outcome (accepted
   or rolled back + why).

Delete the accumulation logic for reflection buffer, persistence summary,
proven patterns, intent collisions, RCA themes, and human suggestions. The
LLM's job is to analyze failures and propose a patch — it doesn't need
15 context fields or unbounded history.

**Result:** `optimization/unified_loop.py` builds a compact prompt from the
last eval failures, a projected current config, and the last two reflections.

### Change O — Unify champion selection + drop unnecessary `run_row` load in publish

**Files:** `optimization/publish.py` (`resolve_champion_row`,
`as_audit_context`, `publish_and_audit`), `optimization/models.py`
(`promote_best_model`)

Two cleanups in the publish task:

1. **Unify champion selection.** `promote_best_model` re-implements the same
   candidate filter + max-accuracy selection as `resolve_champion_row`.
   Extract a shared selector (or pass the selected champion row into the
   stamping path) so `promote_best_model` does only the
   `mark_champion_iteration` + `update_run_status` stamping. Do not make
   `models.py` import `publish.py` directly, because `publish.py` already
   imports `promote_best_model`.
2. **Drop the unnecessary `load_run`.** `publish_and_audit` loads `run_row`
   but `as_audit_context` only reads `run_id` and `space_id` from it — both
   already available as function parameters. Pass them directly; drop the
   initial `load_run` call. Keep the later post-promote `load_run` refresh
   that reads stamped `best_accuracy`.

**Risk:** Low. Pure cleanup in the publish task, but the implementation must be
import-cycle-aware. The publish_record artifact's shape is unchanged. Can be
done independently of the structural changes.

**Downstream note:** Findings 20–22 (enrichment scope special-casing,
`LOOP_STATE_INVALID`, audit prompt terminology) were handled as part of the
structural cutover: promotion is full-scope only, the active path no longer
emits `LOOP_STATE_INVALID`, and the audit prompt describes baseline +
patch/eval iterations.

### Change P — Delete redundant non-`src/` files (package-level cleanup)

**Where:** `packages/genie-space-optimizer/` (files outside `src/`)

A sweep of the package root turned up several files that are orphaned,
one-shot, or broken. Split into two tiers:

**Tier 1 — clearly redundant or narrowly redundant:**

| File | Why it's redundant |
|---|---|
| `deploy.sh` | Package-local standalone deploy wrapper for the old standalone `genie-space-optimizer` app path. Not called by root `install.sh`, root `deploy.sh`, CI, or the active notebook install path. Before deleting, update package-local backend messages/docs that still refer to "run deploy.sh" for the standalone path. |
| `docs/2026-05-05-optimizer-iteration-ledger.md` | Behavioral-hardening cycle ledger for completed Phase-A burn-down work. References **18 sibling planning docs** (`2026-05-01-…`, `2026-05-04-cycle-*-plan.md`, …) — all 18 are **missing** (gitignored under `docs/*`). Broken links, pure historical record. |
| `docs/runid_analysis/cycle_11_falsification_probe.md` | One-off Cycle 11 falsification probe whose result table is filled with `<fill>` placeholders — the investigation was never completed/recorded. |
| `docs/optimizer-process-design/burn-down-ledger.md` | Cycle-11-specific burn-down content. **Not referenced** by `00-index.md` or any other doc. Unreferenced historical artifact. |
| `notebooks/reconstruct_cycle7_fixture.py` | Self-described "one-shot — Not part of the 6-task DAG" operator notebook repairing a *specific* Cycle 7 replay fixture (run ID `78557321-…`). The module it imports (`scripts.reconstruct_airline_real_v1_fixture`) is tested separately in `tests/unit/test_reconstruct_fixture.py`; the notebook wrapper is the redundant artifact. |
| `scripts/create_instruction_quality_dataset.py` + `scripts/run_instruction_quality_eval.py` | **Zero references** anywhere in `src/`, `tests/`, `backend/`, or `frontend/`. Orphaned MLflow `genai.datasets`/scorer scripts. |

**Tier 2 — judgment call (marketing/sales enablement, not engineering):**

These *are* referenced by `docs/optimizer-process-design/00-index.md`, so
they're not orphaned — but they're SA pitch material, not engineering docs.
Move to a separate sales-enablement location or delete if unmaintained:

- `docs/optimizer-process-design/08-slide-outline.md` — 20-slide SA deep-dive storyboard.
- `docs/optimizer-process-design/appendices/B-visual-prompts.md` — designer/LLM image-generation prompts.
- `docs/optimizer-process-design/interactive-optimizer-visualization.html` — 3,757-line standalone interactive microsite.

If Tier 2 is deleted, also prune the corresponding rows from `00-index.md`'s
table of contents and audience-routing matrix.

**Explicitly NOT touched** (verified consumers exist):

`databricks.yml` (package bundle source mirrored by root `databricks.yml`,
`scripts/deploy_lib/gso_job.py`, docs, and `tests/unit/test_phase7_job_dag.py`);
`scripts/dedupe_benchmark_qids.py`, `scripts/lint_example_sql_isolation.py`,
`scripts/refresh_dim_date_flags.sql`, `scripts/migrate_expected_asset.py`,
`scripts/record_replay_baseline.py`, `scripts/extract_replay_fixture_from_log.py`
(all referenced by runtime/test code); `vitest.config.ts`, `.npmrc`, `AGENTS.md`,
`CHANGELOG.md` (active config/docs); `docs/optimizer-process-design/00–07 +
appendices/A,C` (active design docs).

**Risk:** Low for the verified one-shot docs/scripts. Low-medium for package
`deploy.sh` because stale docs/error messages need cleanup. Low for Tier 2 if
the sales-enablement material is intentionally retired — only the `00-index.md`
cross-references need pruning. Independent of all other changes, but no longer
a blanket zero-risk deletion pass.

---

## Recommended Sequencing

**Implemented / closed:**

1. **Changes A/B/I/O/P** and the benchmark fingerprint loader prerequisite for
   Change F were completed before this structural cutover.
2. **Changes G/H** are subsumed by removing Task 02 entirely.
3. **Change J** is complete: Task 02 is gone, baseline eval is iteration 0
   inside `optimize`, and the DAG is four unnumbered tasks.
4. **Change K** is complete for the active job path:
   `optimization/unified_loop.py` owns the eval → analyze → patch → eval loop.
5. **Change L** is complete for the active path: each candidate runs one full
   native Benchmark API eval, and the legacy slice/P0 opt-in is disabled.
6. **Change M** is complete for the active path: enrichment is an LLM-proposed
   patch, not a separate coverage phase.
7. **Change N** is complete for the active path: the strategist prompt is
   reduced to last-eval failures, current config projection, and short
   reflection.

**Remaining candidates:**

1. **Change F** (LLM triage for benchmark reuse) remains open.
2. **Change E** (UC single-source-of-truth) remains deferred; it is still a
   high-risk applier/rendering refactor.
3. Retired `harness.py` / `control_plane.py` compatibility code can be deleted
   in a follow-up once the historical harness tests and replay fixtures are
   intentionally retired. It is no longer in the active Databricks Job path.

## Out of Scope

- The `preflight_fetch_config` API fallback (Finding 4) — keep as defensive
  code; removing it would break manual/test run-row inserts.
- The `run_manifest` duplication (Finding 5) — the manifest has real
  consumers (UI, provenance); the duplication is intentional audit redundancy.
- Any change to `genie_opt_runs.config_snapshot` itself — it's the canonical
  rollback anchor and must stay.

## Resolved Questions

### Q1 — Does any external consumer read the `space_snapshot` artifact?

**Resolved: No.** Verified across the full stack:

- **GSO backend routes** (`backend/routes/`): no `load_artifacts` calls. The
  `space_snapshot` references in `backend/routes/spaces.py` are a *local
  variable name* (a dict holding a freshly fetched space config) used during
  run creation/trigger — not a read from `genie_opt_artifacts`.
- **React frontend** (`frontend/src/`): the `space_snapshot` mentions in
  `components/auto-optimize/resolution.ts` and `ResolutionActions.tsx` are
  *comments describing the rollback concept*, not artifact reads. The actual
  discard call hits the backend `/discard` endpoint, which reads
  `genie_opt_runs.config_snapshot`.
- **GSO UI package** (`ui/`): no artifact reads.
- `load_artifacts` itself has zero runtime callers anywhere in the package.

**Verdict:** Change A is safe to proceed — deleting the `space_snapshot`
artifact write breaks no consumer.

### Q2 — Is `captured_at` on the artifact consumed by any audit report?

**Resolved: No.** The `captured_at` field on the `space_snapshot` payload
(written at `run_00_intake_and_snapshot.py:203`) has no reader. The
`captured_at_utc` references in `tools/evidence_bundle.py` /
`tools/evidence_layout.py` are a *different field* — an MLflow evidence-bundle
timestamp, unrelated to the space_snapshot artifact.

**Verdict:** `captured_at` does not need to be preserved or migrated when the
artifact is deleted. It's dead state and can be dropped alongside Change A.

## Open Questions

(None — all resolved.)
