# Cursor Prompt Playbook — Building the Metric View Advisor in Genie Workbench

*A sequenced set of Cursor prompts that take the POV document from design to a tested, PR-ready feature branch in `databricks-solutions/databricks-genie-workbench`. One prompt ≈ one reviewable commit. Run them in order; each prompt's output is the next prompt's input.*

> **Custody.** From this commit forward, this in-repo copy is the canonical playbook and the canonical MV-D register. Any external copy is downstream: reconcile it to this file, never the reverse.

---

## Before you start (manual steps, 10 minutes)

1. **Commit the design doc AND this playbook into the repo** so Cursor can reference both in every prompt. The playbook is the defining source of the MV-D decision numbering (MV-D1–MV-D9 today, appended to as later prompts take architecture calls); if it is not in the repo, agents cannot resolve the citations and will (correctly) refuse to stamp them:
   ```bash
   git checkout main && git pull
   git checkout -b feature/metric-view-advisor
   mkdir -p docs/design
   cp metric-view-suggestion-engine-pov.md docs/design/
   cp cursor-prompt-playbook-mv-advisor.md docs/design/mv-advisor-playbook.md
   git add docs/design && git commit -m "docs: metric view advisor design POV + build playbook"
   ```
   Note on decision namespaces: this repo already carries a GSO v2 playbook numbering (D1–D9, of which D9 = no task values) and a separate D1–D3 inside `applier.py`. This playbook's decisions are therefore cited as **MV-D1…MV-D8**, and any reference to the others must be namespace-qualified ("GSO v2 D9", "applier.py D2").
2. **Create the Cursor rules file INSIDE the repo** — `databricks-genie-workbench/.cursor/rules/mv-advisor.mdc`, not the workspace root's `.cursor/`. A workspace-level rules file works in your session but is unversioned: it doesn't travel with the branch, doesn't survive a fresh clone, and gives a teammate running these prompts none of the guardrails. Commit it with the design docs. *(This needs a `.gitignore` change, done in the Prompt 1 follow-up: the repo ignored `.cursor/`, so the in-repo copy was just as unversioned as a workspace-root one. `.gitignore` now reads `.cursor/*` plus `!.cursor/rules/`, because a re-include cannot reach inside an excluded parent directory.)* This keeps every prompt honest without repeating the guardrails each time:

   ```
   ---
   description: Non-negotiables for the metric view advisor feature
   alwaysApply: true
   ---
   Paths below are under databricks-genie-workbench/ from this workspace root.
   Repo reality (and the gap report) wins over older POV wording. Do not
   revive taskValues, condition tasks, run_if, extra job tasks, or a second
   eval adapter.

   READ-BEFORE-WRITE CONTRACT (applies to every prompt on this branch):
   - Before modifying ANY file, you must first output a PLAN section containing:
     (1) the exact file paths you intend to modify or create,
     (2) for each existing file, a quoted excerpt of the CURRENT code at the
         location you will change (read it fresh in this session — do not rely
         on memory of a previous session),
     (3) every existing function, class, endpoint, table, task_key, or config
         key you intend to call or extend, each with the file:line where it is
         defined today.
   - If any item in (3) cannot be located in the current codebase, STOP after
     the PLAN and report it as a blocker. Do not scaffold a stand-in.
   - If the PLAN contradicts docs/design/mv-advisor-gap-report.md or the POV
     doc, STOP and report the contradiction before editing.
   - After edits, output a VERIFY section: run the test suite and linter for the
     touched packages, and confirm (by search) that every symbol you referenced
     exists and every symbol you defined is exported/registered the way this
     repo registers it (router inclusion, entrypoint table, migration list).
   - ABSENCE claims require a positive control. Any "X does not exist in this
     repo" check must run from the repo root and first confirm a known-present
     sentinel (e.g. databricks.yml) resolves from the same working directory.
     An absence check whose positive control fails is void — rerun it, do not
     report it.
   - VERIFY includes `git status`. Run Python tooling with a frozen lockfile
     (`uv run --frozen` or equivalent); the working tree after VERIFY must be
     clean apart from the intended changes. A dirtied uv.lock is a finding to
     report, never a file to commit.
   - Context discovery stays inside the repository. Do not read files outside
     the repo (home directories, ~/Downloads, other checkouts) unless the
     prompt names them explicitly.

   FEATURE RULES (amended after Prompt 0 recon — repo reality wins):
   - Design sources of truth: docs/design/metric-view-suggestion-engine-pov.md
     AS AMENDED BY docs/design/mv-advisor-gap-report.md. Where they disagree,
     the gap report wins.
   - Never invent API endpoints, table names, task names, wheel entrypoints,
     or config keys. Read existing code first. If a name is not in the repo,
     ASK.
   - The GSO job is a LINEAR FOUR-TASK NOTEBOOK DAG: intake_and_snapshot →
     benchmark_qc_and_repair → optimize → publish_and_audit. Do NOT add tasks,
     condition tasks, run_if edges, or wheel entry points. GSO v2 playbook decision D9 (a different D-namespace from this playbook's
     MV-D numbering) and test_phase7_job_dag.py forbid condition tasks and
     taskValues — respect the
     tests, do not delete or weaken them.
   - Cross-task and cross-phase state passes through Delta tables keyed by
     run_id (genie_opt_* via optimization/ddl.py). NEVER dbutils.jobs.taskValues.
   - Job parameters are read with dbutils.widgets. Any new parameter is added
     IN LOCKSTEP to all three job-definition mirrors — root databricks.yml,
     packages/genie-space-optimizer/databricks.yml, and
     scripts/deploy_lib/gso_job.py — plus backend/job_launcher.py's run_now map.
     A parameter present in fewer than all four places is a bug.
   - Metric-view code inside the job runs as GATED PHASES within the existing
     optimize task, each wrapped in try/except; on failure, persist a status
     row to Delta and let the optimization continue. Never raise across the
     phase boundary.
   - All benchmark eval calls go through the EvalRunner seam in
     optimization/eval_runner.py (OfficialBenchmarkRunner). No second adapter,
     no raw SDK eval calls anywhere else.
   - Patches are dicts with type/target/new_text/old_text, registered in
     PATCH_TYPES (common/config.py) and allowlisted in unified_loop. New MV
     patch types must implement a REAL applier action that mutates
     data_sources.metric_views — the current Lever-2 MV path renders then
     no-ops at config level, and that no-op is the bug this feature fixes.
   - Rollback is whole-snapshot revert (integration/revert.py). Detach-on-
     regression = snapshot revert. NEVER auto-drop the created UC object
     outside sandbox mode; drop is an explicit backend OBO endpoint.
   - The job runs as the SERVICE PRINCIPAL. Therefore the job NEVER executes
     CREATE VIEW ... WITH METRICS. UC object creation happens ONLY in the
     FastAPI backend under OBO (require_obo_workspace_client), at trigger time,
     against a recorded consent. The job attaches (patch), measures, and
     reverts — writes it already performs as SP today.
   - Do not reuse GET /permissions/{space_id} for entitlement: it probes the
     SP's privileges, not the user's. The entitlement probe is new OBO code.
   - On entitlement/consent mismatch, DOWNGRADE to suggest_only — never
     upgrade, never fall back to a different schema.
   - MLflow is decommissioned in GSO except tracing. No experiments, registry,
     or judges. Grading comes ONLY from native benchmark eval runs.
   - Backend routes live under the /api/auto-optimize router prefix; the
     run-start endpoint is POST /api/auto-optimize/trigger with the inline
     TriggerRequest model — extend it, never add a parallel start endpoint.
   - Run artifacts persist to the genie_opt_artifacts Delta table, not a
     Volumes path.
   - Firewall: extend optimization/leakage.py for MV metadata/DDL rather than
     writing a second scanner. No literals, sample values, or PII in shipped
     metadata, DDL comments, or logs.
   - Materialization is a separate consent (mv_materialize); never bundled.
   - GAP-REPORT FRESHNESS (MV-D9): the gap report outranks the POV, so a stale
     quote there actively misleads. Any commit that changes something the gap
     report quotes or counts — _ALL_DDL membership, task keys, patch shape,
     router paths, parameter lists, file:line anchors — MUST refresh the
     affected gap-report sites in the SAME commit, re-quoting live code rather
     than editing the prose around it. Every PLAN that touches such a surface
     states which gap-report sites it will refresh, or asserts none apply.
   - GENERATED YAML conforms to the generation quality standard in POV Part 4
     (MV-D8): version "1.1" quoted; no name/time_dimension/top-level
     window_measures/join_type/table-in-joins; multi-hop ladder (denormalize ->
     nested joins only on DBR>=17.1 with profiling-proven 1:1 keys ->
     subquery-source fallback); NEVER a flat sibling join referencing another
     join alias; SCD2 joins carry is_current=true; rely.at_most_one_match only
     with proven uniqueness; format types from the closed set (percentage not
     percent, number not decimal); additive measures aggregate fact columns.
   - MV comments use the structured format (PURPOSE/BEST FOR/NOT FOR/...).
     BEST FOR lines are PARAPHRASED intents — verbatim benchmark question text
     in any shipped MV metadata contaminates the benchmark and is a firewall
     violation, same class as literals/PII.
   - Updates to an existing metric view use ALTER VIEW ... AS $$...$$, never
     CREATE OR REPLACE or drop+create (replace deletes UC grants).
   - Post-create: DESCRIBE EXTENDED must show Type: METRIC_VIEW. Validation
     queries use MEASURE(`name`) with GROUP BY — never SELECT *, never a
     re-typed aggregate. Fan-out smoke test on every join.
   - Every new module ships with pytest tests in the same PR-sized commit.
     The repo has NO test CI — run the suites locally and report results in
     every VERIFY section.

   - IMPORT RESOLUTION IS PART OF EVERY VERIFY THAT RUNS BACKEND TESTS: before
     trusting any backend pytest result, print
     `python -c "import genie_space_optimizer; print(genie_space_optimizer.__file__)"`
     and confirm the path resolves inside THIS repository checkout. A test run
     against a foreign checkout is void — report it, do not interpret it.
   ```

3. **Have a dev workspace ready** with the TPC-H samples catalog, a scratch schema you own (e.g. `main.mv_advisor_dev`), a small Genie Space with 10–15 benchmark questions, and a second test identity that *lacks* `CREATE TABLE` on the scratch schema (you need it for the denial-path E2E test).

---

## The standard preamble (paste at the top of every prompt, 1 through 17)

Cursor sessions do not share memory. A prompt run in a fresh chat carries none of the recon from Prompt 0, and the rules file constrains behavior but cannot inject knowledge. So every implementation prompt starts with this block, verbatim, above the prompt body:

```
CONTEXT: @docs/design/metric-view-suggestion-engine-pov.md
         @docs/design/mv-advisor-gap-report.md

PHASE 1 — EXPLORE (no edits): Re-read the two context docs. Then read, in this
session, every file you expect to touch for the task below, plus the files that
define anything you will call. Output your PLAN per the read-before-write
contract in .cursor/rules — file paths, quoted current code at each change
site, and file:line for every existing symbol you will use. If anything the
task assumes does not exist in the current code, or the gap report is stale
relative to what you just read, STOP and report; do not proceed to Phase 2.

PHASE 2 — IMPLEMENT: Only after I reply "proceed" (or if the plan has zero
blockers and zero doc conflicts, in which case say so explicitly and continue).
Then output the VERIFY section: tests, linter, and symbol checks per the rules.
```

Two-phase execution costs one extra reply per prompt and is the single most effective drift control in this playbook: the model must demonstrate it has read the current code — by quoting it — before it is allowed to change it. When a PLAN quotes code that doesn't match what you know is there, you've caught a stale context before it became a bad diff, not after.

If you use Cursor's agent mode end-to-end instead of reviewing each phase, keep the preamble anyway — the STOP conditions still fire on missing symbols and doc conflicts, which are the two failure modes that matter.

---

## Phase 0 — Recon (do not skip)

The POV doc makes assumptions about task names, entrypoints, and schemas that must be reconciled against the real repo before any code is written. This prompt produces the map every later prompt depends on.

### Prompt 0 — Repo reconnaissance and gap report

```
Read docs/design/metric-view-suggestion-engine-pov.md in full, especially Parts 1c,
7, and the Caveats.

Then explore this repo and produce docs/design/mv-advisor-gap-report.md containing:

1. REPO MAP: For each of the following, the exact file paths and names as they
   exist TODAY (quote the code, do not paraphrase):
   - The GSO job definition (Databricks Asset Bundle YAML): every task_key, its
     task type, entrypoint, and parameters. Confirm or correct the doc's claimed
     preflight → baseline → enrichment → lever_loop → finalize → deploy sequence.
   - The optimizer Python package: package name, where entrypoints are registered,
     how a task reads job parameters and task values today.
   - How the codebase currently calls the Genie Benchmark Eval APIs (create eval
     run / get run / list results). File, function names, retry handling.
   - The patch model: the class/schema for field_path + new_value patches, where
     patches are applied, and where rollback lives.
   - Persistence: the Lakebase models and Delta tables a GSO run writes, and the
     migration mechanism for adding new tables.
   - The FastAPI app: router layout, how OBO tokens are extracted and used, how
     the app service principal is used, and the existing run-config endpoint that
     starts an Auto-Optimize run.
   - The React app: component layout, state management, the run-config panel
     component, the run output/results screen, and any existing graph or
     visualization library already in package.json.
   - The existing test layout: unit test framework, fixtures, any integration
     test harness against a live workspace, CI config.

2. GAP TABLE: For every assumption in the POV doc's Part 7 (task names like
   mv_gate/metric_view_advisor/metric_view_apply/mv_baseline, parameter names,
   task-values names), state MATCHES / CONFLICTS / DOES-NOT-EXIST-YET, with the
   repo evidence.

3. DECISIONS NEEDED: A short list of naming or placement decisions I must make
   before implementation (e.g. "the repo calls the loop task `optimize_loop`,
   not `lever_loop` — doc should be amended").

Do not write or modify any feature code in this prompt.
```

**Your job after Prompt 0:** read the gap report, make the naming decisions, and amend the POV doc where the repo wins. Commit both. Every prompt below says "per the gap report" — that is deliberate.

---

## Decisions register (MV-D1–MV-D9)

The recon surfaced five structural conflicts, not naming drift. These decisions resolve them and are baked into the revised prompts below. MV-D1 changes the user-facing flow and needs explicit sign-off. MV-D7 was added during Prompt 1 execution, MV-D8 with the generation quality standard, and MV-D9 from the Prompt 2 readiness check; later decisions append here — this register is the defining namespace, and the playbook copy committed at docs/design/mv-advisor-playbook.md must be refreshed whenever it changes.

**MV-D1 — Two-run consent model (the big one).** The job launches as the service principal (`integration/trigger.py` → `backend/job_launcher.py`), and the no-SP-writes rule stands. So the job cannot run `CREATE VIEW … WITH METRICS` under the user's identity, and there is no supported way to run the job as the requesting user per-run. Resolution: **creation moves to the backend, at trigger time, under OBO — which means create_and_attach applies to already-approved proposals.** The flow becomes: run N (any mode) produces proposals → user reviews and approves → **[Re-run with this metric view]** → backend re-probes entitlement, creates the approved MV under OBO, passes its identifier as a job parameter → run N+1 attaches it via patch, measures lift, and optimizes on top. A *first* run for a given proposal is always suggest-only, because the proposal does not exist until the advisor has seen the baseline SQL. Rejected alternatives: passing an OBO token as a job parameter (a credential in run metadata), and SP-created views (ownership lands on the app identity and violates the design's own rule). The consent-panel copy in Prompt 10 changes accordingly: "Create and attach" is enabled only when approved proposals exist for the space.

**MV-D2 — Attach is a real patch type, and it fixes an existing no-op.** New entries in `PATCH_TYPES` (e.g. `mv_attach_data_source`, plus optional raw-table removal) with genuine applier actions that mutate `data_sources.metric_views`, added to the unified-loop allowlist. The recon found Lever-2 MV patches currently render and then no-op at config level (`applier.py:3914-3932`) — this feature ships the missing applier action rather than working around it.

**MV-D3 — In-job work is phases inside the optimize task, not new tasks.** Sequence inside `run_optimize`: iteration-0 baseline eval (already exists) → *mv_attach phase* (only when an approved-view identifier arrived as a job parameter) → *mv-lift eval* (a labeled eval run via the EvalRunner seam, isolating the metric view's delta before the loop moves anything else) → unified loop → *mv_advisor phase* persisting proposals for the next run. Each phase is try/except-isolated with a Delta status row; handoff is Delta-by-run_id per GSO v2 D9.

**MV-D4 — The rules file and POV are amended, not the tests.** `test_phase7_job_dag.py` encodes deliberate decisions (no condition tasks, no task values, the four-task collapse). The feature conforms; Prompt 0.5 records the deltas in the POV.

**MV-D5 — Four-place lockstep for job parameters.** Root `databricks.yml`, the package-bundle mirror, `scripts/deploy_lib/gso_job.py`, and `backend/job_launcher.py` must all carry every new `mv_*` parameter. The gap report should state this explicitly so no prompt touches one mirror without the others.

**MV-D6 — Rollback simplifies; drop stays manual.** Whole-snapshot revert means detach-on-regression comes free with the existing `integration/revert.py` path. The never-auto-drop rule is unchanged; drop is a backend OBO endpoint gated on DETACHED status.

**MV-D7 — Stateful MV entities get dedicated tables; run-scoped blobs stay in `genie_opt_artifacts`.** Three tables join `_ALL_DDL`: `genie_opt_mv_candidates` (partitioned by target_space_id — the upsert key is space-scoped and a candidate outlives the run that proposed it under MV-D1), `genie_opt_mv_consents` (unpartitioned — run_id is NULL at probe time and filled at trigger), and `genie_opt_mv_created_objects` (partitioned by run_id; merge key run_id + suggestion_id). The rendered DDL text remains a `genie_opt_artifacts` row under a new artifact_kind, with `content_hash` set to the dedup fingerprint so the stores cross-reference. Rationale: all three entities carry mutable, cross-run-lifecycle state that a run-partitioned append-style handoff table cannot host without fighting its own partitioning. Supersedes POV Appendix A Deltas 4 and 6 where they assert otherwise. Process rule established alongside it: **appendices record decisions; they do not make them** — any new architecture call surfaces here as an MV-D entry first, then the appendix cites it.

*As implemented (Prompt 1):* accessors live in `optimization/mv_state.py`, one module rather than an extension of the already-large `optimization/state.py`, following the `scan_snapshots.py` precedent for feature-scoped persistence. Every writer goes through a new `delta_helpers.merge_row` primitive, so the §7.9 idempotency key `sha256(space_id | canonical_measure_expr | sorted_source_set)` is enforced as a single-statement upsert-on-conflict rather than a read-then-insert a retry can duplicate. JSON payload columns carry a `_json` suffix in storage and travel base64-encoded; the accessor signatures use the POV Part 4 field names (`score_components`, `evidence`, `provenance`, `alternatives`, `conflicts`, `probe_results`) verbatim. POV Part 4's proposal `type` is stored as `candidate_type` to avoid shadowing the builtin at the API. `reverified_at_trigger` is a `TIMESTAMP` (NULL = never re-verified), and `updated_at` sits on all three tables. `lift_report_json` is deferred to an `ADDITIVE_COLUMN_MIGRATIONS` entry in Prompt 7, when the lift report exists to store. Three judgment items raised in the Prompt 1 VERIFY were reviewed and approved in the Prompt 1 follow-up and are now settled: decision-column ownership (`upsert_mv_candidate` never writes them; `record_mv_candidate_decision` owns them, pinned by `test_re_proposing_does_not_resurrect_a_rejected_candidate`), the "Resolved by MV-D7" pointers added to the gap report, and the user-facing table rows in `docs/docs/features/auto-optimize.md` with their empty-table-is-not-a-failed-run framing.

**MV-D9 — The gap report is refreshed in the same commit that invalidates it.** Commit 1 added three tables to `_ALL_DDL` and left four gap-report sites (`:784` heading, the `:789` quoted block, the `:1382` verdict row, the `:1697` summary row) asserting six tables. Because the rules make the gap report outrank the POV, and the POV's Delta 6 had already been retitled "Nine Delta tables," an agent reading both was told the authoritative source says six and the POV's nine is the error — exactly backwards. Rule: any commit that changes something the gap report quotes or counts refreshes those sites in the same commit, re-quoting live code with current line anchors. Enforced by a rules-file bullet and by every PLAN naming the gap-report sites it will refresh.

**MV-D8 — Generation quality standard adopted (metric-views-patterns v5.2).** All engine-emitted YAML is rendered and validated by one module (`mv_yaml`, Prompt 5.5) enforcing: v1.1 unsupported-field and format-type lints; the multi-hop decision ladder (denormalize → nested joins on DBR ≥ 17.1 with profiling-proven 1:1 keys → subquery-source fallback) with a hard transitive-join gate; SCD2 `is_current` guards; `rely.at_most_one_match` only on proven uniqueness; `MEASURE()`-composed derived metrics, `FILTER`-clause conditional counts, and Fixed-LOD percent-of-total (never `MEASURE()/MEASURE()`); structured comments with **paraphrased** BEST FOR lines (verbatim benchmark text is a firewall violation — it contaminates the benchmark the view is graded by); `ALTER VIEW` for all updates (grants-preserving); post-create `DESCRIBE EXTENDED` type assertion; `MEASURE()`-syntax validation queries with a fan-out smoke test; a copy-ready `GRANT SELECT` checklist surfaced, never auto-applied; and capability rows (DBR 17.3/17.1/18.1 floors) in the entitlement probe that downgrade the join strategy rather than emit unplannable YAML. Origin: the metric-views-patterns skill (v5.2, 2026-06-06); the sample YAML in POV Part 4 was itself corrected under this standard (its customer join was transitive).

### Prompt 0.5 — Amend the design docs (run before Phase 1)

```
Per docs/design/mv-advisor-gap-report.md and decisions MV-D1–MV-D6 recorded in the
playbook, add an "Implementation deltas" appendix to
docs/design/metric-view-suggestion-engine-pov.md that supersedes the conflicting
parts of Part 7: the two-run consent model (MV-D1) replacing single-run
create-and-attach; phases-in-optimize-task replacing the mv_gate/mv_write_gate/
mv_baseline task DAG and the task-values contract (7.2, 7.6, 7.7, 7.7.1); the
dict patch model with new PATCH_TYPES replacing field_path/new_value (7.8 step
6); genie_opt_artifacts replacing the Volumes DDL artifact path; POST
/api/auto-optimize/trigger replacing /runs; six genie_opt_* tables (extended)
replacing "~15 Delta tables"; MLflow tracing-only. Do not rewrite the
superseded sections — mark each with a one-line pointer to the appendix so the
original reasoning stays legible. Keep every product-behavior requirement
intact: consent scoping and re-verification, downgrade-never-upgrade, the
suggest-only output contract (7.5), detach-never-drop, the firewall, and the
"Lift not measured" label.
```

### Prompt 0.6 — Reconcile decision numbering in the appendix

```
docs/design/mv-advisor-playbook.md is now committed and is the defining source
of the MV-D1..MV-D6 decision numbering (three-namespace note near the top).
Update Appendix A of the POV: replace the sequentially numbered deltas'
"needs reconciliation" markers with their MV-D identifiers per the playbook;
keep the three-namespace disambiguation table; namespace-qualify every
reference to the other two numberings ("GSO v2 D9", "applier.py D1–D3")
everywhere in the appendix. No other content changes. VERIFY: every MV-D
citation in the appendix resolves to a matching entry in the playbook, and no
bare D<number> remains in the appendix outside the disambiguation table.
```


---

## Phase 1 — Backend foundations

### Prompt 1 — Data model and migrations

```
Per the gap report: persistence uses genie_opt_* Delta tables created in
optimization/ddl.py with name constants in common/config.py and additive-column
migrations via ADDITIVE_COLUMN_MIGRATIONS. Follow that mechanism exactly — no
new migration framework, no Lakebase for these (note gso_lakebase synced reads
are hard-disabled).

Add three tables per POV Parts 4 / 7.7.1 as amended by the deltas appendix:
1. genie_opt_mv_candidates: full proposal payload (suggestion_id, type,
   confidence_score, tier, target_space_id, proposed_object, score_components,
   evidence, provenance, dedup_fingerprint, alternatives, conflicts) plus
   run_id, requested_mode, effective_mode, created_at, decided_by, decision,
   decided_at, suppressed_until, approved_for_rerun (bool — MV-D1 hinges on it).
2. genie_opt_mv_consents: probe_id, run_id, granted_by, granted_at,
   target_catalog, target_schema, materialize_consented, probe_results,
   verdict, reverified_at_trigger, downgrade_reason.
3. genie_opt_mv_created_objects: run_id, suggestion_id, full_name, created_by,
   created_at, attach_patch_id, baseline_eval_run_id, post_attach_eval_run_id,
   status (CREATED|ATTACHED|DETACHED|DROPPED), on_regression_action.

Match the existing tables' column-type and naming conventions (read two of the
six current genie_opt_* definitions first and mirror them). Add typed accessors
following the repo's existing persistence-helper pattern. Idempotency key:
sha256(space_id | canonical_measure_expr | sorted_source_set) enforced as
upsert-on-conflict at the accessor level. Pytest round-trip coverage per table,
in the GSO package's test layout (pythonpath=["src"], existing fixtures).
```

### Prompt 2 — Extend the existing eval-run seam

```
The gap report confirms the Beta eval APIs are already isolated behind exactly
the seam the design wants: the EvalRunner protocol and OfficialBenchmarkRunner
in optimization/eval_runner.py, with poll/timeout/page settings in
common/config.py. EXTEND that module; do not create a parallel adapter, and do
not change the existing protocol methods' signatures (the unified loop depends
on them).

Add:
- run_subset(space_id, question_ids, label): a labeled eval run over a question
  subset, reusing the existing polling/terminal-status handling.
- lift_report(pre_run, post_run, question_subset): excludes needs-review
  questions from BOTH numerator and denominator; returns delta_affected,
  delta_suite, regressed_question_ids, needs_review_count.
- Assessment reasons are ALREADY surfaced — genie_eval_taxonomy.py, with
  handling in benchmarks.py and map_eval_detail_to_row (:251). CONSUME that
  taxonomy; do not build a second one. If lift_report or run_subset needs a
  reason-derived grouping the taxonomy does not expose, extend the taxonomy
  module itself and say so in VERIFY.
- If useful for cross-run history, wire genie_list_eval_runs (present in the
  SDK, currently uncalled in this repo) behind the same seam.

lift_report's return shape is a CONTRACT, not an implementation detail: MV-D7
deferred genie_opt_mv_created_objects.lift_report_json to Prompt 7, and that
column stores exactly this structure. Define it once here as a typed
dataclass with a to_dict(); Prompt 7 persists to_dict() verbatim rather than
reshaping. Name the fields in VERIFY so Prompt 7's PLAN can cite them.

Respect the existing rate posture (poll 20s / timeout 2700s per config) and
the ~20 q/min workspace ceiling: subset runs serialized, never concurrent.
Contract tests with mocked SDK responses for every terminal status and every
assessment reason. There is NO test_eval_runner.py today; the nearest files
(test_eval_timeouts.py, test_evaluation_extract_json.py) are narrowly scoped
by name. Create tests/unit/test_eval_runner.py as the seam's own contract-test
home and leave test_eval_timeouts.py to its narrower job.
```

### Prompt 3 — SQL fingerprinting engine

```
Per POV Parts 2 and 3, build the sqlglot-based canonicalization/fingerprinting
module (dialect="databricks"):

- canonicalize(sql): alias renaming by first appearance, AND-predicate flatten
  + sort, literal normalization to placeholders, case/whitespace stripping.
- extract_measures(sql): recurring aggregate expressions (SUM/COUNT/AVG/MIN/MAX,
  incl. SUM(CASE WHEN ...)), each with source columns and tables.
- extract_dimensions(sql): GROUP BY sets; extract_filters; extract_join_keys.
- fingerprint(expr) -> sha256 per the POV's dedup_fingerprint format.
- classify_shapes(corpus): tag recurring patterns the generator consumes —
  RATIO (SUM(x)/SUM(y) over the same grain -> emit atomic measures + a
  MEASURE()-composed derived measure), CONDITIONAL_COUNT
  (SUM(CASE WHEN c THEN 1 END) -> COUNT(1) FILTER (WHERE c)), and
  PCT_OF_TOTAL (a windowed total in the denominator -> Fixed-LOD dimension
  + ANY_VALUE(), NEVER MEASURE()/MEASURE() which always yields 1.0).
- corpus_scan(iterable of (sql, provenance)) -> fingerprints with recurrence
  counts, distinct provenance ids, first/last seen.
- FIREWALL: canonicalized output must contain NO literal values; add an
  assertion test that a query containing an email/SSN-shaped literal produces
  a fingerprint with placeholders only.
Test corpus: 15+ TPC-H-style queries including the discounted revenue pattern
SUM(l_extendedprice * (1 - l_discount)) with alias/whitespace/predicate-order
variants that MUST collapse to one fingerprint, plus near-misses that must NOT.
```

### Prompt 4 — Scoring, dedup, and proposal generation

```
Per POV Part 3, implement the scoring engine:

- Score = 100 * (0.35*L + 0.30*Y + 0.20*S + 0.15*D), weights in config not code.
- L: Jaccard of column sets from lineage overlap (input: precomputed overlap
  data — define the input contract, do not query system tables from this module).
- Y: normalized recurrence * AST-equivalence flag against existing MV measures
  (parse existing MV YAML from DESCRIBE ... AS JSON view_text).
- S: max cosine vs MV field text; make the embedding client injectable with a
  deterministic fake for tests (FMAPI gte-large-en in prod; note gte does not
  normalize — normalize vectors in our code).
- D: frequency*cost*breadth normalized, with half-life decay
  D_eff = D * 0.5^(age_days/30).
- Tiers: HIGH >= 75, MEDIUM 50–74, LOW 25–49, suppress < 25.
- Dedup gate per POV: exact fingerprint match on an existing MV -> block with
  pointer; partial -> alternatives[]; fingerprint matching an MV AND a
  conflicting instruction definition -> state CONFLICT, never a suggestion.
- Emit the full proposal payload (POV Part 4 JSON) and persist via Prompt 1
  accessors.
- Reproduce BOTH worked examples from POV Part 3 as exact-value unit tests
  (80.0 -> HIGH, 58.75 -> MEDIUM).
```

### Prompt 5 — Entitlement probe and consent service

```
Per POV Part 7.3.1 and Part 5, implement the entitlement probe + consent:

- probe(catalog, schema, space_id, source_tables) reads EFFECTIVE privileges
  for the SIGNED-IN USER, using require_obo_workspace_client from
  backend/services/auth.py. This is greenfield: the existing
  GET /permissions/{space_id} probes the SERVICE PRINCIPAL's access and must
  not be reused or extended for this. Checks: USE CATALOG, USE SCHEMA,
  CREATE TABLE on schema, SELECT on each source table, CAN MANAGE on the
  Genie Space. NEVER perform a trial CREATE.
- Returns the structured probe result exactly per POV 7.3.1 JSON, including
  remediation_sql GRANT statements with the principal filled in.
- consent record: create/persist per Prompt 1 model; verify(consent, fresh_probe)
  is called by the TRIGGER flow (Prompt 9) immediately before any OBO create —
  any mismatch returns effective_mode=suggest_only with downgrade_reason.
- FastAPI route: POST under the existing /api/auto-optimize router prefix
  (e.g. /api/auto-optimize/mv/probe), OBO, returning the probe result. Follow
  backend/routers/auto_optimize.py conventions exactly.
- Tests: all-granted, each-single-privilege-missing, schema-not-found, revoked-
  between-config-and-trigger (verify() downgrade), and an assertion that no
  code path issues DDL.
```

### Prompt 5.5 — YAML generator and static validator (MV-D8)

```
Per the generation quality standard in POV Part 4 (MV-D8 in the playbook
register), add mv_yaml.py to the GSO package: the ONLY place metric view YAML
is rendered, plus its static validator. Both the advisor (Prompt 6) and the
backend create path (Prompt 9) call these; neither renders YAML itself.

generate(candidate, profiling) rules — all normative:
- version "1.1" quoted. Never emit: name, time_dimension, top-level
  window_measures, join_type, or table inside joins. 'on' key quoted.
- Multi-hop decision ladder, chosen from GSO profiling (join-key uniqueness,
  denormalized-column availability, warehouse DBR): (1) denormalized column on
  the first-hop dimension; (2) nested joins ONLY if DBR >= 17.1 AND every
  intermediate key is proven 1:1 (nested columns referenced as
  parent_join.child_join.column); (3) subquery-source pre-join with explicit
  is_current guards and uniqueness enforced in the subquery. Record which rung
  was chosen and why on the proposal (join_strategy + evidence).
- rely.at_most_one_match: true only when profiling proves the dimension key
  unique; omit otherwise (it is unvalidated at runtime and fan-out inflates
  SUM/COUNT).
- SCD2: any joined dimension with an is_current-style column gets
  AND {dim}.is_current = true in the join.
- Additive measures must aggregate source (fact) columns; a measure
  aggregating a joined dimension column is rejected into the CONFLICT path.
- Shapes from Prompt 3 classify_shapes: RATIO -> atomic measures +
  MEASURE()-composed derived measure; CONDITIONAL_COUNT -> COUNT(1) FILTER
  (WHERE ...); PCT_OF_TOTAL -> Fixed-LOD dimension (SUM(x) OVER ()) read via
  ANY_VALUE(), never MEASURE()/MEASURE().
- format.type from the closed set {byte,currency,date,date_time,number,
  percentage} — reject percent/decimal/integer at generation time.
- Structured comment: PURPOSE / BEST FOR / NOT FOR / DIMENSIONS / MEASURES /
  SOURCE / JOINS / NOTE. BEST FOR = PARAPHRASED question intents (never
  verbatim benchmark text — extend the leakage firewall check to reject any
  comment line that matches a benchmark question at >0.9 normalized
  similarity). NOT FOR cross-references the adjacent MV from the dedup gate
  when one exists. 3-10 synonyms per field, <=255 chars each.

validate(yaml_text) — static, no warehouse:
- Parses; unsupported-field lint; format-type lint; transitive-join detector
  (port the left-head-of-'on' check: the left side of every first-level join's
  'on' must resolve to source, and every deeper level to its parent alias);
  synonyms limits; comment structure present; capability check against the
  probe's runtime rows (downgrade nested->subquery rather than emit
  unplannable YAML).

Tests: golden YAML per ladder rung; a transitive sibling join MUST be caught;
percent/decimal MUST be rejected; PCT_OF_TOTAL must not render as
MEASURE()/MEASURE(); a benchmark-verbatim BEST FOR line MUST be rejected;
round-trip parse with sqlglot(dialect="databricks") on the wrapping DDL.
```

---

## Phase 2 — Job integration

### Prompt 6 — Advisor phase inside the optimize task

```
Per MV-D3 and the gap report: no new job tasks, no wheel entrypoints, no task
values. Add an mv_advisor PHASE invoked from jobs/run_optimize.py, in a new
module under the GSO package following its existing module layout.

- Gating: read enable_metric_view_suggestions via dbutils.widgets, following
  the pattern in jobs/run_intake_and_snapshot.py:81-119. If not "true"
  (STRING compare), log via the jobs/_helpers.py logging helpers and return
  immediately — zero cost when off.
- Placement: after the unified loop completes (the advisor consumes
  iteration-0 generated SQL and does not need to block optimization) — read
  unified_loop.py around the iteration-0 baseline (2854-2859) to find where
  per-question generated SQL is persisted, and consume it from Delta by
  run_id, NOT from in-memory state.
- Pipeline: load iteration-0 generated SQL + benchmark SQL from the
  genie_opt_* run tables -> corpus_scan (Prompt 3) -> estate MV index via
  DESCRIBE ... AS JSON over in-scope schemas -> score + dedup (Prompt 4) ->
  persist to genie_opt_mv_candidates -> render YAML EXCLUSIVELY through
  mv_yaml.generate + validate (Prompt 5.5; the advisor never hand-builds
  YAML) -> write the DDL artifact (one SQL text blob, header comments citing
  evidence, NO literals — extended leakage.py scan, including the
  benchmark-verbatim comment check) as a row in genie_opt_artifacts, matching
  how existing artifacts are written. Persist join_strategy and its evidence
  on each candidate.
- The advisor ONLY proposes, in every mode.
- Isolation per the rules: the whole phase in try/except; on any exception,
  persist a status row (phase=mv_advisor, status=FAILED, reason) and return
  normally. Add a test proving an advisor exception does not propagate to the
  optimize task.
- Unit tests with a fixture corpus (reuse metric_view_only_space and the GSO
  conftest fixtures where they fit); golden-file test for the DDL artifact.
```

### Prompt 7 — Attach patch type and lift-measurement phase

```
Per MV-D1 the job never creates UC objects; it receives the identifier of a
metric view the backend already created under OBO. Per MV-D2/MV-D3, implement:

A. New patch types (the applier fix):
- Register mv_attach_data_source (and mv_remove_raw_table for optional table
  replacement) in PATCH_TYPES in common/config.py, add them to the unified-loop
  allowlist (unified_loop.py:79-93 region), and implement REAL applier actions
  in optimization/applier.py that mutate data_sources.metric_views in the space
  config — read _apply_action_to_config (:3409) and the Lever-2 MV no-op
  (:3914-3932) first, and route the new types through a genuine mutation, not
  the render-only path. Patch dict shape stays type/target/new_text/old_text.
- Tests: apply -> serialized_space contains the identifier; snapshot revert
  (integration/revert.py path) removes it; render_patch output is reviewable.

B. mv_attach + mv_lift phases in run_optimize, ordered BEFORE the unified loop:
1. Read job widgets: mv_attach_views (JSON list of identifiers), mv_consent_id.
   Empty -> skip both phases silently.
2. Validate: the consent row exists in genie_opt_mv_consents with verdict
   SUFFICIENT and reverified_at_trigger set; each identifier exists in
   genie_opt_mv_created_objects with status CREATED and was created_by the
   consent's granted_by. Any mismatch -> persist status row, skip (never
   attach an object the trigger flow didn't just create).
3. Iteration-0 baseline runs first (existing behavior — do not reorder it).
4. Apply the mv_attach patch(es) via apply_patch_set.
5. mv-lift eval: run_subset over the full benchmark set (or the affected
   subset if question ids were recorded on the proposal) via the Prompt 2
   seam, labeled "mv_lift"; store lift_report vs iteration-0 and both
   eval_run_ids on genie_opt_mv_created_objects; set status ATTACHED.
6. On regression per lift_report: revert to the pre-attach snapshot (detach),
   set status DETACHED with the report attached. NEVER drop the UC object.
   Sandbox mode is out of scope for the job (sandbox creation/teardown is a
   backend concern under MV-D1).
7. The unified loop then runs on whatever foundation now exists.
Each phase try/except-isolated with Delta status rows; tests cover happy path,
missing consent row, identifier/creator mismatch, regression->DETACHED, and
that a phase failure leaves the loop running.
```

### Prompt 8 — Job parameter wiring (no DAG changes)

```
Per MV-D3/MV-D5: the four-task DAG is untouched. This prompt only adds parameters,
in lockstep, to all four places the gap report identified:

- Root databricks.yml (job gso-optimization-runner, parameters block :74-109)
- packages/genie-space-optimizer/databricks.yml (mirror)
- scripts/deploy_lib/gso_job.py (JOB_PARAMETERS, :87-103)
- backend/job_launcher.py (the run_now parameter map, :105-125)

New parameters (string defaults, matching the 15 existing ones' style):
enable_metric_view_suggestions="false", mv_action_mode="suggest_only",
mv_attach_views="" (JSON list), mv_consent_id="", mv_min_confidence="75".
(No mv_target_* in the job — the target lives on the consent row; no
mv_materialize in the job — materialization is a backend/OBO concern under MV-D1.)

- Add the corresponding dbutils.widgets reads to jobs/run_optimize.py per the
  existing widget pattern.
- Validate with `databricks bundle validate` for BOTH bundles.
- Extend test_phase7_job_dag.py in the spirit of its existing assertions: the
  new parameters exist in all mirrors with identical defaults; still exactly
  four tasks; still no condition_task anywhere; still no taskValues anywhere.
  Do NOT weaken any existing assertion.
- Add a small sync test that parses all four sources and asserts the parameter
  sets are identical, so MV-D5 drift fails a test instead of a demo.
```

---

## Phase 3 — App API and UI

### Prompt 9 — FastAPI surface

```
Per the gap report: the router prefix is /api/auto-optimize, the run-start
endpoint is POST /api/auto-optimize/trigger with the inline TriggerRequest
model (backend/routers/auto_optimize.py:171-187, :1401-1458), OBO comes from
the ContextVar + x-forwarded-access-token machinery, and the job launch itself
stays SP via backend/job_launcher.py.

- Extend TriggerRequest (in place, inline, matching its existing 8-field
  style) with: enable_metric_view_suggestions, mv_action_mode,
  mv_approved_suggestion_ids, mv_consent (granted_by, granted_at, probe_id),
  mv_materialize.
- Trigger flow for create_and_attach (MV-D1 — this is the heart of it): before
  launching the job, (1) verify(consent, fresh probe) under OBO; on mismatch
  coerce to suggest_only, persist downgrade_reason, continue; (2) for each
  approved suggestion, run the pre-create checks from POV 7.8 under OBO —
  existing-object check at the target name, EXPLAIN CREATE MATERIALIZED VIEW
  only if mv_materialize, semantic validation vs the originating aggregation —
  using the backend's existing OBO SQL-execution path (find how the backend
  runs warehouse SQL today and reuse it; if none exists, use the SDK statement
  execution API via require_obo_workspace_client and say so); (3) CREATE VIEW
  ... WITH METRICS in the consented schema — YAML comes from mv_yaml (Prompt
  5.5), never rendered inline — then DESCRIBE EXTENDED and assert
  Type: METRIC_VIEW before recording genie_opt_mv_created_objects
  status=CREATED; semantic validation queries use MEASURE(`name`) with
  GROUP BY plus the fan-out row-count smoke test; (4) pass mv_attach_views +
  mv_consent_id through job_launcher's parameter map, and attach a copy-ready
  GRANT SELECT checklist for the space's audience to the run record (never
  auto-granted). Any subsequent edit endpoint for an engine-created MV must
  use ALTER VIEW ... AS $$...$$ (grants-preserving), never CREATE OR REPLACE.
  The probe (Prompt 5) additionally returns capability rows (DBR 17.3+
  create/edit, 17.1+ nested joins, 18.1+ fields/agg/window offset) that
  mv_yaml.validate consumes. Any create failure -> that suggestion drops
  out, run proceeds; all failing -> suggest_only.
- New routes under /api/auto-optimize: GET /runs/{run_id}/mv-proposals,
  GET /runs/{run_id}/mv-ddl (DDL artifact from genie_opt_artifacts + GRANT
  remediation), POST /mv/proposals/{id}/decision (approve sets
  approved_for_rerun; reject writes suppression), POST /mv/created/{id}/drop
  (OBO, confirm token, refuses unless status=DETACHED),
  POST /mv/probe (from Prompt 5).
- OpenAPI schemas; route tests with the repo's OBO fixture pattern, covering
  coercion-to-suggest_only, create-failure fallthrough, and 403 on drop by a
  non-owner.
```

### Prompt 10 — Mockups first (review checkpoint)

```
Before implementing UI, generate static mockups I can review, extending the
repo's existing components and styles (the run-config panel is
frontend/src/components/auto-optimize/OptimizationConfig.tsx; match its
patterns — local useState, no store):

1. Run-config panel expanded state, first run: "Suggest metric views" toggle ->
   explanatory copy -> mode line showing "Create and attach" DISABLED with the
   MV-D1 rationale: "Available after this run produces proposals you approve."
2. Run-config panel, re-run state: approved proposals listed with checkboxes ->
   target catalog.schema (read from the consent/proposal) -> inline probe
   result (granted state per POV 7.3 copy) -> "Create and attach, then
   optimize" enabled -> separate "Also materialize" checkbox with cost warning.
3. Same panel, denial state: warning banner, [Copy grant request]
   [Choose a different schema] [Continue in suggest-only mode].
4. Output screen, suggest-only: proposals list with confidence badges, DDL
   panel (prism-react-renderer, already in package.json) with copy button,
   GRANT panel, the mandatory "Lift not measured" label, [Approve for re-run]
   and [Re-run with this metric view].
5. Output screen, create_and_attach: per-proposal card with created object,
   baseline vs post-attach accuracy (both eval_run_ids linked), tables_freed,
   regression state with DETACHED badge and the explicit [Drop view] action.
6. Semantic model visualization (see Prompt 12) — one representative frame.

Deliver as Storybook stories if the repo has Storybook, otherwise as vitest-
renderable components plus a static HTML export per screen under
docs/design/mockups/. Use the exact copy from POV 7.3 and 7.5 as amended by
the deltas appendix. No backend wiring in this prompt.
```

**Your job after Prompt 10:** review the mockups, mark up changes, and paste your corrections as a short follow-up prompt before proceeding. Cheaper to move pixels here than after wiring.

### Prompt 11 — Run-config consent panel (implementation)

```
Implement mockups 1–3 for real in
frontend/src/components/auto-optimize/OptimizationConfig.tsx and the payload
builder frontend/src/components/auto-optimize/optimizationRequest.ts:

- Toggle off by default. First-run state: "Create and attach" disabled with
  the "available after this run produces proposals you approve" copy (MV-D1).
- Re-run state (approved proposals exist for the space, fetched from
  GET /runs/{run_id}/mv-proposals of the prior run): proposal checkboxes,
  target read from the proposal/consent, probe call to
  POST /api/auto-optimize/mv/probe on expand; granted/denied states per the
  mockups; "Create and attach" enabled only on verdict SUFFICIENT;
  [Copy grant request] copies remediation_sql.
- Start records the mv_* fields + consent object into the TriggerRequest via
  optimizationRequest.ts. "Suggest only" sends mode=suggest_only, no consent.
- Materialize checkbox independent, default off, with the cost note.
- Component tests (vitest, matching existing frontend tests): probe
  loading/granted/denied, first-run vs re-run gating, consent payload shape,
  and that toggling off clears every mv_* field from the request.
```

### Prompt 12 — Semantic model visualization

```
Implement the semantic model view used in both the proposal review and the
output screen. Use react-force-graph-2d (already in package.json at 1.29.1) for
the graph, react-diff-viewer-continued for the space-config diff mode, and
prism-react-renderer for any inline SQL. Do not add a new graph dependency.

Graph spec:
- Nodes: proposed metric view (distinct styling), its source table, join tables,
  measures, dimensions, and the Genie Space's current raw tables.
- Edges: source->MV, join tables->MV labeled with the ON predicate, measure/
  dimension->MV membership, and dashed "replaces" edges from raw space tables
  the MV would cover (drives the tables_freed story).
- Node detail panel on click: for a measure, the expr, synonyms, format, and
  the evidence (recurrence count, contributing benchmark question ids); for a
  join, cardinality if known.
- Diff mode toggle: current space data sources vs post-attach state.
- Data comes from the proposal payload + serialized_space via existing
  endpoints; add a thin GET .../mv-proposals/{id}/graph endpoint if needed that
  returns nodes/edges JSON (schema in the OpenAPI spec).
- Render-level tests with a fixture proposal; a Storybook story per state
  (single MV, multi-join MV, conflict state).
```

### Prompt 13 — Output screen panels

```
Implement mockups 3–4 in the run output/results screen:

- Suggest-only panel per POV 7.5: DDL with copy (from GET /runs/{run_id}/mv-ddl),
  GRANT with copy, space-config diff (react-diff-viewer-continued) against
  current data_sources.metric_views[], evidence block, the verbatim "Lift not
  measured — this metric view was not created or attached during this run."
  label, [Approve for re-run] (POST /mv/proposals/{id}/decision), and
  [Re-run with this metric view] which opens the run config pre-filled in
  create_and_attach mode with that proposal checked (MV-D1 flow).
- Create-and-attach panel: created object name linking to Catalog Explorer,
  baseline vs post-attach accuracy with both eval_run_ids as links, needs-review
  count shown separately (never folded into accuracy), tables_freed,
  regression -> DETACHED badge + [Drop view] flow (confirm dialog quoting the
  "other consumers may depend on it" warning), downgrade_reason banner when the
  run degraded, a join-strategy badge (denormalized / nested / subquery-source)
  with its cardinality evidence, and a "Grant access" panel rendering the
  copy-ready GRANT SELECT checklist with the note that other users' Genie
  answers silently degrade without it.
- Wire to the Prompt 9 endpoints; component tests for every state incl.
  downgraded-run and detached-with-drop.
```

---

## Phase 4 — Testing end to end

### Prompt 14 — Test hardening pass

```
Audit test coverage across everything added on this branch and close gaps:

- Unit: fingerprint collapse/near-miss suite; both scoring worked examples;
  decay math; dedup/CONFLICT states; probe permission matrix; consent verify
  downgrade; DDL artifact golden file; firewall literal-leak assertions.
- Contract: eval adapter against mocked responses for every status and
  assessment reason; pagination; timeout.
- Job: the parameter lockstep and DAG-invariant tests from Prompt 8; a dry-run
  harness that executes the optimize-task phases in-process with fakes to
  verify the Delta-by-run_id handoff end to end (trigger-created object rows ->
  attach phase -> mv_lift phase -> advisor phase -> proposal rows the next
  trigger reads).
- API: route tests incl. auth failures and coercion.
- UI: component tests + stories for all states.
Additionally: the recon found the ONLY GitHub Actions workflow is docs deploy —
there is no test CI. Add a test workflow running the backend pytest suite,
the GSO package pytest suite (pythonpath=["src"]), and frontend vitest on PRs,
so this branch's tests actually gate the merge. Keep it minimal and match the
existing workflow's conventions.
Report a coverage summary and list anything intentionally untested with why.
```

### Prompt 15 — E2E playbook against the dev workspace

```
Write scripts/e2e/mv_advisor_e2e.md plus a runnable pytest -m e2e suite
(env-gated on DATABRICKS_HOST/TOKEN + config for space_id and scratch schema)
that executes the three scenarios end to end against my dev workspace:

Scenario A — suggest_only:
  Start a run with the toggle on, mode suggest_only. Assert: run completes;
  optimization tasks unaffected; >=1 candidate persisted; DDL artifact exists,
  parses with sqlglot, and contains no literals; UI endpoints return the DDL
  and GRANT; created_metric_views is empty.

Scenario B — denied permission:
  Run as the low-privilege test identity requesting create_and_attach on the
  scratch schema. Assert: probe verdict INSUFFICIENT with the right missing
  privilege; run auto-downgrades (effective_mode=suggest_only, downgrade_reason
  set); NO UC object created; optimization still completes.

Scenario C — approve, re-run, create_and_attach with lift (two runs, per MV-D1):
  Run 1 in suggest_only produces proposals; approve one via the decision
  endpoint; trigger run 2 in create_and_attach with consent. Assert: the MV was
  created by the BACKEND under the user's identity (check created_by/owner),
  in the consented schema only, BEFORE the job started; DESCRIBE EXTENDED on it
  shows Type: METRIC_VIEW; its YAML passes mv_yaml.validate (no transitive
  joins, structured comment, no benchmark-verbatim BEST FOR line); the
  semantic-validation query used MEASURE() syntax; the job attached it via
  the mv_attach patch (visible in serialized_space); the mv_lift eval run
  reached DONE; lift_report stored with both eval_run_ids on
  genie_opt_mv_created_objects; the unified loop ran after attach; audit rows
  in genie_opt_mv_consents/_created_objects answer "who created this and why".
  Teardown: detach + drop the scratch MV, delete scratch objects. Respect the
  ~20 q/min ceiling: subset eval runs, serialized, suite marked slow.

Also include a manual smoke checklist for the UI (10 items max) covering the
consent panel, denial banner, output panels, and the semantic model graph.
```

### Prompt 16 — Docs, changelog, PR

```
Finish the branch:

- Update the repo docs (per its docs structure) with: feature overview, the
  consent model, mode table, how to enable, and screenshots exported from the
  Storybook/mockup states.
- Add an entry to the changelog/release notes per repo convention.
- Update docs/design/metric-view-suggestion-engine-pov.md status flags for
  anything the implementation resolved or contradicted, with a short
  "implementation deltas" appendix.
- Write the PR description: problem, design link, screenshots, the three E2E
  scenarios and their results, rollout guidance (feature defaults to off;
  suggest_only is the safe first mode), and explicit reviewer callouts:
  (1) the CREATE TABLE privilege assumption for metric views needs runtime
  verification, (2) the eval APIs are Beta and isolated behind the adapter,
  (3) detach-never-drop semantics.
- Run the full test suite and linters; fix anything red; list remaining known
  issues honestly in the PR.
```

---

## Optional follow-on branch

### Prompt 17 — Discover curator (Part 8), separate branch

```
On a NEW branch feature/discover-curator off the merged advisor branch,
implement POV Part 8 phase 1 only: the discover_gate + discover_curator task
(propose-only), domain/subdomain proposal scoring per POV 8.3, the membership
diff against system.information_schema.table_tags, the ALTER ... SET TAGS apply
plan generator, the blocking firewall scan, and the copy-ready manual-steps
checklist for UI-only domain/Page creation. No tag writes in this phase.
Reuse the advisor's consent-gate pattern for the eventual tag-apply mode.
```

---

## Working rhythm

- **One prompt, one commit, review the diff before the next prompt.** Cursor drift compounds; the gap report and rules file are your rails, but your review is the brake.
- **Every prompt runs two-phase.** Read the PLAN before saying "proceed." The moment a PLAN quotes code you don't recognize, or names a symbol without a file:line, stop — that's the stale-context signal, and it's cheaper to catch there than in the diff.
- **The gap report is a living document, not a one-time artifact.** When any PLAN phase reveals the repo has changed relative to the gap report (a renamed task, a moved module, a refactored router), update `mv-advisor-gap-report.md` in the same commit as the code change. A stale gap report silently re-poisons every subsequent prompt that references it.
- **Drift check against main every few working days:** rebase the branch, and if `git diff --stat main...` on the areas the gap report covers (job YAML, optimizer package, routers, UI components) shows upstream movement, re-run Prompt 0 in diff mode — "re-verify only the sections of the gap report touching these changed paths" — before continuing. Genie Workbench moves quickly; a two-week-old recon of an active repo is a liability.
- **When Cursor reports a doc-vs-repo conflict, resolve it in the doc first**, commit, then re-run the prompt. Never let the two diverge silently.
- **Prompts 0, 10, and 15 have human checkpoints built in** — recon review, mockup review, E2E sign-off. Those three are where the feature is actually decided; everything else is typing.
