# Cursor Prompt Playbook — Building the Metric View Advisor in Genie Workbench

*A sequenced set of Cursor prompts that take the POV document from design to a tested, PR-ready feature branch in `databricks-solutions/databricks-genie-workbench`. One prompt ≈ one reviewable commit. Run them in order; each prompt's output is the next prompt's input.*

> **Custody.** From this commit forward, this in-repo copy is the canonical playbook and the canonical MV-D register. Any external copy is downstream: reconcile it to this file, never the reverse.

---

## Before you start (manual steps, 10 minutes)

1. **Commit the design doc AND this playbook into the repo** so Cursor can reference both in every prompt. The playbook is the defining source of the MV-D decision numbering (MV-D1–MV-D15 today, appended to as later prompts take architecture calls); if it is not in the repo, agents cannot resolve the citations and will (correctly) refuse to stamp them:
   ```bash
   git checkout main && git pull
   git checkout -b feature/metric-view-advisor
   mkdir -p docs/design
   cp metric-view-suggestion-engine-pov.md docs/design/
   cp cursor-prompt-playbook-mv-advisor.md docs/design/mv-advisor-playbook.md
   git add docs/design && git commit -m "docs: metric view advisor design POV + build playbook"
   ```
   Note on decision namespaces: this repo already carries a GSO v2 playbook numbering (D1–D9, of which D9 = no task values) and a separate D1–D3 inside `applier.py`. This playbook's decisions are therefore cited as **MV-D1…MV-D10**, and any reference to the others must be namespace-qualified ("GSO v2 D9", "applier.py D2").
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
   - RULES COPIES: exactly two exist — .cursor/rules/mv-advisor.mdc in this
     repo and the fenced block in docs/design/mv-advisor-playbook.md — and they
     are kept byte-identical. Any third copy found outside the repository (a
     workspace-root .cursor/, another checkout) is stale by definition: report
     it, do not read it as guidance.

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
   - LIVE CLAIMS vs FROZEN TRANSCRIPTS: prompt bodies for already-executed
     prompts are historical records — do not retrofit them when facts change.
     Statements outside those bodies (headings, register preamble, custody
     notes, rules, gap-report quotes) are live claims and must be refreshed
     under MV-D9. When in doubt, ask rather than edit a transcript.
   - CITE DOCUMENTS BY SECTION, CODE BY FILE:LINE. Never cite another design
     document by line number — doc line anchors rot on every edit and each
     rot costs a reconciliation pass. Use section headings or quoted phrases.
     file:line into source code is required and unchanged.
   - DOC FREEZE: no further doc-only commits on this branch. Playbook, POV,
     and gap-report changes ride inside the code commit that made them
     necessary (MV-D9 already requires same-commit gap-report refresh).
     Exception: a factual error that would misdirect the next prompt.
   - METRIC VIEW BEST PRACTICES: before authoring or reviewing metric view YAML,
     joins, level-of-detail semantics, validation queries, or Genie-facing MV
     metadata, read the skills under
     /Users/prashanth.subrahmanyam/Projects/vibe-coding-workshop-template/data_product_accelerator/skills/semantic-layer/01-metric-views-patterns
     — SKILL.md plus references/ (yaml-reference, advanced-patterns,
     composability-patterns, level-of-detail, querying-metric-views,
     validation-checklist, validation-queries, implementation-workflow) and
     scripts/validate_metric_view.py. This is a standing NAMED EXCEPTION to the
     in-repository context rule above, and the only one. It is external,
     machine-local guidance: it informs generated YAML and validation but never
     outranks repo reality, the gap report, or MV-D8 below. If the path is not
     present on this machine, proceed without it — do not block, and do not
     substitute a different checkout.
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
   - PINNED-DEPENDENCY RESOLUTION: for any behavior that depends on a pinned
     library version (sqlglot parsing and rendering above all), run only under
     `uv run --frozen`. A bare pytest on an ambient interpreter is a void run:
     the pyenv global carries sqlglot 30.0.1 against the pinned 30.0.3, and
     canonicalization output can differ across parser patch releases. Report
     the resolved version in VERIFY alongside the import path.
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

## Decisions register (MV-D1–MV-D15)

The recon surfaced five structural conflicts, not naming drift. These decisions resolve them and are baked into the revised prompts below. MV-D1 changes the user-facing flow and needs explicit sign-off. MV-D7 was added during Prompt 1 execution, MV-D8 with the generation quality standard, MV-D9 from the Prompt 2 readiness check, MV-D10 during Prompt 3 execution, MV-D11 and MV-D12 during Prompt 4 execution, MV-D13 during Prompt 5 execution, MV-D14 during Prompt 5.5 execution, and MV-D15 during Prompt 6 execution; later decisions append here — this register is the defining namespace, and the playbook copy committed at docs/design/mv-advisor-playbook.md must be refreshed whenever it changes.

**MV-D1 — Two-run consent model (the big one).** The job launches as the service principal (`integration/trigger.py` → `backend/job_launcher.py`), and the no-SP-writes rule stands. So the job cannot run `CREATE VIEW … WITH METRICS` under the user's identity, and there is no supported way to run the job as the requesting user per-run. Resolution: **creation moves to the backend, at trigger time, under OBO — which means create_and_attach applies to already-approved proposals.** The flow becomes: run N (any mode) produces proposals → user reviews and approves → **[Re-run with this metric view]** → backend re-probes entitlement, creates the approved MV under OBO, passes its identifier as a job parameter → run N+1 attaches it via patch, measures lift, and optimizes on top. A *first* run for a given proposal is always suggest-only, because the proposal does not exist until the advisor has seen the baseline SQL. Rejected alternatives: passing an OBO token as a job parameter (a credential in run metadata), and SP-created views (ownership lands on the app identity and violates the design's own rule). The consent-panel copy in Prompt 10 changes accordingly: "Create and attach" is enabled only when approved proposals exist for the space.

**MV-D2 — Attach is a real patch type, and it fixes an existing no-op.** New entries in `PATCH_TYPES` (e.g. `mv_attach_data_source`, plus optional raw-table removal) with genuine applier actions that mutate `data_sources.metric_views`, added to the unified-loop allowlist. The recon found Lever-2 MV patches currently render and then no-op at config level (`applier.py:3914-3932`) — this feature ships the missing applier action rather than working around it.

**MV-D3 — In-job work is phases inside the optimize task, not new tasks.** Sequence inside `run_optimize`: iteration-0 baseline eval (already exists) → *mv_attach phase* (only when an approved-view identifier arrived as a job parameter) → *mv-lift eval* (a labeled eval run via the EvalRunner seam, isolating the metric view's delta before the loop moves anything else) → unified loop → *mv_advisor phase* persisting proposals for the next run. Each phase is try/except-isolated with a Delta status row; handoff is Delta-by-run_id per GSO v2 D9.

**MV-D4 — The rules file and POV are amended, not the tests.** `test_phase7_job_dag.py` encodes deliberate decisions (no condition tasks, no task values, the four-task collapse). The feature conforms; Prompt 0.5 records the deltas in the POV.

**MV-D5 — Four-place lockstep for job parameters.** Root `databricks.yml`, the package-bundle mirror, `scripts/deploy_lib/gso_job.py`, and `backend/job_launcher.py` must all carry every new `mv_*` parameter. The gap report should state this explicitly so no prompt touches one mirror without the others.

**MV-D6 — Rollback simplifies; drop stays manual.** Whole-snapshot revert means detach-on-regression comes free with the existing `integration/revert.py` path. The never-auto-drop rule is unchanged; drop is a backend OBO endpoint gated on DETACHED status.

**MV-D7 — Stateful MV entities get dedicated tables; run-scoped blobs stay in `genie_opt_artifacts`.** Three tables join `_ALL_DDL`: `genie_opt_mv_candidates` (partitioned by target_space_id — the upsert key is space-scoped and a candidate outlives the run that proposed it under MV-D1), `genie_opt_mv_consents` (unpartitioned — run_id is NULL at probe time and filled at trigger), and `genie_opt_mv_created_objects` (partitioned by run_id; merge key run_id + suggestion_id). The rendered DDL text remains a `genie_opt_artifacts` row under a new artifact_kind, with `content_hash` set to the dedup fingerprint so the stores cross-reference. *(True as of Prompt 6, and it was not before: `write_artifact` computed `content_hash` from the payload bytes and had no parameter to override it, so this sentence and the matching comment on `genie_opt_mv_candidates.dedup_fingerprint` described a cross-reference that did not exist. Prompt 6 adds `content_hash` as a keyword-with-default and the advisor passes the fingerprint. A content hash could not have served the purpose anyway — MV-D15 requires Prompt 9 to regenerate the YAML under the backend's probe capabilities, so the same candidate's text legitimately differs between the two stores while its identity does not.)* Rationale: all three entities carry mutable, cross-run-lifecycle state that a run-partitioned append-style handoff table cannot host without fighting its own partitioning. Supersedes POV Appendix A Deltas 4 and 6 where they assert otherwise. Process rule established alongside it: **appendices record decisions; they do not make them** — any new architecture call surfaces here as an MV-D entry first, then the appendix cites it.

*As implemented (Prompt 1):* accessors live in `optimization/mv_state.py`, one module rather than an extension of the already-large `optimization/state.py`, following the `scan_snapshots.py` precedent for feature-scoped persistence. Every writer goes through a new `delta_helpers.merge_row` primitive, so the §7.9 idempotency key `sha256(space_id | canonical_measure_expr | sorted_source_set)` is enforced as a single-statement upsert-on-conflict rather than a read-then-insert a retry can duplicate. JSON payload columns carry a `_json` suffix in storage and travel base64-encoded; the accessor signatures use the POV Part 4 field names (`score_components`, `evidence`, `provenance`, `alternatives`, `conflicts`, `probe_results`) verbatim. POV Part 4's proposal `type` is stored as `candidate_type` to avoid shadowing the builtin at the API. `reverified_at_trigger` is a `TIMESTAMP` (NULL = never re-verified), and `updated_at` sits on all three tables. `lift_report_json` is deferred to an `ADDITIVE_COLUMN_MIGRATIONS` entry in Prompt 7, when the lift report exists to store. Three judgment items raised in the Prompt 1 VERIFY were reviewed and approved in the Prompt 1 follow-up and are now settled: decision-column ownership (`upsert_mv_candidate` never writes them; `record_mv_candidate_decision` owns them, pinned by `test_re_proposing_does_not_resurrect_a_rejected_candidate`), the "Resolved by MV-D7" pointers added to the gap report, and the user-facing table rows in `docs/docs/features/auto-optimize.md` with their empty-table-is-not-a-failed-run framing.

*As implemented (Prompt 2) — `lift_report_json` contract:* Prompt 7 persists `LiftReport.to_dict()` verbatim into `genie_opt_mv_created_objects.lift_report_json`. The shape is frozen at `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py` (`LiftReport` fields at `:177-190`, `to_dict` at `:192-193`). Fourteen keys, 0–1 accuracy/delta fractions, regressions = GOOD→BAD excluding needs-review: `delta_affected`, `delta_suite`, `regressed_question_ids`, `needs_review_count`, `pre_eval_run_id`, `post_eval_run_id`, `question_subset`, `pre_accuracy_affected`, `post_accuracy_affected`, `pre_accuracy_suite`, `post_accuracy_suite`, `needs_review_question_ids`, `graded_affected_count`, `graded_suite_count`. Any later reshape needs an `ADDITIVE_COLUMN_MIGRATIONS` entry, not an in-place rewrite of this dict.

**MV-D8 — Generation quality standard adopted (metric-views-patterns v5.2).** All engine-emitted YAML is rendered and validated by one module (`mv_yaml`, Prompt 5.5) enforcing: v1.1 unsupported-field and format-type lints; the multi-hop decision ladder (denormalize → nested joins on DBR ≥ 17.1 with profiling-proven 1:1 keys → subquery-source fallback) with a hard transitive-join gate; SCD2 `is_current` guards; `rely.at_most_one_match` only on proven uniqueness; `MEASURE()`-composed derived metrics, `FILTER`-clause conditional counts, and Fixed-LOD percent-of-total (never `MEASURE()/MEASURE()`); structured comments with **paraphrased** BEST FOR lines (verbatim benchmark text is a firewall violation — it contaminates the benchmark the view is graded by); `ALTER VIEW` for all updates (grants-preserving); post-create `DESCRIBE EXTENDED` type assertion; `MEASURE()`-syntax validation queries with a fan-out smoke test; a copy-ready `GRANT SELECT` checklist surfaced, never auto-applied; and capability rows (DBR 17.3/17.1/18.1 floors) in the entitlement probe that downgrade the join strategy rather than emit unplannable YAML. Origin: the metric-views-patterns skill (v5.2, 2026-06-06); the sample YAML in POV Part 4 was itself corrected under this standard (its customer join was transitive).

*As implemented (Prompt 5) — the probe's two answer kinds, and where UNKNOWN goes.* `backend/services/mv_entitlement.py` (`probe`, `record_consent`, `verify`) behind `POST /api/auto-optimize/mv/probe`. Privileges come from `grants.get_effective(..., principal=<user>)` under `require_obo_workspace_client`, one read per securable, so a privilege held through a group counts; the space check is a new public `user_can_manage_space` in `common/genie_client.py` wrapping the previously-unused `_check_user_manage_from_rest_acl`, because CAN_EDIT does not authorize a `data_sources.metric_views` rewrite. Capability rows come from `current_version()` and follow **MV-D13**, registered in this commit: floors and the two-directional `UNKNOWN` contract live in `common/config.py` (`MV_CAPABILITY_FLOORS`, `MV_OPTIONAL_CAPABILITIES`), and each row carries `observed_warehouse_id` so `verify()` can refuse a compute swap. A NOT_FOUND securable is DENIED, not UNKNOWN: UC returns the same error for absent and for invisible, and either way the create cannot proceed — the detail line keeps the distinction readable for a human without pretending the probe resolved it. Consent is written by a new `wh_upsert_mv_consent` / `wh_load_mv_consent` pair in `common/warehouse.py` — the Statement-Execution twins of the `mv_state` accessors, because the backend has no SparkSession — and persistence is best-effort: a warehouse hiccup returns the probe with `consent_recorded=false` rather than losing it behind a 500. That reachability is exactly why **a missing consent row is a downgrade, never an absence that waves the write through** (`test_verify_treats_a_missing_consent_row_as_a_downgrade`). Cross-reference: MV-D7 for the table, MV-D8 for the floors, MV-D13 for the `UNKNOWN` contract. Prompt 5's playbook body did not specify capability rows and POV §7.3.1's sample JSON omitted them while its prose required them; both were corrected in this commit under the DOC FREEZE factual-error exception, since Prompt 5.5 reads these rows to choose a join strategy.

**MV-D9 — The gap report is refreshed in the same commit that invalidates it.** Commit 1 added three tables to `_ALL_DDL` and left four gap-report sites — the §1.6 Persistence heading and its `_ALL_DDL` quote block, the §2.6 Evaluation verdict row, and the Appendix summary row — asserting six tables. Because the rules make the gap report outrank the POV, and the POV's Delta 6 had already been retitled "Nine Delta tables," an agent reading both was told the authoritative source says six and the POV's nine is the error — exactly backwards. Rule: any commit that changes something the gap report quotes or counts refreshes those sites in the same commit, re-quoting live code with current line anchors. Enforced by a rules-file bullet and by every PLAN naming the gap-report sites it will refresh. Keyword search is insufficient enforcement: a quote can go stale by *position* with its content unchanged, invisible to any grep for stale wording, so VERIFY must byte-match every fenced reference rather than search for stale words.

*Mechanised after Prompt 5.5, and the rule is unchanged.* Four of nine line counts drifted inside one prompt, which is a tax every later prompt would keep paying, so the arithmetic moved off the human: `scripts/gap_report_counts.py` fills live counts into a curated template (`--write`) and fails on a stale report (`--check`), the package-layout block is now generated output between markers, and `test_gap_report_counts.py` asserts both the block and every `(N L)` claim outside it. What is **not** delegated is the part that requires reading: byte-matching fenced quotes and re-checking anchors stays manual, for the reason the paragraph below gives — a quote goes stale by position with its content unchanged, and no count check can see that. The curation stays editorial too; the template decides which modules are worth naming, and the script only supplies numbers.

*Empirical record, as of Prompt 4.* Byte-match VERIFY has now caught **four** anchor defects that no keyword search could have detected: the §2.7 S row's `leakage.py:132-137` (pointing at the `BenchmarkCorpus` shingle block instead of the embedding path), `config.py`'s stale line count, and the two fenced quotes at `leakage.py:279-289` and `:284-289` (correct content, positions shifted seven lines). Two of the four — both `leakage.py` fences — quote `_PATCH_TEXT_FIELDS` and `_EXAMPLE_SQL_PATCH_TYPES`, the exact dict Prompt 5.5 must add an MV patch type to, so they would have misdirected that prompt specifically. Note also that three of the four were stale *by position with content unchanged*, and `leakage.py` is untouched on this branch: they were wrong when written rather than rotted by a commit, which is a defect class a freshness discipline keyed on "what did this commit change" cannot find at all. Keep the byte-match in every VERIFY that touches a quoted surface, whether or not the commit appears to have invalidated anything.

**MV-D10 — Two fingerprint levels, permanently distinct.** The readable composite string in the POV Part 4 sample payload (`"sha256:sum(l_extendedprice*(1-l_discount))|group:order_status,market_segment"`) predated the shipped key and was never implemented. It is not a format to resurrect: despite the prefix it is not a sha256 digest, it omits the space id, and it keys on the grouping set rather than the source set. Corrected to the hex-digest form in the same commit as Prompt 3, under the DOC FREEZE factual-error exception. The two levels that do exist are:

- **`expr_fingerprint`** (`optimization/mv_fingerprint.py`) — *expression*-grained, `sha256` of the canonical expression text. It counts recurrence inside a corpus scan and does nothing else. It collides across spaces and across source sets by construction, so it is **never persisted as a dedup key** — pinned by `test_expr_fingerprint_appears_in_no_persistence_path`, which asserts no other module in the package even references it.
- **`mv_state.mv_candidate_fingerprint`** — *candidate*-grained, `sha256(space_id | canonical_measure_expr | sorted_source_set)`. The MV-D7 upsert key for `genie_opt_mv_candidates.dedup_fingerprint` **and** the `genie_opt_artifacts.content_hash` cross-reference. It is the only thing this feature calls a "dedup fingerprint," and it consumes `MeasureRef.canonical_expr` from the fingerprint module as its `canonical_measure_expr` argument, so canonicalization has exactly one implementation. Cross-reference: MV-D7.

Settled alongside it, from the same conflict: the new module's canonicalizer is `canonicalize_sql_ast` / `canonicalize_expr` rather than a reuse of `canonicalize_sql`, because `leakage.canonicalize_sql` already exists with the **inverted** literal contract — it *preserves* literals (verbatim benchmark text is the evidence it hunts) where the fingerprint module *erases* them (two queries differing only in a date are one shape). Both docstrings state the inversion; `leakage.py` behavior is untouched, and the firewall requirement is a property test on the new function.

*Canonicalization invariants established by Prompt 3 (do not re-derive these).* Prompt 4's dedup gate and Prompt 5.5's generator both consume canonical forms produced here, and a second implementation of any of these rules is how two components start disagreeing about what one measure is:

- **(a) Table qualifiers are STRIPPED from measure expressions, never renamed.** `SUM(l.l_extendedprice * (1 - l.l_discount))`, the `li.`-qualified spelling and the unqualified one all canonicalize to `sum(l_extendedprice * (?n - l_discount))`. Table identity is not lost — it travels in `MeasureRef.source_tables`, which is exactly what the MV-D7 key hashes as its sorted source set. Positional renaming (`t1`, `t2`) is correct for *whole statements* and wrong for measures: it would make the same measure fingerprint differently depending on the join order the query happened to use. Both levels therefore agree that one measure is one row.
- **(b) Date-part units fold into the function name.** `DATE_TRUNC('month', d)` canonicalizes to `timestamp_trunc_month(d)`. Erasing the unit with the literals would merge a monthly grain with a daily one — the silent-wrong-numbers class of defect; keeping it quoted would leave a token in the canonical form indistinguishable from data. Folding preserves the grain *and* yields the firewall invariant in its mechanically-auditable form: **no quote character survives canonicalization.** That is a grep, not a judgment call — any quote in a canonical form or a fingerprint input is a firewall violation, full stop. Pinned across the whole test corpus by `test_no_corpus_statement_leaks_a_quoted_literal`.
- **(c) Shape identity is the generator's TARGET form, not the corpus spelling.** `ShapeMatch.fingerprint` hashes `target_form` — what the generator will emit — so `SUM(CASE WHEN c THEN 1 END)` and `SUM(CASE WHEN c THEN 1 ELSE 0 END)` are one `CONDITIONAL_COUNT`. They ask for the same `COUNT(1) FILTER (WHERE c)` measure, and counting them apart would halve the recurrence of the exact thing being proposed. `SHAPE_GUIDANCE` holds the MV-D8 mandate per shape so the generator reads the rule rather than re-deriving it.

*And the deliberate bias:* where canonicalization cannot prove two expressions are the same shape, it emits two fingerprints rather than one. **Recurrence therefore under-counts, never over-counts.** Under-counting can delay a proposal; over-counting would fabricate one. Two consequences are tested as intended behavior rather than left implicit: expressions differing only in a literal value *do* collapse (the firewall demands it, so a generator must recover concrete predicate values from profiling — there is nothing left in a fingerprint to read them out of), and aggregates in `ORDER BY`/`GROUP BY` positions are references to a projection, not second measures.

**MV-D11 — Scoring: what Y multiplies, how Y and D normalize, and what gets persisted.** Taken during Prompt 4 execution, from four questions POV Part 3 leaves open or answers inconsistently.

- **(a) Y's equivalence flag is corpus-internal, and governed-MV equivalence gates instead of damping.** Prompt 4's own bullet said "normalized recurrence × AST-equivalence flag *against existing MV measures*". That is not implementable against POV Part 3's first worked example, which scores **Y = 0.95 for a measure with no MV equivalent** — an MV-matching flag would zero it and yield 51.5, not the 80.0 the example asserts. The flag therefore asserts that the counted occurrences genuinely collapse to one canonical form; a consumer that bucketed by anything looser passes `False` and Y is zero. Equivalence against governed metric views lives in the dedup gate, where POV Part 3's "Conflicting or partial matches" paragraph already put it. **Gating is strictly stronger than damping:** a measure that is already governed is not a weaker proposal, it is not a proposal. Prompt 4's Y bullet is corrected in place under the DOC FREEZE factual-error exception so no later prompt re-derives the wrong reading from the prompt text.
- **(b) Y's recurrence curve is log-saturating; D's factors combine as a geometric mean.** POV supplies component *values* in both worked examples and no curve for either, so the examples pin the blend, not the curves. Y is `min(1, log1p(r) / log1p(R_sat))` with `R_sat = 75`, which yields 0.9492 at the example's `r = 60` — the 0.95 it states, to two places — and gives diminishing returns so one pathological dashboard cannot saturate the signal alone. D saturates frequency, cost and breadth against their own ceilings and takes their **geometric mean**, not the literal product POV's prose ("frequency × cost × distinct users") suggests: three normalized factors multiplied collapse toward zero — `0.8³ = 0.512` — so a literal product **cannot reach the 0.80 that POV's own worked example asserts** for a busy measure. The geometric mean keeps D on the same 0-1 scale as L, Y and S, which is the only way the weights mean what they say. A zero on any factor still zeroes D. POV Part 3's D row now carries a one-line pointer here, since its body text is loose prose this register makes precise.
- **(c) Only PROPOSE and CONFLICT are persisted.** `CONFLICT` persists as `candidate_type` (that is what the enum is for) and is never a suggestion. Dedup-**blocked** and sub-25 **suppressed** candidates are returned to the caller for run reporting and not written: `MV_CANDIDATE_TYPES` has no state for either and `genie_opt_mv_candidates.tier` is documented `HIGH|MEDIUM|LOW`. No `genie_opt_artifacts` row is written at scoring time either — the rendered DDL text that POV Appendix A Delta 4 assigns to that table does not exist until Prompt 5.5. `suggestion_id` is `"sug_" + dedup_fingerprint[:12]`, derived rather than random so a re-proposing run produces the same id (POV's `"sug_9f2a"` is illustrative). **If Prompt 13's UI later needs blocked-candidate counts persisted, that is an `ADDITIVE_COLUMN_MIGRATIONS` change plus an enum extension at that time — not a reason to widen `MV_CANDIDATE_TYPES` now.**
- **(d) The MV-D10 guard test correctly blocks Prompt 4, and stays.** `test_expr_fingerprint_appears_in_no_persistence_path` is a substring scan over every package `.py` file, so it forbids even a prose mention of the name. Prompt 4 conformed — `mv_scoring.py` compares canonical expression *text* via `canonicalize_expr` and refers to "the expression-grained fingerprint in `mv_fingerprint`" — and the test was **not** weakened. Should a later prompt need a legitimate prose reference, the sanctioned fix is to narrow the scan to persistence modules **and** add a positive assertion that no call site feeds the expression-grained fingerprint into `upsert_mv_candidate`'s `dedup_fingerprint` kwarg — strictly stronger than the substring test it replaces. Deleting or loosening it without that replacement violates MV-D4.

*As implemented (Prompt 4).* `optimization/mv_scoring.py`, with weights and every normalization constant in `common/config.py:2423-2506` (a `_float_env` helper sits there rather than beside `_int_env` at `:15`, whose `max(1, value)` clamp is wrong for weights, and appending held every gap-report `config.py` line anchor stable). The module **queries nothing** — lineage overlap, recurrence and demand arrive as frozen input contracts and existing metric-view definitions as `MetricViewField` values flattened from `metric_view_catalog.detect_metric_views_via_catalog`, so the `DESCRIBE ... AS JSON` parsing stays where it already lives. The embedding client is an injected `Protocol`; the production adapter borrows `leakage.get_embedding` and **no other firewall symbol** (pinned by an AST assertion, not a substring scan), defaults to `databricks-gte-large-en`, and L2-normalizes in our code because GTE does not normalize its output while BGE does. The blend is deliberately **not** rounded: both worked examples land on exact IEEE doubles, and rounding to two places would drag a 24.999 across the suppression floor into LOW. Benchmark question text cannot reach a proposal structurally — `MetricViewCandidate` has no field for it, so `evidence.benchmark_questions` carries ids because there is nothing else to carry.

**MV-D12 — S has two reference kinds, because an MV-field-only reading retires a fifth of the blend.** Prompt 4 first implemented POV Part 3's S literally — "max cosine of intent text vs **MV field text**" — and reported the Part 4 payload's `{"field": null, "cosine": 0.40}` as an unresolvable inconsistency: a space with no metric views has no field text, so S is structurally 0.0. That reading is wrong, and the payload was the evidence. Consequences of the literal reading, which is why it cannot stand:

- **S = 0.0 for every `NEW_METRIC_VIEW` candidate** — the class the engine exists to produce. Twenty percent of the blend becomes dead weight exactly where the feature earns its keep.
- **Those candidates cap at 80** against thresholds calibrated for 100. HIGH stays reachable only because 0.35 + 0.30 + 0.15 = 0.80 clears 75 with nothing to spare; any future weight retune or threshold tightening makes the top tier structurally unreachable for the primary output. Systematically depressing the highest-value output is a worse defect than a null field in a sample payload.
- **POV Part 3's own first worked example asserts S = 0.40 for a measure with no MV equivalent.** Under the literal reading that number is unreachable, so the document already contained the refutation.

Resolved with the symmetry L already has (MV-D11's `reference_kind`): S takes a reference set plus a recorded kind.

- **`GOVERNED_MV_FIELDS`** — preferred for `REPLACE_RAW_TABLE` and `ADD_MEASURE`, which act on an existing view. Max cosine of intent text against MV field text (display name, comment, synonyms).
- **`SOURCE_COLUMN_METADATA`** — preferred for `NEW_METRIC_VIEW`, where no view exists yet. Max cosine of intent text against the candidate's source column names and comments.
- **Neither available → 0.0 with a null field**, reported honestly rather than imputed.

Preference is keyed on candidate type rather than availability because the question S answers differs between the two; where the preferred set is empty the other is used and **the kind recorded is always the one actually compared**, never the one wanted. `SOURCE_COLUMN_METADATA` is knowingly the weaker reference — a column comment describes a column, not a business measure — and that is acceptable because it is the only semantic evidence that exists before a metric view does, and the alternative is a structurally unreachable score band. Recording the kind is what keeps the weakness visible: 0.40 against a curated field and 0.40 against a column comment are different strengths of evidence, and a payload reporting only the number cannot be reviewed. POV Part 4's payload and Part 3's S row are corrected accordingly under the DOC FREEZE factual exception. **Neither pinned worked example changes** — both supply component values directly, so 80.0 and 58.75 still hold.

*As implemented (Prompt 6) — the defect re-entered through the PRODUCER, and the scorer test passed the whole time.* MV-D12 was resolved in the scorer: `semantic_reference_for` prefers `SOURCE_COLUMN_METADATA` for `NEW_METRIC_VIEW` and `test_a_new_metric_view_candidate_scores_s_against_its_source_columns` proved it. When Prompt 6 built the first real producer of candidates, `candidate_from_measure` left `source_column_metadata` empty, so `semantic_reference_for` correctly found an empty reference set, `semantic_score` correctly reported `EMPTY`, and every `NEW_METRIC_VIEW` candidate in the real pipeline had S at 0.0 — the exact defect MV-D12 rejected, reached by a different route. Every MV-D12 test stayed green throughout, because none of them touched the producer.

The general lesson, which is not specific to S: **a scorer test proves the consumer, not that anything feeds it.** A resolution expressed as "the scorer prefers X when X is available" is only half a contract; the other half is "the producer makes X available," and that half has no natural home in a scorer test file. The same shape is the standing risk for the `PROVEN`-fixture rung in `mv_yaml` — the fixture proves the consumer of `EXACT` uniqueness evidence, and the probe that will produce it does not exist yet.

The standing assertion this implies, which now applies to every future producer: **for a candidate whose source columns exist in the wide-schema inventory, S's reference set must be NON-EMPTY** — not merely handled correctly when empty. Handling an empty set gracefully is necessary and is separately tested; it is not evidence that the set is ever populated. Pinned at both ends: `test_the_candidate_carries_a_semantic_reference_set` on the producer in isolation, and `test_a_candidate_whose_columns_are_in_the_inventory_has_a_non_empty_reference_set` end-to-end through `run_mv_advisor_phase`, asserting the persisted proposal's `semantic_top_match` reports `reference_kind: SOURCE_COLUMN_METADATA` with status `COMPUTED` rather than the `NONE`/`EMPTY` pair an unfed producer yields.

Both halves of that were demonstrated by reintroducing the defect rather than argued: with `_source_column_metadata` stubbed back to `()`, the end-to-end pin fails with `reference_kind: NONE`, `status: EMPTY`, `field: None`, while the thirteen MV-D12 scorer tests stay green — they contain no reference to the producer at all, so no addition to that file could have caught this. **And `evidence_coverage` is 0.50 in both the healthy and the defective case**, because `EMPTY` keeps its weight under MV-D15: the coverage figure is *by design* blind to a producer that never ran versus one that ran empty, so it cannot substitute for this assertion. That is the cost of MV-D15's `EMPTY`-keeps-weight rule, and it is the right trade — but it means an unfed producer is invisible in every persisted field except the reference kind.

*One correction to the record.* The prompt authorizing this decision quoted POV Part 3 as reading "S = 0.40 (no MV text to match yet, only weak column-name signal)". That parenthetical is **not in the document** — Part 3 says only "L = 0.90, Y = 0.95 (identical canonical fingerprint seen 60×), S = 0.40, D = 0.80". The reasoning stands without it, and on stronger ground: the bare `S = 0.40` for a candidate with no MV equivalent is itself the proof that S was never meant to be structurally zero. But the "weak column-name signal" phrasing is this decision's own resolution, not a recovered authorial intent, and must not be re-cited as a POV quotation.

**MV-D13 — Capability rows under a SQL warehouse: `UNKNOWN` resolves in two directions.** Taken during Prompt 5 execution. MV-D8 states the capability floors as Databricks Runtime versions because that is how the metric view documentation states them, and they are only *decidable* where the compute reports one. On a SQL warehouse `current_version().dbr_version` is NULL — only `dbsql_version` is populated, and Databricks publishes no DBSQL-to-DBR mapping — so the probe reports `UNKNOWN` rather than converting through a mapping the platform does not document. Under MV-D1 the create path is the FastAPI backend, whose only SQL compute is a warehouse, so `UNKNOWN` is the **normal** case and not an edge one. It therefore has to mean two different things, and the two directions are not symmetric:

- **Optional capabilities (`mv_nested_joins`, `mv_fields_agg_window_offset`) → unavailable.** The generator steps down the ladder rather than emitting YAML the runtime may not be able to plan. Emitting an unvalidated construct is the silent-wrong-answer class MV-D8 exists to prevent; withholding a feature costs verbosity.
- **`mv_create_edit` → never blocks authorization.** No `GRANT` can satisfy a runtime floor, so treating its `UNKNOWN` as a denial would deny every warehouse-only user — which under MV-D1 is all of them. Only the write itself can prove that floor, and a failed create is already handled as a downgrade. A *decided* `DENIED` (an actual cluster below 17.3) does yield `INSUFFICIENT`, and it carries **no** remediation SQL: the fix is a runtime upgrade, and a fabricated GRANT would send the user chasing the wrong thing.

**The consequence, stated plainly: on a SQL warehouse the nested-join rung is unreachable, so every multi-hop candidate lands on rung 3 (subquery-`source`).** That output is correct on all runtimes and more verbose than necessary on a runtime that would have supported rung 2. That is the intended trade: verbose-but-correct over concise-but-unvalidated.

*Revisit triggers* — either would make rung 2 reachable again, and neither has happened: Databricks publishes a DBSQL↔DBR mapping, or the create path moves to a DBR cluster (which MV-D1 does not currently allow). **An operator override forcing rung 2 is explicitly rejected.** A flag that says "assume ≥17.1" reintroduces exactly the unvalidated claim `UNKNOWN` exists to prevent, and it fails in the direction that produces confidently wrong numbers rather than verbose ones.

*Binding on Prompt 5.5:* `mv_yaml.validate` MUST read the flat `results` map from the probe and **step down on `UNKNOWN`**. An absent row is never permissive — a missing capability key means the probe could not speak to it, which is the same instruction to step down. Capability rows also carry `observed_warehouse_id`, because a capability is a property of the compute and not of the user: `verify()` downgrades when trigger-time re-verification names a different warehouse, since a capability observed on warehouse A says nothing about a create executed on warehouse B.

**MV-D14 — Uniqueness is a claim with provenance, and only an exact count is proof.** Taken during Prompt 5.5 execution. MV-D8 gates two constructs on a key being "proven 1:1": `rely.at_most_one_match: true` and the nested-join rung. The gate had no defined evidence standard, and the profiling GSO already computes cannot meet the obvious reading of it — `_data_profile` and `_join_overlaps` sample, so a sampled `COUNT(DISTINCT k) = COUNT(*)` says the duplicate rows were not in the sample and nothing more. Both constructs fail in the expensive direction when the claim is wrong: `at_most_one_match` is **unvalidated at runtime**, so a false claim silently inflates every `SUM` and `COUNT` through fan-out rather than erroring. So `MvProfiling` carries uniqueness as a `KeyUniqueness` with an explicit `kind`, and `generate` treats exactly one kind as proof:

- **`EXACT`** — a full-relation count, not a sample. The only kind that is proof, and only when it actually shows no duplicates; an `EXACT` row reporting duplicates is decisive evidence *against*.
- **`SAMPLED`** — recorded as evidence on the proposal, never used as proof.
- **`UC_CONSTRAINT`** — a declared primary key is **not** proof either. Unity Catalog primary keys are informational and unenforced, so the declaration restates the same unvalidated claim one layer down. **This includes a key declared `RELY`, and that case was considered rather than overlooked** — see below.
- **`UNKNOWN`** — absent evidence, which is the default and steps down.

*The `RELY` case, considered and rejected — do not relitigate as an oversight.* A UC primary key declared `RELY` is genuinely a different act from inferring uniqueness off a sample: it is the customer asserting the constraint to the optimizer, and Databricks already rewrites queries on that assertion, so there is a real argument that trusting it merely inherits a trust the platform has already extended. It is still refused here, for three reasons that are about this generator rather than about `RELY`'s soundness. The **failure mode is not shared**: a wrong `RELY` on a query rewrite yields a wrong result set the customer can see and report, while a wrong `rely.at_most_one_match` in a metric view inflates `SUM` and `COUNT` through fan-out into numbers that look plausible and are never flagged, because nothing validates the claim at runtime. The **cost of refusing is near zero**: the two constructs it gates are optimizations — rung 3 answers every question rung 2 answers, and omitting `rely` changes no number — so conservatism buys correctness at the price of verbosity, not capability. And **the evidence is cheap to get properly**: one exact `COUNT(*) = COUNT(DISTINCT k)` on the dimension turns the claim into `EXACT` and unlocks both constructs through the gate that already exists, which is strictly better than promoting a declaration. If a later prompt wants `RELY` honored, the argument to make is that the exact count is too expensive on some dimension at some scale — not that this case was missed.

**The consequence, stated plainly: nothing in the repository produces `EXACT` evidence today, so `rely` is never emitted and the nested rung is unreachable on every compute — independently of MV-D13's capability floor, which already blocks it on warehouses.** Multi-hop candidates land on rung 3 (subquery-`source`), where uniqueness is *enforced* by construction rather than asserted. That output is correct on all runtimes; the two blocked constructs are optimizations, and neither changes an answer. Prompt 6 may add an exact-count probe against the candidate's join keys, which would make `rely` reachable through the same gate with no change here.

*Two sentences above are known optimistic and under revision in Prompt 6 — do not build on either.* Marked at review time rather than left to be found, because a correct-when-written claim that a later finding has undermined is precisely what a decision note exists to prevent and what a doc freeze otherwise preserves. **(1) "the evidence is cheap to get properly"**, in the `RELY` paragraph, was reasoned against promoting a declaration and not against a full scan: `COUNT(DISTINCT k) = COUNT(*)` on a large dimension is the very cost that made GSO sample in the first place, so Prompt 6 needs a cost guard or an opt-in rather than a bare probe, or it will define proof nobody can afford to produce. **(2) "with no change here"**, in the paragraph immediately above, holds only if evidence is timeless. It is not — a table proven unique at profile time can gain duplicates before the view is queried, and because `at_most_one_match` is unvalidated at runtime the failure is silent aggregate inflation rather than an error — so at minimum `KeyUniqueness` gains when the count was taken and against what, which is a change here.

*Standing preference on (2), to be settled on measured cost rather than on principle.* Run the probe at **trigger time and keep the window short by construction**, rather than making `proven` time-dependent: a staleness bound obliges every consumer to reason about a clock, and the first consumer that forgets reintroduces exactly the inflation the evidence standard exists to stop. A short window is harder to get wrong than a window that must be checked. This is a preference and not a ruling — if a trigger-time scan proves unaffordable on the dimensions that actually matter, timestamped evidence carrying an explicit staleness bound is the honest fallback, and that call belongs on the probe's real cost profile.

*And one seam Prompt 6 creates.* The nested rung and `rely` are exercised today only through the `PROVEN` fixture, which injects synthetic `EXACT` rows at the generator's input boundary. That proves the *consumer* of the evidence; it says nothing about a producer that does not exist yet. The moment a probe emits real `EXACT` rows, the untested surface becomes the producer and the handoff — so assert that a probe result reaches the gate in the shape the gate expects, rather than trusting the dataclass boundary to hold because both sides were written from the same description.

*Two smaller calls from the same prompt, recorded because both deviate from the prompt text.* First, MV-D8's transitive-join detector was written as "port the left-head-of-`on` check," which presumes an implementation to port; there is none — the rule exists only in the external patterns skill, which under Prompt 5.5's own terms is a source and not an authority. It is therefore **restated in-repo** in `mv_yaml`'s module docstring and implemented from the restatement, and the check **splits by severity**: a first-level `on` whose columns reach a sibling join is an error (that is the transitivity the rule exists to catch), while one written `dim.k = source.k` instead of `source.k = dim.k` is a **warning**, because operand order is a style convention and rejecting it would fail valid YAML. Second, the comment-echo check reuses `LeakageOracle.contains_question` at `MV_COMMENT_ECHO_THRESHOLD` rather than adding a second matcher, per the standing instruction to extend `leakage.py` instead of writing a parallel scanner.

*The echo comparison is `>=`, and MV-D8 was corrected to match the code rather than the reverse.* MV-D8 and this prompt's body both said "at >0.9," while `leakage.py:752` compares `score >= thresh`, so a line landing on exactly 0.90 was rejected by the firewall and permitted by the standard. Resolved toward `>=` in both documents, under the DOC FREEZE factual-error exception, for two reasons. `contains_question` is shared: its other caller is the example-SQL firewall at 0.85, so changing the operator to satisfy one new caller would alter detection behavior on a path this feature does not own — and it would move that path's boundary in the permissive direction, which is the wrong way for a firewall to drift. And the inclusive reading is the one a firewall wants anyway: at the threshold the evidence for "this echoes a benchmark question" is exactly as strong as the threshold was chosen to represent, so the tie belongs to rejection. The boundary is now executable rather than prose — `test_a_line_at_exactly_the_threshold_is_rejected` builds a line sharing nine of ten content tokens with a benchmark question (Jaccard exactly 0.90) and asserts rejection, alongside an 0.80 line that passes, so an operator flipped to `>` fails the suite.

**MV-D15 — A signal that was never measured is not a signal that measured zero.** Taken during Prompt 6 execution, on the Prompt 6 signal recon (`docs/design/mv-advisor-signal-recon.md`). The recon established that of the four blend inputs only Y has a producer today: **L** has none at all (`LineageOverlap` is an input contract at `mv_scoring.py:150` that nothing outside tests constructs, and column-level overlap additionally needs a `WATCH_SYSTEM_GRANTS` addition), **D** has no cost or distinct-user source and its only available grain is wrong, and **S** silently returns a default `SemanticMatch` whose zero is byte-identical to "there was nothing to compare." Scoring all three as 0.0 would make the blend arithmetic sound and its output meaningless. So availability becomes part of the score rather than something a reader has to infer from it.

- **Each of L/Y/S/D records a status on `score_components`: `COMPUTED` | `UNAVAILABLE` | `EMPTY`.** `UNAVAILABLE` means no producer exists — nothing looked. `EMPTY` means a producer ran and found nothing to compare, which is a real measurement of zero and is treated as one: it scores 0.0 and **keeps its weight**. `UNAVAILABLE` does **not** score 0.0; it leaves the blend entirely.
- **The blend renormalizes over what was actually measured.** The weighted sum divides by the summed weight of the `COMPUTED` and `EMPTY` signals, so the scale stays 0–100 whatever the mix. That sum is recorded as `evidence_coverage`, because a renormalized score without its divisor is unauditable.
- **Coverage caps the tier.** `>= 0.80` is HIGH-eligible; `>= 0.50` ceilings at MEDIUM; `< 0.50` ceilings at LOW. When the cap is what bound the tier, `tier_capped_by_coverage: true` is recorded with the uncapped tier alongside it, so a reviewer can see both what the evidence said and what the coverage allowed.

**The canonical statement of this decision, in two rows.** Anything later that contradicts these two lines contradicts MV-D15:

| components | score | coverage | tier |
|---|---|---|---|
| L `UNAVAILABLE`, others at 1.0 | **100.00** | 0.65 | MEDIUM (capped) |
| L `EMPTY`, others at 1.0 | **65.00** | 1.0 | MEDIUM (uncapped) |

**Identical `L` input, different status, different score. Status decides.** In both rows `L = 0.0` — the number is the same byte. The first row's L never entered the blend, so the remaining three renormalize over 0.65 and the score is a full-confidence statement about a partial evidence set, which is why the *tier* rather than the score is what coverage bounds. The second row's L is a measurement: something looked, found no overlap, and 0.0 is the finding — so it keeps its 0.35 weight and drags the blend down honestly. Reading the score alone cannot distinguish them, which is the entire reason `statuses` and `evidence_coverage` are persisted next to it. Pinned by `test_an_unavailable_signal_does_not_score_zero`.

*Why renormalize AND cap, when either alone looks sufficient.* They fix different defects, and the second one is the one that bites. **Uniform absence collapses the SCALE:** if every candidate is missing L, every score is computed against a 0.65 ceiling while the tier thresholds are still calibrated for 100 — HIGH becomes structurally unreachable for the entire output class. That is precisely the defect **MV-D12** rejected when an MV-field-only reading of S capped every `NEW_METRIC_VIEW` candidate at 80, and rejecting it there while accepting it here would be incoherent. Renormalization alone fixes that. But **mixed availability scrambles ORDER**, which renormalization cannot fix and actively conceals: a candidate scored on Y alone renormalizes to 90 and outranks a candidate scored on all four at 78, when the first is one signal's opinion and the second is a corroborated finding. Ranking is the advisor's actual product — a reviewer reads down the list — so a confident-looking score built from a quarter of the evidence is worse than a low one. Hence coverage is recorded **per candidate**, not per run, and the cap binds the tier rather than adjusting the number: the score stays an honest statement about what was measured, and the tier stays an honest statement about how much was measured.

*What the coverage numbers actually are today, which is not what this decision's authorizing prompt assumed.* The prompt said coverage is 0.65 with L absent. That is the state **after** a partial 6a that lands D but not L. Today **D is `UNAVAILABLE` too** (for the reasons in the next paragraph), so coverage is **0.50**. The three states, then: **0.50 today** (L and D absent), **0.65** when 6a lands D, **1.0** when 6a lands L as well. All three sit below 0.80 until L exists, so **nothing can exceed MEDIUM until there is a lineage producer** — which generalizes the authorizing prompt's conclusion rather than contradicting it, since L alone is 0.35 and HIGH needs 0.80. If the embedding endpoint is also unreachable, S is `UNAVAILABLE`, coverage is 0.30, and the ceiling is LOW. This is the intended, legible state, and it is legible precisely because the coverage figure is on every candidate.

*Why D is `UNAVAILABLE` rather than approximated — recorded so 6a does not relitigate it.* Prompt 6 was authorized to ship "frequency-only at mapped column grain if you can do it cleanly." It cannot be done cleanly, on two independent grounds, and either alone is sufficient:

- **(a) Population mismatch.** The advisor's corpus is iteration-0 *generated SQL from benchmark questions*, while `wide_schema_history`'s counts are *real query-history traffic*. These are two different populations. Attributing history counts to a benchmark-derived measure is conflation, not measurement — the benchmark asks what a curated question set covers, the history says what humans actually ran, and a measure can rank high in one and be absent from the other. D's job in the blend is to say "real users care about this," which a benchmark corpus cannot answer at any grain.
- **(b) Double-counting.** The only per-measure recurrence that exists today is `corpus_scan`'s, and that is already **Y**. Routing it into D would spend one piece of evidence twice: the same number would raise the blend through two weights *and* raise `evidence_coverage` by 0.15, so a candidate would look better corroborated precisely because one signal was counted again. That is worse than absence, because renormalization makes it invisible — the coverage figure, the one field a reviewer uses to judge how much was measured, would be inflated on evidence already claimed.

`wide_schema_history` also cannot supply the other two `DemandSignal` fields at all: its `SELECT` reads `statement_text` and `start_time` and no duration or billing column, so neither `cost_ms` nor `distinct_users` has a source (see the corrected docstring at `mv_scoring.py:200-216`, whose earlier version wrongly claimed it did). **A defensible D therefore needs its own per-measure query over `system.query.history` — cost and distinct users at measure grain — not a remap of an existing signal.** That is 6a's work, and it carries the same service-principal read constraint as L.

*Both pinned worked examples are unchanged, and a test says so.* POV Part 3's two examples supply all four components directly, so they are all `COMPUTED`, coverage is 1.0, and the divisor is 1.0 — 80.0 and 58.75 are the same IEEE doubles they were, not values that happen to round back. `test_the_pinned_worked_examples_are_untouched_by_renormalization` asserts the scores, the coverage, and that neither tier is coverage-capped, so a future change to the renormalization cannot quietly move the two numbers the POV pins.

*S distinguishes its two zeros; L does not have to.* `SemanticMatch` gains a `status` mirroring the `MV_ECHO_CHECK_COMPARED` / `NOT_COMPARED` pair B4 established, because the recon found a dead endpoint and an absent reference set producing identical payloads. An unconfigured or unreachable endpoint — including the `client is None` case and any `embed` failure — is `UNAVAILABLE`. A client that ran against a real reference set and found no positive cosine is `EMPTY`: that is a measurement. L needs no such split yet because it has no producer at all; when 6a adds one, a lineage read that resolves to two genuinely disjoint column sets is `EMPTY` and a read that could not run is `UNAVAILABLE`.

*In-job generation always lands on rung 3, and Prompt 9 must not replay this artifact.* The job has no entitlement probe — that is a backend/OBO surface under MV-D1 — so `MvProfiling.capabilities` is empty in the advisor phase, and per MV-D13's step-down-on-`UNKNOWN` contract every multi-hop candidate renders as rung 3 (subquery-`source`). The YAML in `genie_opt_artifacts` is therefore correct-on-every-runtime and deliberately more verbose than a probed runtime might allow. **Prompt 9 must REGENERATE the YAML under the backend's probe capabilities rather than replay the run-1 artifact.** Replaying it would ship rung 3 to a workspace whose probe proved rung 2 available, permanently — the artifact records what the job could prove, not what the create path can. The artifact's `content_hash` is the candidate's dedup fingerprint (MV-D7), so the regenerated YAML can be compared against the recorded one for the same candidate rather than guessed at.

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
- Assessment reasons are ALREADY surfaced — genie_eval_taxonomy.py, consumed
  via map_eval_detail_to_row (eval_runner.py). CONSUME that taxonomy; do not
  build a second one. If lift_report or run_subset needs a reason-derived
  grouping the taxonomy does not expose, extend the taxonomy module itself
  and say so in VERIFY.
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
- Y: normalized recurrence * corpus-internal AST-equivalence flag (did the
  counted statements genuinely collapse to one canonical form). CORRECTED under
  MV-D11(a): an earlier wording said "against existing MV measures", which
  contradicts POV Part 3's first worked example (Y = 0.95 with no MV
  equivalent). Equivalence against governed MVs belongs to the dedup gate
  below, which blocks rather than damps. Existing MV YAML still parses from
  DESCRIBE ... AS JSON view_text — for the gate, not for Y.
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
- Also returns CAPABILITY rows alongside the privilege rows, per MV-D8: DBR
  17.3+ create/edit, 17.1+ nested joins, 18.1+ fields/agg/window offset.
  Prompt 5.5's generator reads them to pick a join strategy, so if the probe
  omits them the generator either invents them or emits YAML the runtime
  cannot plan. Detect from current_version(); a SQL warehouse reports only
  dbsql_version and there is no published DBSQL-to-DBR mapping, so those rows
  are UNKNOWN, never a guess. UNKNOWN resolves in two directions: for the two
  optional capabilities it means unavailable (the generator steps down the
  ladder), while for create/edit it never blocks authorization — no GRANT can
  satisfy a runtime floor, and only the write itself can prove it.
- consent record: create/persist per Prompt 1 model; verify(consent, fresh_probe)
  is called by the TRIGGER flow (Prompt 9) immediately before any OBO create —
  any mismatch returns effective_mode=suggest_only with downgrade_reason.
- FastAPI route: POST under the existing /api/auto-optimize router prefix
  (e.g. /api/auto-optimize/mv/probe), OBO, returning the probe result. Follow
  backend/routers/auto_optimize.py conventions exactly.
- Tests: all-granted, each-single-privilege-missing, schema-not-found, revoked-
  between-config-and-trigger (verify() downgrade), capability rows for a DBR
  above and below each floor plus the warehouse-only UNKNOWN case, and an
  assertion that no code path issues DDL.
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
  comment line that matches a benchmark question at >=0.90 normalized
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

For this prompt only, you MAY read the metric-views-patterns skill at
/Users/prashanth.subrahmanyam/Projects/vibe-coding-workshop-template/data_product_accelerator/skills/semantic-layer/01-metric-views-patterns
for reference detail (composability-patterns.md, level-of-detail.md,
yaml-reference.md, validation-checklist.md). It is a SOURCE, not an authority:
where it disagrees with MV-D8 or POV Part 4's generation standard, the in-repo
documents win, and anything you rely on gets restated in-repo rather than cited
by external path.
```

*Why the skill pointer was scoped to this prompt, and why it no longer is.*
The pointer was originally confined to Prompt 5.5 on the reasoning that a
standing named exception would re-introduce what the in-repository context rule
exists to prevent — an unversioned dependency on one machine's filesystem,
invisible to a fresh clone and to every teammate running these prompts — and
that the rule already carried the right mechanism in *"unless the prompt names
them explicitly."* That call was **reversed** in the METRIC VIEW BEST PRACTICES
bullet in the standing preamble above, which promotes the same folder to a
standing named exception in both rules copies, on the reasoning that YAML, join
and level-of-detail semantics were being re-derived from scratch every prompt.
The trade was accepted deliberately and bounded three ways: the exception is
the only one, it never outranks repo reality or the gap report or MV-D8, and a
missing path is a proceed rather than a block.

Two things the reversal does not change. The skill remains a SOURCE and not an
authority, so anything relied on still gets restated in-repo rather than cited
by external path — the substance is already absorbed and versioned as MV-D8 and
POV Part 4's generation standard. And the trigger still does not fire before
this prompt: Prompt 4 confirmed scoring authors no YAML and only consumes
metric-view fields `metric_view_catalog` has already parsed.

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
- ALSO OWNED HERE (assigned during Prompt 6, from the signal recon): a FOUR-WAY
  mirror test. Today's pin is only two-way — test_deploy_lib.py:488
  (test_gso_job_settings_mirror_package_bundle_4task) checks gso_job.py against
  the package bundle, with llm_model as the sole sanctioned exception. The ROOT
  databricks.yml and backend/job_launcher.py's run_now map are UNPINNED, so a
  parameter can be added to two mirrors and silently omitted from the other two.
  That failure is invisible at runtime: run_optimize.py declares every widget
  with dbutils.widgets.text(<name>, <default>) BEFORE reading it, so an omitted
  job parameter resolves to the notebook default instead of raising. The advisor
  gate inherits that shape — enable_metric_view_suggestions defaults to "false",
  so a missing mirror silently disables the feature rather than failing loudly.
  Extend the pin to all four mirrors, keeping llm_model's exception explicit.
  (Prompt 6 added the enable_metric_view_suggestions widget read to
  run_optimize.py ahead of this prompt, since the phase cannot be gated without
  it. The other four parameters and all four mirror entries remain Prompt 8's.)
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
