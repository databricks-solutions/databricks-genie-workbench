# Cursor Prompt Playbook — Building the Metric View Advisor in Genie Workbench

*A sequenced set of Cursor prompts that take the POV document from design to a tested, PR-ready feature branch in `databricks-solutions/databricks-genie-workbench`. One prompt ≈ one reviewable commit. Run them in order; each prompt's output is the next prompt's input.*

> **Custody.** From this commit forward, this in-repo copy is the canonical playbook and the canonical MV-D register. Any external copy is downstream: reconcile it to this file, never the reverse.

---

## Before you start (manual steps, 10 minutes)

1. **Commit the design doc AND this playbook into the repo** so Cursor can reference both in every prompt. The playbook is the defining source of the MV-D decision numbering (MV-D1–MV-D25 today, appended to as later prompts take architecture calls); if it is not in the repo, agents cannot resolve the citations and will (correctly) refuse to stamp them:
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
     SCOPE (MV-D23): this governs state passing INSIDE the job. It does not make
     run_id the only legitimate key on the MV tables. genie_opt_mv_candidates is
     partitioned by target_space_id precisely because a candidate outlives the run
     that proposed it (MV-D7), and wh_load_mv_candidates already reads by
     target_space_id or run_id. A space-keyed READ of metric-view state is correct
     and is not a violation of this rule.
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
     FastAPI backend under OBO (require_obo_workspace_client), against a
     recorded consent. The job attaches (patch), measures, and
     reverts — writes it already performs as SP today.
     "AT TRIGGER TIME" IS NOT ONE OF THE INVARIANTS (MV-D23). Through Prompt 9 it
     was the only moment a create could happen, so the rule read as though the
     timing carried the safety. It does not. The four invariants are: the identity
     (require_obo_workspace_client, which hard-fails — never the SP-tolerant
     get_workspace_client), a recorded and freshly re-verified consent,
     downgrade-never-upgrade on any mismatch, and the target being the consented
     schema and nowhere else. A create initiated outside a run must satisfy all
     four. Until MV-D23 is decided, do not build one — but do not treat the
     timing as the reason.
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

   - RUN THE SUITES WITH `./scripts/test.sh`. It runs both suites through
     `uv run --frozen --extra dev`. Expected baseline: 596 backend + 1420 GSO,
     measured at c4e72077 as 594 + 1420, +2 backend at Prompt 11 (the space-scoped
     mv-proposals route). A count BELOW this is a regression — investigate. A
     count ABOVE it is normal growth: update this line and the playbook's copy in
     the same commit that adds the tests (test_rules_parity.py enforces the two
     copies match, so you cannot update one).
     `--extra dev` is mandatory, not stylistic: `asyncio_mode = "auto"` needs
     pytest-asyncio, which lives in the dev extra, and without it exactly 12
     async backend tests fail on uncollectable coroutines. Those 12 are an
     INVOCATION defect. Do NOT report them as pre-existing and do NOT debug the
     code — fix the command. If you call pytest directly, it is
     `uv run --frozen --extra dev pytest`.
   - IMPORT RESOLUTION IS PART OF EVERY VERIFY THAT RUNS BACKEND TESTS: before
     trusting any backend pytest result, print
     `uv run --frozen --extra dev python -c "import genie_space_optimizer as g; print(g.__file__)"`
     and confirm the path resolves inside THIS repository checkout. A test run
     against a foreign checkout is void — report it, do not interpret it.
   - PINNED-DEPENDENCY RESOLUTION: for any behavior that depends on a pinned
     library version (sqlglot parsing and rendering above all), run only under
     `uv run --frozen`. A bare pytest on an ambient interpreter is a void run:
     the pyenv global carries sqlglot 30.0.1 against the pinned 30.0.3, and
     canonicalization output can differ across parser patch releases. Report
     the resolved version in VERIFY alongside the import path.
   - `uv lock --check` FAILS STRUCTURALLY on this repo and is NOT drift: the GSO
     package version is dynamic (git-describe), so re-resolution differs from the
     lockfile at every commit, on a clean tree. Verify with
     `git status -- uv.lock` instead. Never run `uv lock` to silence it, and do
     not wire `uv lock --check` into CI.
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

## Decisions register (MV-D1–MV-D25)

The recon surfaced five structural conflicts, not naming drift. These decisions resolve them and are baked into the revised prompts below. MV-D1 changes the user-facing flow and needs explicit sign-off. MV-D7 was added during Prompt 1 execution, MV-D8 with the generation quality standard, MV-D9 from the Prompt 2 readiness check, MV-D10 during Prompt 3 execution, MV-D11 and MV-D12 during Prompt 4 execution, MV-D13 during Prompt 5 execution, MV-D14 during Prompt 5.5 execution, MV-D15 during Prompt 6 execution, MV-D16 during Prompt 7 execution, and MV-D17 (decided during Prompt 6c execution) and MV-D18 during the Prompt 7 review. MV-D19 was recorded OPEN when Prompts 6a and 6b were drafted and is decided during Prompt 6a — like MV-D17 before it, it is flagged here so no earlier prompt quietly settles it by accident. MV-D20 and MV-D21 were recorded OPEN from the Prompt 9 gap check and are decided during Prompt 9, flagged the same way so the "add four routes" framing does not quietly settle the executor-identity and state-access questions by default. MV-D22 was recorded during Prompt 9 execution — it supersedes MV-D15's regeneration clause once the persistence picture showed regeneration was neither achievable nor meaningful. MV-D23 was recorded OPEN immediately after Prompt 9 landed, from a review asking whether the advisor can serve a space that has never been optimized, and is decided during Prompt 13.5 — flagged here, like MV-D17 and MV-D19 before it, because every persistence surface Prompts 1–9 built is keyed on `run_id` and the four prompts between this note and 13.5 would otherwise harden that assumption into the UI without anyone choosing it. MV-D24 was recorded OPEN at the Prompt 10 mockup review, from four user questions about the create path the suggest-only screen invites but cannot complete — it is decided during Prompt 13.5 alongside MV-D23, flagged the same way. MV-D25 was recorded OPEN before Prompt 12, from the question of whether the engine can suggest metric views from schema and profiling alone, with no SQL corpus — it is NOT decided on this branch (owner: the create-agent branch, after Prompt 16), and is registered here so no prompt on this branch quietly builds a speculative candidate producer. Later decisions append here — this register is the defining namespace, and the playbook copy committed at docs/design/mv-advisor-playbook.md must be refreshed whenever it changes.

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

*Third defect class, found in Prompt 7.* Two fences were already stale at HEAD when Prompt 7 began — `test_phase7_job_dag.py` and `ddl.py:182` — and neither file was touched by the commit that staled them. Prompt 6 changed the enumerated `artifact_kind` values; the gap report quoted them; the quote rotted in a file Prompt 6 never opened. So the classes are three, not two: **wrong when written** (the `leakage.py:132-137` mispointer), **shifted by an edit to the quoted file** (the two `leakage.py` fences), and **invalidated by an edit somewhere else entirely** (these two). The third is the one that defeats any freshness discipline keyed on the commit's own diff, because the changed path and the stale path have no overlap — which is precisely why the byte-match sweep runs unconditionally over every fence rather than only over the paths a commit touched.

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

Both halves of that were demonstrated by reintroducing the defect rather than argued: with `_source_column_metadata` stubbed back to `()`, the end-to-end pin fails with `reference_kind: NONE`, `status: EMPTY`, `field: None`, while the thirteen MV-D12 scorer tests stay green — they contain no reference to the producer at all, so no addition to that file could have caught this. **And `evidence_coverage` is 0.50 in both the healthy and the defective case in a workspace where L and D are also unavailable**, because `EMPTY` keeps its weight under MV-D15: the coverage figure is *by design* blind to a producer that never ran versus one that ran empty, so it cannot substitute for this assertion. That is the cost of MV-D15's `EMPTY`-keeps-weight rule, and it is the right trade — but it means an unfed producer is invisible in every persisted field except the reference kind.

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

**MV-D15 — A signal that was never measured is not a signal that measured zero.** Taken during Prompt 6 execution, on the Prompt 6 signal recon (`docs/design/mv-advisor-signal-recon.md`). The recon established that of the four blend inputs only Y has a producer today: **L** has none at all (`LineageOverlap` is an input contract at `mv_scoring.py:158` that nothing outside tests constructs, and column-level overlap additionally needs a `WATCH_SYSTEM_GRANTS` addition), **D** has no cost or distinct-user source and its only available grain is wrong, and **S** silently returns a default `SemanticMatch` whose zero is byte-identical to "there was nothing to compare." Scoring all three as 0.0 would make the blend arithmetic sound and its output meaningless. So availability becomes part of the score rather than something a reader has to infer from it.

> *Regeneration clause superseded by MV-D22 (recorded during Prompt 9).* MV-D15's direction that Prompt 9 *regenerate* the YAML under the backend's probe is superseded: the backend replays the persisted `yaml_text` with revalidation, it does not regenerate. The reasoning below stays as recorded; see MV-D22 for why regeneration was neither achievable nor meaningful.

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

*What the coverage numbers were before the producers landed, and what unlocks each state.* The decision's authorizing prompt assumed coverage 0.65 with L absent — the state after a partial 6a that lands D but not L. Before Prompt 6b wired the producers, both L and D were `UNAVAILABLE`, so coverage sat at **0.50** and nothing could exceed MEDIUM. The states in ascending coverage: **0.30** (only Y, embedding endpoint unreachable, ceiling LOW), **0.50** (Y + S), **0.65** (Y + S + D), **1.0** (all four). HIGH needs 0.80 and L alone is 0.35, so HIGH becomes reachable once a genuinely computed L joins the blend. Prompt 6b makes all four producible from the system tables; whether a given workspace reaches 1.0 or degrades toward 0.30 is now a **per-workspace fact** of grants and data availability, not a fixed ceiling — and it stays legible either way because the coverage figure rides on every candidate.

*Why D is `UNAVAILABLE` rather than approximated — recorded so 6a does not relitigate it.* Prompt 6 was authorized to ship "frequency-only at mapped column grain if you can do it cleanly." It cannot be done cleanly, on two independent grounds, and either alone is sufficient:

- **(a) Population mismatch.** The advisor's corpus is iteration-0 *generated SQL from benchmark questions*, while `wide_schema_history`'s counts are *real query-history traffic*. These are two different populations. Attributing history counts to a benchmark-derived measure is conflation, not measurement — the benchmark asks what a curated question set covers, the history says what humans actually ran, and a measure can rank high in one and be absent from the other. D's job in the blend is to say "real users care about this," which a benchmark corpus cannot answer at any grain.
- **(b) Double-counting.** The only per-measure recurrence that exists today is `corpus_scan`'s, and that is already **Y**. Routing it into D would spend one piece of evidence twice: the same number would raise the blend through two weights *and* raise `evidence_coverage` by 0.15, so a candidate would look better corroborated precisely because one signal was counted again. That is worse than absence, because renormalization makes it invisible — the coverage figure, the one field a reviewer uses to judge how much was measured, would be inflated on evidence already claimed.

`wide_schema_history` also cannot supply the other two `DemandSignal` fields at all: its `SELECT` reads `statement_text` and `start_time` and no duration or billing column, so neither `cost_ms` nor `distinct_users` has a source (see the corrected docstring at `mv_scoring.py:200-216`, whose earlier version wrongly claimed it did). **A defensible D therefore needs its own per-measure query over `system.query.history` — cost and distinct users at measure grain — not a remap of an existing signal.** That is 6a's work, and it carries the same service-principal read constraint as L.

*Both pinned worked examples are unchanged, and a test says so.* POV Part 3's two examples supply all four components directly, so they are all `COMPUTED`, coverage is 1.0, and the divisor is 1.0 — 80.0 and 58.75 are the same IEEE doubles they were, not values that happen to round back. `test_the_pinned_worked_examples_are_untouched_by_renormalization` asserts the scores, the coverage, and that neither tier is coverage-capped, so a future change to the renormalization cannot quietly move the two numbers the POV pins.

*S distinguishes its two zeros; L does not have to.* `SemanticMatch` gains a `status` mirroring the `MV_ECHO_CHECK_COMPARED` / `NOT_COMPARED` pair B4 established, because the recon found a dead endpoint and an absent reference set producing identical payloads. An unconfigured or unreachable endpoint — including the `client is None` case and any `embed` failure — is `UNAVAILABLE`. A client that ran against a real reference set and found no positive cosine is `EMPTY`: that is a measurement. L needs no such split yet because it has no producer at all; when 6a adds one, a lineage read that resolves to two genuinely disjoint column sets is `EMPTY` and a read that could not run is `UNAVAILABLE`.

*In-job generation always lands on rung 3, and Prompt 9 must not replay this artifact.* The job has no entitlement probe — that is a backend/OBO surface under MV-D1 — so `MvProfiling.capabilities` is empty in the advisor phase, and per MV-D13's step-down-on-`UNKNOWN` contract every multi-hop candidate renders as rung 3 (subquery-`source`). The YAML in `genie_opt_artifacts` is therefore correct-on-every-runtime and deliberately more verbose than a probed runtime might allow. **Prompt 9 must REGENERATE the YAML under the backend's probe capabilities rather than replay the run-1 artifact.** Replaying it would ship rung 3 to a workspace whose probe proved rung 2 available, permanently — the artifact records what the job could prove, not what the create path can. The artifact's `content_hash` is the candidate's dedup fingerprint (MV-D7), so the regenerated YAML can be compared against the recorded one for the same candidate rather than guessed at.

**MV-D16 — Attach is not an LLM lever, and it lands after iteration-0.** Taken during Prompt 7 execution, resolving two defects in that prompt's own body. Both were authored from the gap report's description of the unified loop rather than from the loop's code, and both would have been executed as written.

**(a) `mv_attach_data_source` is registered in `PATCH_TYPES` with a real applier action and is deliberately NOT in the `unified_loop` allowlist.** Prompt 7's body said to add it, describing that frozenset as the lever surface. It is not: `_ALLOWED_PATCH_TYPES` is the **LLM-proposal** surface. It is enumerated into the proposal prompt (`unified_loop.py:1321`), used to drop any LLM-returned patch type outside it (`:1393`), and used to decide whether a post-leak pivot has anywhere to go (`:2158`). `apply_patch_set` never reads it. So listing attach there would not "enable the lever" — it would hand the loop's LLM the ability to invent a UC identifier and attach it mid-loop with **no consent row and no `genie_opt_mv_created_objects` entry**, defeating MV-D1's consent chain end to end. The attach phase calls `apply_patch_set` directly and needs no allowlist entry. The type is classified `HIGH_RISK`. This is asserted two ways, because the absence of a line is not self-documenting: one test pins that the type is not in the frozenset, and one drives an LLM response proposing it and asserts the patch is dropped. That second test is the one that protects the consent chain from a future contributor who adds the type to the allowlist for what will look like a good reason.

**(b) Attach and `mv_lift` run AFTER iteration-0 completes and BEFORE the first lever patch — inside `run_unified_optimization_loop`, not before the call.** Prompt 7's body said "ordered BEFORE the unified loop" and, three lines later, "iteration-0 baseline runs first (existing behavior — do not reorder it)". Those cannot both hold, because iteration-0 lives *inside* the loop (`unified_loop.py:2894-2899`). "Before the lever loop" meant before **tuning**; it was written as before the **call**, and that reading is superseded. The reason the distinction matters is not tidiness: attaching before iteration-0 makes the baseline corpus **post-attach SQL**, so the metric view contaminates the very evidence Prompt 6's advisor fingerprints when proposing the next one — a feedback loop in which each attached view biases the case for its successor. With the corrected order, baseline measures the pre-attach space, `mv_lift` measures the attach in isolation against that baseline, and the levers tune on top of whatever foundation survives the lift verdict. Prompt 7's body is corrected in place under the DOC FREEZE factual-error exception so no later prompt re-derives the wrong order; pinned by asserting the baseline eval-run id precedes the attach patch id.

*Three consequences recorded so they are not relitigated.* **Detach uses `applier.rollback` against `apply_log["pre_snapshot"]`, not `integration/revert.py`.** `revert_optimization` (`integration/revert.py:55`) is a backend surface: it reads run history through the warehouse and its `_assert_no_active_space_runs` guard (`:221`) rejects mid-run **by design**. The primitive changes; detach-never-drop does not. **`mv_remove_raw_table` is dropped from the design.** `remove_table` already exists, is `HIGH_RISK` (`common/config.py:1481`), has a working render (`applier.py:3166-3170`) and config applier (`:3880-3886`), and is already outside the LLM allowlist — a twin buys nothing and creates a second path for one behavior. **The `mv_consent_id` job parameter carries a `probe_id`**, because `genie_opt_mv_consents` is keyed on `probe_id` (`ddl.py:235`) and no `consent_id` column exists; the read site says so.

**MV-D17 — DECIDED (Prompt 6c): curated provenance credits extra occurrences inside Y, and the corpus 6c harvests is narrower than the five-source list implied.** Raised during the Prompt 7 review, decided during Prompt 6c execution. **Y** counted every corpus occurrence equally (`normalized_recurrence` over `FingerprintRecurrence.recurrence`), so once 6c harvests curated sources into the same corpus, 60 occurrences of generated SQL would outscore 3 occurrences of trusted-asset SQL — inverting the POV's evidence hierarchy, which calls a curated source the higher-precision seed.

**(a) The provenance credit lives inside Y, and it is neutral at zero.** Chosen over a fifth blend signal and over a `tier_for` floor. The deciding reason is arithmetic, not preference: `evidence_coverage` (`mv_scoring.py`) rounds to six places and the docstring is explicit that the rounding is load-bearing — the S-unavailable case sums to `0.7999999999999999`, one ULP under `MV_COVERAGE_HIGH_MIN`. A fifth weight puts every authored decimal in that sum back in play, including the two IEEE-exact pins at `test_mv_scoring.py`. **The credit is added to the occurrence count inside the log, not multiplied onto its output:** `Y = normalized_recurrence(r + k * curated_provenance_count)`, `k = MV_CURATED_OCCURRENCE_EQUIVALENT` (default 20). The first draft multiplied the saturated base by `1 + w * curated_provenance_count`; that shape was wrong twice over — it under-credited the modal case (one human curates a measure the generator derived once: `0.115 * 2 = 0.23`, still buried) and walled at the 1.0 clamp for anything already recurring, so Y stopped discriminating among curated candidates at `r ≥ 8` for a single source. Adding inside the log is monotone throughout and clamps only at genuinely saturating effective counts. It is neutral **by construction** when `curated_provenance_count` is 0 (`k * 0 == 0` returns the identical float), so every generated-only candidate — the pinned POV worked examples included — scores precisely as before; the blend and the MV-D15 divisor are untouched.

**`k` is an authored number, not a clamp accident.** `k` is how many generated occurrences one curated source is worth, and the modal question MV-D17 names — one human curated the measure once, the generator produced sixty — is answered by `k`'s value, not by the function's shape. At the default `k = 20` (with `MV_RECURRENCE_SATURATION = 75`): a single curated source with `r = 1` scores `normalized_recurrence(21) = 0.714`, worth ~21 generated derivations — decisively above a lightly-recurring generated measure and below sixty of them (`r = 60 → 0.949`); the named example of 3 curated occurrences scores `normalized_recurrence(63) = 0.960` and clears 60 generated. `k = 20` is the default and there is no legitimate opposite pole. Raising `k` toward 60 is **not** the symmetric "highest-precision seed" choice it looks like — it defeats itself. At `k = 60` the single-curated case scores `normalized_recurrence(61) = 0.953` against generated-sixty's `0.949`: that does not make curated outrank volume, it pushes both into saturation where Y stops ordering anything, reintroducing the clamp cliff through the env var. A 0.004 "win" in the saturated region is a coin flip against the other signals' noise, not a ranking. The deeper reason `k` stays low: **Y is recurrence evidence and nothing else, and authority already has its own channels.** The intuition that one curated definition should top sixty derivations conflates per-occurrence precision with authority — but when curated SQL contradicts a proposal, `trusted_asset_definitions` feeds the conflict path, and when a governed MV already defines the measure, the dedup gate blocks outright. Sixty independent re-derivations of one canonical form is genuinely strong demand evidence; a curated measure the corpus never re-derived should rank *below* it on Y while still surfacing. `k = 20` preserves that ordering; `k = 60` erases it. The earlier draft's claim that the credit "lets a curated expression the corpus rarely re-derived outrank a high-recurrence measure only the generator produced" is **withdrawn** as unconditional — it is true only above a `k` that saturates Y, i.e. no `k` orders it both correctly and non-trivially, which is the point.

**Coverage note — a LOW-tier curated candidate is a signal-availability problem, not a `k` problem.** In a workspace where the producers are unavailable (L and D `UNAVAILABLE` for want of grant or reader, S empty without an embedding endpoint) `evidence_coverage` is 0.50, so the single-curated candidate lands at score 42.8 (LOW) while generated-sixty gets 57.0 (MEDIUM). That reads like curated evidence being disrespected, but the cause is the 0.50 coverage, not the occurrence-equivalent. Raising `k` to compensate would bake a workaround for missing signals into a constant that outlives them; the L and D producers (6a, wired by 6b) are the actual fix, since curated candidates sitting on established lineage pick up L credit that generated-only ones may not. Recorded explicitly because someone will read the LOW tier as a `k` problem: flipping the env var to `k = 60` collapses Y's discrimination at the top (0.953 vs 0.949) and is **not** the remedy for curated candidates ranking LOW under partial coverage.

**Curated-ness is a recorded kind, not an inferred prefix.** The credit reads a new `FingerprintRecurrence.curated_provenance_count`, threaded through the bucket: `mv_fingerprint.CURATED_PROVENANCE_KIND` on `Provenance.kind`, one `_Bucket` slot, one line in `observe`, one line in `freeze`, one pass-through in `candidate_from_measure`, and `RecurrenceSignal.curated_provenance_count` that `syntactic_score` reads. It is **not** inferred from a `provenance_ids` prefix — the prefix route was available (ids survive `freeze` and `TRUSTED_ASSET_SOURCE_PREFIX` already establishes the convention) but making a string prefix structural to scoring cuts against this codebase's house style (`LineageOverlap.reference_kind`, `Provenance.kind`, the MV-D15 status vocabulary all record a kind). A `FingerprintRecurrence.to_dict()` key-set pin was added at the same time, since none existed — so the new key is a deliberate addition to persisted `evidence_json`, and `evidence["ast_curated_provenance_count"]` is the new auditable surface.

**Sub-decision — breadth damping is separable and deferred.** `FingerprintRecurrence`'s own docstring argues "sixty occurrences from one query is not a recurring measure," yet `syntactic_score` uses raw recurrence and ignores `provenance_count` (which is populated but read nowhere in scoring). Damping Y by distinct-source **breadth** is a distinct fix from up-weighting curated **provenance**; MV-D17 does only the latter, because breadth-damping would move Y for the two IEEE-pinned POV examples and every existing candidate — a separate calibration owed its own change. Recorded here so the curated credit does not quietly stand in for the breadth fix.

**Blocker 2 — `sql_functions` dropped, `sql_snippets` substituted.** `SqlFunction` carries only `id` + `identifier` in `serialized_space` (no body), so harvesting it needs a `DESCRIBE FUNCTION` UC read outside 6c's boundary. Dropped. Its curated-measure role is served by `instructions.sql_snippets` → `filters` / `expressions` / `measures`, each a `SqlSnippetBase` carrying `sql: list[str]` — real inline curated SQL already in the applied `serialized_space`, needing no new read. The five-source list this playbook implied missed it; this is a scope substitution, not a cut. A bodyless `sql_functions` entry (the shape the synthetic-data path appends) is skipped rather than crashing the loader, pinned by test.

**Blocker 3 — `join_specs` skipped, no synthetic wrapping.** `JoinSpec.sql` is a bare predicate fragment (`a.x = b.y`) that no extractor reads without synthetic wrapping, which is forbidden — and join keys do not feed Y for measure candidates anyway (the seed surface is `scan.measures`; join keys feed the YAML ladder and source-set). Skipped. Using `JoinSpec.left/right.identifier` as curated join topology for `mv_yaml`'s ladder is a legitimate but different consumer — **deferred**, not done here.

**Blocker 4 — governed metric views are a post-scan seed exclusion, not a corpus channel.** `dedup_gate` returns `VERDICT_BLOCKED` on an exact match, so a governed-MV measure entering the seed set would produce a candidate blocked by the very MV it came from. `corpus_scan` has no notion of an evidence-only entry — anything fed to it becomes a `_Bucket` and therefore a seed — so rather than add an evidence-only channel to the scan, `mv_advisor` filters seeds post-scan against the estate MV index it already holds at the assembly site. Realized as: harvest `example_question_sqls` + `sql_snippets` + `genie_opt_patches` into the corpus (curated), and drop any `scan.measures` entry whose canonical matches a governed measure before it consumes a candidate slot.

**MV-D18 — A kept attach survives every loop outcome, and the champion record has to say so.** Taken during the Prompt 7 review, from tracing the four finalize trajectories rather than from reading the design. The starting question was whether champion selection could drop a metric view that passed its own lift gate. It cannot drop it from the **space** — but it could drop it from the **record**, which costs the user the same third run.

*What the trace found.* `publish_and_audit` never re-deploys a config (`publish.py:16-18`, `:891` — "idempotent Delta-only `promote_best_model`; NO live-space mutation"), so the deployed `serialized_space` is whatever the optimize task left live. Each lever rollback restores `apply_log["pre_snapshot"]` (`applier.py:4628`), which is a deepcopy of the config the loop handed to `apply_patch_set` (`applier.py:4120`, called with `current_config` at `unified_loop.py:3282-3289`) — post-attach. So the view stays live in all four cases: a late champion carries it, a fully-regressing loop reverts *to* it rather than past it, a mid-loop exception leaves it untouched, and a failed lift removed it before the levers ever ran.

| Case | In the deployed `serialized_space`? | `genie_opt_mv_created_objects.status` | Test |
|---|---|---|---|
| 1 — lift passes, loop improves, champion is a late iteration | **Yes.** Every accepted attempt built on the post-attach config | `ATTACHED`, true | `test_case_1_a_late_champion_still_carries_the_metric_view` (run twice: live read-back and unavailable) |
| 2 — lift passes, loop regresses, finalize reverts to baseline/champion | **Yes.** Lever rollback restores the loop's own `pre_snapshot`, which is post-attach — it reverts *to* the attach, not past it. The expected defect was real but sat one layer over: the champion **row** pointed at a pre-attach config | `ATTACHED`, true only after the `observed_config_json` re-point below | `test_case_2_a_regressing_loop_keeps_a_view_that_passed_its_own_lift`, `test_case_2_the_champion_record_is_repointed_at_the_attached_config`, `test_case_2_the_submitted_baseline_config_is_left_pre_attach` |
| 3 — lift passes, loop errors mid-way and the run reverts | **Yes.** The exception propagates past the terminal exit without touching the space | `ATTACHED`, true — and deliberately unreconciled, because reconciliation runs at the terminal exit this case never reaches and the row is already correct. Correct *because the attach completed first*; an error inside the attach phase itself is the unprotected window below | `test_case_3_a_mid_loop_failure_leaves_the_status_alone` |
| 4 — lift fails, attach already reverted, loop then runs normally | **No**, correctly — the attach phase detached it before attempt 1 | `DETACHED` with the lift report attached; the UC object is never dropped. (An *ungradeable* lift is the separate case: it also reverts, but leaves the object `CREATED`) | `test_case_4_a_failed_lift_leaves_no_view_and_no_claim` |

*The defect was one column.* Iteration 0's config columns are written **before** the attach phase (`write_iteration(..., config_snapshot=current_config)` at `unified_loop.py:2926-2937`, phase at `:3028`), and `_stamp_terminal` (`:2537-2566`) rewrites neither. When no lever attempt is accepted, that pre-attach row is also the **champion** — and `revert_optimization(target="champion")` resolves to `observed_config_json or config_json` off the `is_champion` row (`revert.py:692`, `:733`). So the button a user presses to *keep* the optimized configuration would strip a consent-backed view that had already measured positive. The same row simultaneously carried `best_config_version_id = _stable_config_id(current_config)` — the post-attach hash (`:2436`, `:2564`) — so it contradicted itself.

*The fix restores an existing contract rather than inventing one.* `observed_config_json` is defined as the authoritative `serialized_space` as the API reports it after the write (`state.py:962-966`, `ddl.py:98`) — what is deployed. After a kept attach, iteration 0's value is simply **stale**, so `state.update_iteration_observed_config` re-points it and the attach phase calls that on the ATTACHED path only. `revert.py`, champion selection and the accuracy arithmetic are all untouched.

*The two columns disagree on purpose. Do not "fix" it.* After a kept attach, iteration 0 carries a **post-attach `observed_config_json`** and a **pre-attach `config_json`**, and that asymmetry will read as a bug to anyone who finds it without this paragraph. It is not one, because the columns answer different questions. `observed_config_json` answers *what is deployed*, which is why revert and champion selection read it. `config_json` answers *what was measured* — and iteration 0's accuracy genuinely was measured against a space without the metric view. That is the whole point of the ordering in MV-D16(b): the baseline is deliberately pre-attach so `mv_lift` has an uncontaminated comparand. **Rewriting `config_json` to match `observed_config_json` would silently falsify the baseline that every accuracy delta in the run is measured against** — the lift report, each lever attempt's accept/reject decision, and the run's reported improvement all subtract from iteration 0. The record would become self-consistent and wrong, which is strictly worse than stale, and nothing would fail: the numbers would still add up, against a baseline that never existed. `test_case_2_the_submitted_baseline_config_is_left_pre_attach` exists to make that edit fail a test rather than pass review.

*Status truthfulness is enforced, not assumed.* `mv_attach.reconcile_attached_objects` runs at every post-attach loop exit and demotes any `ATTACHED` row whose identifier is absent from the config the run ends on, because an `ATTACHED` row pointing at a space that no longer references the view is worse than no row — Prompt 9's re-run flow and Prompt 13's UI both read it. Demotion is a status correction: it never drops the UC object, and it leaves verified rows completely untouched so it is idempotent across the loop's several exits.

*Reconciliation is demote-only, and that is a property rather than a behaviour.* It writes exactly one status, `DETACHED`, and it considers only rows that already claim `ATTACHED` — a `CREATED` or `DETACHED` row is skipped **even when its identifier is present on the final config**. The asymmetry is deliberate: an identifier on `data_sources.metric_views` proves only that something put it there, not that it came through MV-D1's consent gate, so promoting on that evidence is exactly how an attach that bypassed the gate would acquire a legitimate-looking `ATTACHED` status. `ATTACHED` is written in one place only — the attach phase, after it has validated the consent row and the created object — and reconciliation is a check on that claim, never a second way to make it. Pinned by `test_reconciliation_never_promotes_a_row_the_config_happens_to_carry` (parametrized over `CREATED` and `DETACHED`, with the identifier deliberately *present*, which is the shape that would tempt a promotion), `test_reconciliation_writes_no_status_other_than_detached` (asserted over the whole `MV_CREATED_OBJECT_STATUSES` vocabulary, so a status added later cannot quietly become writable), and `test_reconciliation_ignores_a_non_attached_row_the_read_lets_through`. The filter is applied twice — on the read and again per row — so the property survives an edit to either; the per-row half is what the third test exercises, by stubbing the read to hand back a `CREATED` row.

*One window is unprotected, and the reason to leave it is the property above.* Between the attach PATCH landing (`mv_attach.py:515`) and the `ATTACHED` status write (`:611-623`) sits the entire lift eval — the longest operation in the phase. An exception anywhere in there leaves the view **live in the space** with the row still saying `CREATED`. This fails in the safe direction: it **under-claims**, and the direction that would actually hurt — `ATTACHED` recorded for a view that is not there — cannot arise here, because nothing between that write and the loop's exit removes the view. `attach_patch_id` is stamped on the `CREATED` row immediately after the PATCH (`:541-551`), so a reader can distinguish "the PATCH landed, the verdict is unknown" from "never attached"; only the two Delta writes between the PATCH and that stamp are fully ambiguous. We are **not** adding an exception handler to close it, because the only thing such a handler could do is read the live config and promote `CREATED` → `ATTACHED` — which is precisely the promotion path the demote-only property forbids, and for the same reason: config presence is not consent. A conservative under-claim costs the user a re-run; a promotion path costs the consent chain its meaning. Prompt 9 and Prompt 13 must therefore treat `CREATED` as "not attached, may still be live" rather than as "not live", and Prompt 9's re-attach must be idempotent against an identifier already on `data_sources.metric_views`.

*One dependency the trace surfaced, worth knowing before Prompt 8.* The loop replaces its in-memory config from a live GET each iteration (`unified_loop.py:3382-3390`, preferring the observed read-back over its own submitted config). A late champion therefore carries the view because the attach really PATCHed the space — not because the loop remembers it. Both paths are asserted: the four cases are tested under a live read-back *and* under an unavailable one, the second proving the attach survives through the loop's own config lineage. Should a future API normalization ever drop `metric_views` from a read-back, the loop would lose it silently and reconciliation is what turns that into a truthful `DETACHED` rather than a lie.

**MV-D19 — Lineage grain for L (DECIDED at Prompt 6a — (b) column grain).** Recorded when Prompts 6a and 6b were drafted, so no other prompt settles it by accident. **L carries the blend's largest weight (0.35, `MV_SCORE_WEIGHTS`) and had no producer** — the advisor constructs an empty `LineageOverlap()` and reports `UNAVAILABLE` (`mv_advisor.py:664`, `:725`); Prompt 6a lands the producer in `optimization/mv_signals.py`. The signal recon (Q2) found the grant surface splits the options: the SP's system-schema grants (`WATCH_SYSTEM_GRANTS`, `scripts/deploy_lib/uc.py:30-43`, applied by `scripts/grant_permissions.py`) already include `system.access.table_lineage` SELECT, but `system.access.column_lineage` is **not granted**. The two options that were weighed:

- **(a) Table-grain L on the existing grant.** No install change and no new permission ask — but `LineageOverlap` and `lineage_overlap_score` (`mv_scoring.py:159`, `:432`) are written and documented for *column* sets, so the normalization and every docstring that says "column" would have to be restated for table grain, and the POV Part 3 arithmetic re-derived at that grain. The recon's RECOMMENDATION sanctioned this as the shippable first version.
- **(b) Column-grain L.** Matches POV Part 3 as written, at the cost of a new grant row in `WATCH_SYSTEM_GRANTS`, an upgrade path for existing installs (re-run `grant_permissions.py`), and a documented degradation when the grant is absent: the read fails, L is `UNAVAILABLE` with the missing grant named in the recorded reason — never a silent zero.

**Resolution: (b) column grain. Table grain was rejected as a correctness finding, not a cost preference** — record it this way so a future reader does not reopen it as "why didn't they just take the free grant." `lineage_overlap_score` (`mv_scoring.py:432`) is `Jaccard(candidate_columns, reference_columns)` by construction, and grain changes what that number *means*, not just its resolution. Walk the dominant candidate type, `NEW_METRIC_VIEW → REFERENCE_LINEAGE_FOOTPRINT`: at table grain the reference set is every source table the space's queries touch, and the candidate's tables are — necessarily, since the candidate was mined from the space's own SQL — a subset of that footprint. Intersection collapses to the candidate's tables and union to the footprint, so L degenerates to `|candidate_tables| / |footprint_tables|`: it stops answering "how squarely does this measure sit on established lineage" and starts scoring "how large a fraction of the estate does this candidate sprawl across" — penalizing the clean single-table measure (usually the best metric-view candidate) and rewarding sprawl. The signal is not merely coarsened; for the primary case it is **inverted**, and no other table-grain operator rescues it (coverage `|cand ∩ footprint| / |cand|` goes to ~1.0 for every candidate — no discrimination). Table grain remains meaningful only for the governed-MV reference cases (`REPLACE_RAW_TABLE` / `ADD_MEASURE`, a small candidate set vs a specific MV's small set), which are not the volume. A table-grain L would report `COMPUTED` and lift the coverage cap — potentially to HIGH — on a number that is noise-or-inverted at 0.35 weight, which is the exact failure MV-D15 exists to prevent ("a signal never measured is not a signal measured zero"): an honest `UNAVAILABLE` renormalizes the weight away and keeps the ranking trustworthy at MEDIUM, while a false `COMPUTED` poisons the top of the ranking and claims a confidence tier it did not earn. Column grain is also the **lower-churn** option inside the code: the `LineageOverlap` fields were written for columns, so (b) needs zero docstring restatement, zero POV Part 3 re-derivation, and no touch to the `LineageOverlap` construction sites in `test_mv_scoring.py` — the mv_scoring churn is entirely a cost of (a). Its only real cost is external: one `WATCH_SYSTEM_GRANTS` row for `system.access.column_lineage` SELECT plus the re-run-`grant_permissions.py` upgrade path — the same path every other system grant in that list already relies on. *Empirical caveat, folded into 6a rather than discovered at deploy — and now probed.* `system.access.column_lineage` is Public Preview with a one-year retention window (POV Caveat 8), so column-grain L will be `UNAVAILABLE` in workspaces where the grant or the data is absent — which is honest (coverage falls back to 0.65, cap MEDIUM) and still strictly better than an always-present inverted signal. The Prompt 6a probe was run against workspace `fevm-serverless-stable-6t92c3` and came back favorable on every point that shapes the read: (1) the table **carries `entity_metadata.genie_space_id`** inside its `entity_metadata` struct — the same field `table_lineage` exposes — so `_footprint_sql`'s space-scoping is correct and no source-table-only fallback is needed; (2) it is **heavily populated** — ~117M rows in the last 90 days, of which ~458K are Genie-attributed across **2,393 distinct spaces** — so L will genuinely contribute rather than being perpetually `UNAVAILABLE`; and (3) the exact `SELECT DISTINCT source_table_full_name, source_column_name … WHERE entity_metadata.genie_space_id = … AND source_table_full_name IN (…)` shape executes and returns real `(table, column)` footprint rows for a live space (column names arrive upper-case, which is why the producer lower-cases both sides before the Jaccard). The probe ran as an interactive user with the necessary grants; the one remaining deploy step is applying the new `WATCH_SYSTEM_GRANTS` row to the **app service principal** via `grant_permissions.py`, and MV-D15 still gives the graceful landing (`UNAVAILABLE`, grant named) on any workspace where that has not yet run.

*Settled regardless of grain, per the recon's contradiction #3 (POV:266):* lineage evidence is **computed under the SP and filtered at presentation** per the viewing user's grants — never computed under OBO in-job, which the job's identity makes impossible anyway (MV-D1). And per MV-D15's vocabulary: a lineage read that runs and resolves to genuinely disjoint sets is `EMPTY` (a measurement); a read that could not run is `UNAVAILABLE` with a recorded reason. One-year lineage retention and Public Preview status (POV Caveat 8) are reasons, not surprises.

**MV-D20 — The create path runs under OBO through the warehouse helper, never the read-only executor and never the SP fallback (OPEN — decided at Prompt 9).** Recorded when the Prompt 9 gap check found two paths that both *look* like "how the backend runs SQL" and only one is correct for a `CREATE VIEW … WITH METRICS`. MV-D1 already settled *that* creation moves to the backend under OBO at trigger time; MV-D20 settles the two mechanical questions that decision leaves open, because getting either wrong silently violates MV-D1 rather than erroring.

*Which executor.* The backend's general SQL helper, `execute_sql` / `validate_sql_read_only` (`backend/sql_executor.py:32`), hard-blocks `CREATE|ALTER|GRANT|DROP|DELETE|…` and requires a statement to start with SELECT or WITH. That guard protects the `/api/space` ad-hoc SQL surface and **must not be relaxed to let the create path through** — doing so would widen a security boundary far beyond this feature. The create/`EXPLAIN`/`DESCRIBE EXTENDED`/`MEASURE()`-smoke statements instead go through `genie_space_optimizer.common.warehouse.sql_warehouse_query` (`warehouse.py:34`), which carries no read-only guard — the same path the entitlement probe already runs under OBO (`backend/services/mv_entitlement.py:194`). So the spec's "reuse the backend's OBO SQL path if one exists, else the SDK" resolves to: a path exists, it is `sql_warehouse_query` called with the OBO client, and no new executor is invented.

*Under whose identity.* The trigger endpoint today resolves its client with `get_workspace_client()` (`auto_optimize.py:1472`), which **falls back to the service principal** when no OBO token reached the request. The create path must instead take the OBO client from `require_obo_workspace_client()` (`backend/services/auth.py:117`), which **hard-fails** rather than falling back. A metric view created as the SP lands ownership on the app identity — precisely the outcome MV-D1 exists to prevent — and a silent SP fallback would produce it with no error. The hard-fail is the enforcement of MV-D1's identity contract, not an ergonomic choice. Reads (list proposals, load DDL artifact) may keep the SP-tolerant `get_workspace_client()`; only writes that create or drop a UC object require the hard-fail client.

*Consequences carried by Prompt 9.* Post-create verification (`DESCRIBE EXTENDED` asserting `Type: METRIC_VIEW`) and the fan-out `MEASURE()`-with-`GROUP BY` smoke test are themselves warehouse statements on this same OBO path, not on `execute_sql`. Any later edit endpoint uses `ALTER VIEW … AS $$…$$` (grants-preserving), never `CREATE OR REPLACE` — also on this path. The route tests must include a case proving a create is refused (not silently SP-run) when no OBO token is present.

**MV-D21 — The backend reaches MV state through new warehouse helpers pinned to the `mv_state` schema, not through cross-package Spark imports (OPEN — decided at Prompt 9).** Recorded when the gap check found the Prompt 9 route list quietly assumes the `mv_state` accessors are callable from the backend. They are not. `mv_state.py`'s accessors (`load_mv_candidates`, `upsert_mv_created_object`, `update_mv_created_object_status`, `record_mv_candidate_decision`) are **Spark-based and run in-job**; the backend imports nothing from `genie_space_optimizer.optimization` and reads GSO-written Delta through the warehouse `wh_*` helpers in `common/warehouse.py`. Only two MV warehouse helpers exist today — `wh_upsert_mv_consent` (`warehouse.py:473`), `wh_load_mv_consent` (`:540`) — both added in Prompt 5.

*What this obliges Prompt 9 to build.* **Five** new `wh_*` helpers (the count moved from four to five during execution — see below), mirroring the Spark accessors over the SQL-warehouse path the backend actually uses: `wh_load_mv_candidates` (GET `/runs/{run_id}/mv-proposals`), `wh_record_mv_candidate_decision` (POST `/mv/proposals/{id}/decision` — approve sets `approved_for_rerun`, reject writes suppression), `wh_upsert_mv_created_object` (the create-and-attach record), `wh_update_mv_created_object_status` (the status transition on drop), and `wh_load_mv_created_object` (the fifth, added at execution: the drop route needs a *read* keyed on `(run_id, suggestion_id)` to authorize against `created_by` and confirm `status = DETACHED` before it drops anything — a write-only helper set could not enforce MV-D6). This is real work the "add four routes" framing hides, and it is the reason Prompt 9's backend is larger than its route count suggests. The one route that needs **no** new helper is GET `/runs/{run_id}/mv-ddl`: the existing `_load_latest_artifact(run_id, "mv_candidate_ddl")` (`auto_optimize.py:437`) already reads the advisor's DDL artifact — note the kind is `mv_candidate_ddl` (`mv_advisor.py:1155`), not `mv_ddl`.

*The drift hazard, and the pin that closes it.* Two code paths now read and write the same three `genie_opt_mv_*` Delta tables in two languages — `mv_state` (Spark, in-job) and these `wh_*` helpers (warehouse SQL, backend) — the exact "two readers of one field start disagreeing" failure the Prompt 6c harvest guarded against for trusted assets. The `wh_*` helpers must be pinned to the `mv_state` column contract (a shared column-list constant, or a test that asserts the warehouse helper's projected/written columns match `mv_state`'s), so a schema change on either side fails a test rather than a demo. That pin is part of Prompt 9's VERIFY, not a follow-on. `created_by` (the consenting user) already exists on the created-objects row (`mv_state.py:468`), so the drop route's non-owner 403 has a field to authorize against without a schema change. The pin lands as `packages/genie-space-optimizer/tests/unit/test_wh_mv_state.py`.

**MV-D22 — The backend replays the stored YAML with revalidation; it does not regenerate (supersedes MV-D15's regeneration clause).** Recorded during Prompt 9. MV-D15 directed Prompt 9 to regenerate the YAML under the backend's probe rather than replay the run-1 artifact, on the premise that the OBO backend would see richer capabilities than the SP job. That premise is false: `generate()` reads capabilities off `profiling.capabilities` (`mv_yaml.py:288`, rung gated at `:668`), capabilities derive from the compute type not the identity, and the backend probes via SQL warehouse exactly as the job does — so MV-D13 pins both to the same UNKNOWN→unavailable floor, and a regeneration would produce byte-identical YAML to the job's rung-3 render. Regeneration is unachievable regardless, because `generate()`'s inputs (`MetricViewCandidate`, `MvProfiling`) are not persisted. Resolution: the advisor additionally persists the rendered `yaml_text` in the `mv_candidate_ddl` payload (`mv_advisor.py` `_write_ddl_artifact`); at trigger time the backend recovers it, re-wraps it for the consented target via `create_ddl(consented_full_name, yaml_text)` — necessary because `_proposed_object` (`mv_advisor.py:721`) derives the render-time name from source-table location, before consent exists and possibly differing from it — re-validates under the fresh probe, and hard-aborts the create (drops the suggestion) if revalidation returns a `downgrade_to` below the stored rung. The abort guard makes create-time safety independent of MV-D13 rather than silently dependent on it. Firewall unchanged: the persisted `yaml_text` is the already-echo-checked shipped body; because it is immutable between render and create, the backend accepts the render-time echo result and does not require the benchmark oracle at trigger time (`validate` reporting `NOT_COMPARED` without the oracle is not a create failure). The create-and-attach orchestration lives in the backend `mv_create.py` service (MV-D20); the engine's `trigger_optimization` reaches it through an `mv_attach_hook` callback so the dependency arrow stays backend→engine. The abort guard is pinned by `test_mv_create.py::test_revalidation_downgrade_aborts_the_create`.

**MV-D23 — Whether a candidate can exist, and be created, without a run (OPEN — decided at Prompt 13.5).** Recorded from a post-Prompt-9 review of the question the feature cannot currently answer: *can the advisor suggest a metric view for a Genie Agent that has never been optimized?* Today it cannot, and the reason is not a policy — it is four `run_id` constraints that Prompts 1 through 9 each added for good local reasons and that compose into a hard coupling nobody chose. They are recorded here because three of the four cannot be relaxed by the `ADDITIVE_COLUMN_MIGRATIONS` mechanism, so the decision has a schema cost that a later prompt must not discover mid-flight.

*The four couplings, precisely.* **(1)** The corpus is gated, not merely seeded: `_advise` opens with `load = load_iteration_zero_corpus(...)` and returns `STATUS_SKIPPED` on `not load.usable` (`mv_advisor.py:922-928`), so the curated half Prompt 6c shipped is strictly additive to a gate it cannot open — and `curated_corpus_entries` additionally consumes `load.applied_config`, itself derived from the same iteration rows. A space with no eval run has no corpus even when its `example_question_sqls`, `sql_snippets.measures` and benchmark answers are full of recurring measures. **(2)** `genie_opt_mv_candidates.run_id` is `NOT NULL` (`ddl.py`, the candidates DDL) with the comment "the run whose advisor phase last proposed or refreshed this candidate. A candidate outlives it (MV-D1)" — the comment already concedes the candidate outlives the run, but the column still requires one. **(3)** The rendered body lives in exactly one place: `_write_ddl_artifact` persists `yaml_text` into a `genie_opt_artifacts` row (`artifact_kind = 'mv_candidate_ddl'`), and that table is `run_id NOT NULL` and `PARTITIONED BY (run_id)`. So MV-D22's replay contract — the backend recovers the immutable body rather than regenerating it — has nothing to recover for a candidate proposed outside a run. This is the sharpest of the four, because it makes MV-D22 and a standalone create mutually exclusive as currently built. **(4)** The created-objects ledger is `PARTITIONED BY (run_id)` and keyed `(run_id, suggestion_id)`, and all three backend writers take `run_id: str` non-optionally (`warehouse.py`, `wh_upsert_mv_created_object`, `wh_update_mv_created_object_status`, `wh_load_mv_created_object`).

*What is already fine, and should not be re-solved.* `wh_load_mv_candidates` was built during Prompt 9 accepting `target_space_id` **or** `run_id`, with a guard that refuses to scan every space. The space-scoped read therefore already exists; only the route above it (`GET /runs/{run_id}/mv-proposals`) is run-keyed. The 6a/6b signal producers read through `sql_warehouse_query` rather than Spark, so L and D need no second producer for a backend-side caller. And with L and D wired, `evidence_coverage` is no longer 0.50 — a standalone candidate is not structurally capped at MEDIUM, which was the strongest argument against this path while it held.

*Three options, none chosen here.* **(a) A sentinel advice run.** Standalone advice writes a `genie_opt_runs` row distinguished by kind, and every FK, partition, helper and MV-D22 replay path works untouched. The cost is honest but real: a run row that never ran an eval, which the run-history list and every accuracy aggregate must then exclude by construction rather than by convention — and a filter that is forgotten in one place produces a run with no accuracy rather than an error. **(b) Relax `run_id` and re-key.** Truthful to the model, and the candidate row's own comment argues for it, but `ADDITIVE_COLUMN_MIGRATIONS` can only *add* columns — dropping `NOT NULL` on couplings (2) and (3) and re-partitioning (4) needs table recreates on three tables that already hold customer data on any workspace that has run this branch. **(c) Separate standalone tables.** Cleanest schema, worst outcome for the drift hazard MV-D21 just spent a pin closing: a third reader and writer of the same three concepts in a third dialect. A leaning, for 13.5 to accept or overturn with reasons: **(a)**, with the exclusion expressed as a predicate in the shared run-list query rather than as a rule contributors must remember, plus an additive `yaml_text` column on the candidate row so coupling (3) stops depending on the artifact partition at all — the one genuinely additive part of (b), worth taking whichever option wins.

*What this obliges the prompts between here and 13.5.* Prompt 12's graph and Prompt 13's cards must be built against a proposal payload whose `run_id` is **presentational, not structural** — no component may key state, fetch, or identity on it — because 13.5 supplies the same `ScoredProposal` shape from a space-scoped source and a payload divergence rebuilds both. Prompt 11 must read approved proposals **by space** (see its amended body). And no prompt before 13.5 may add a fifth `run_id` coupling to the MV surface without recording it here.

**MV-D24 — Bring-your-own metric view: a user-created view can be reported back, verified, and attached (OPEN — decided at Prompt 13.5).** Recorded at the Prompt 10 mockup review, from the question the suggest-only output invites but the system cannot complete. The DDL panel hands the user a copy-ready `CREATE VIEW … WITH METRICS` and the GRANT to go with it — an explicit invitation to create the view themselves, in a SQL editor, under their own identity, in whatever schema they choose. The moment they accept that invitation, the feature goes blind. Three dead ends compose: `mv_attach` skips any identifier without a `CREATED` ledger row for the run (`mv_attach.py:478`) and any row whose `created_by` mismatches the consent's `granted_by` (`:484`); the ledger's only writer is the backend create path (`mv_create.py`), so a self-created view never acquires a row; and the drop and status routes key on that same ledger. Copied DDL is therefore a one-way exit — the app can neither attach, measure, nor acknowledge the resulting view. The contrast that makes this a defect rather than a scope choice: the **Create Agent already attaches pre-existing metric views natively** (`data_sources.metric_views[]` accepts any UC identifier the user holds), so the workbench's two surfaces currently disagree about whether a user-created metric view is a first-class object.

*Resolution shape, for 13.5 to accept or overturn with reasons.* A registration path: the user reports the identifier (a [I created this myself] affordance on the proposal card, or a free-standing input on the IQ Scan panel); the backend **verifies under OBO** — `DESCRIBE EXTENDED` asserts `Type: METRIC_VIEW`, the YAML is recovered via `DESCRIBE … AS JSON` `view_text`, `mv_yaml.validate` lints it, and when the user claims it implements a specific proposal the dedup fingerprint is compared so the claim is checked rather than trusted; on success a ledger row is written with a `provenance` discriminator (`OBO_CREATED` | `USER_CREATED`, an additive column — `ADDITIVE_COLUMN_MIGRATIONS` suffices here) and `created_by` recording the verifying user; the normal attach-and-lift path then runs on the next run, which requires a **sanctioned, narrow relaxation** of the `mv_attach.py:484` identity guard for `USER_CREATED` rows — the guard's purpose is to stop the job attaching an object the consent chain never covered, and a verified registration IS that coverage.

*Two invariants stated now so 13.5 does not relitigate them.* **The app never drops a `USER_CREATED` view** — stronger than detach-never-drop: we did not create it, we do not own its lifecycle, and the drop route must refuse on provenance, not merely on status. And **registration is verification, not trust**: an identifier that cannot be verified (not a metric view, not visible to the caller, YAML fails validation) is refused with the reason, never recorded provisionally. Permission guidance is already solved and must be reused, not rebuilt: `mv_entitlement.probe` + `_remediation_sql` render the exact GRANTs for the self-create path, and the frame-4 GRANT panel covers the audience grant.

**MV-D25 — Schema-only (cold-start) metric view suggestion (OPEN — owner: the create-agent branch, after Prompt 16; NOT decided on this branch).** Recorded before Prompt 12, from a direct capability question. The engine cannot suggest a metric view from tables, column metadata, and data sampling alone, and this is by construction, not omission: the sole candidate producer is `candidate_from_measure` over a `FingerprintRecurrence` (`mv_advisor.py:646`) — every candidate is a measure that RECURRED in SQL somebody or something wrote. No SQL corpus, no candidates, and Delta 9's honest limit says that state presents as `EMPTY`. The blend enforces the same epistemology: Y is recurrence, S needs a candidate to compare, L and D score evidence about usage — none of the four can conjure a proposal from a schema. The reason this is a feature and not a gap on THIS branch: proposals arrive pre-trusted (MV-D8's premise), and a recurrence-backed proposal asserts "people already compute this"; a schema-derived one asserts "this looks computable" — a categorically weaker claim that must never share a confidence scale with the first. Where the capability actually lives: the create-agent branch, whose evidence route is profiling by design — `plan_builder`'s analytics section already generates measures and join_specs from table context via LLM, `assess_readiness` already grades modelability, and the EXACT-uniqueness probe planned there supplies MV-D14's missing evidence. If a cold-start mode is ever wanted on the IQ Scan surface too, the shape is: a separate producer emitting a new candidate provenance (`SCHEMA_DERIVED`), hard-capped below the recurrence tiers, suggest-only forever, never blended into the LYDS score — but that is that branch's decision to take, with its own MV-D entry.

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

A. New patch type (the applier fix):
- Register mv_attach_data_source in PATCH_TYPES in common/config.py, classified
  HIGH_RISK, and implement a REAL applier action in optimization/applier.py that
  mutates data_sources.metric_views in the space config — read
  _apply_action_to_config (:3409) and the Lever-2 MV no-op (:3914-3932) first,
  and route the new type through a genuine mutation, not the render-only path.
  Patch dict shape stays type/target/new_text/old_text.
  [MV-D16(a), superseding this prompt's original text] Do NOT add it to the
  unified-loop allowlist. _ALLOWED_PATCH_TYPES (unified_loop.py:79-93) is the
  LLM-PROPOSAL surface, not the lever surface: apply_patch_set never reads it,
  and listing attach there would let the loop's LLM invent a UC identifier and
  attach it with no consent row. The attach phase calls apply_patch_set direct.
  [MV-D16] mv_remove_raw_table is DROPPED. Use the existing remove_table type
  for optional table replacement — already HIGH_RISK, already rendered and
  applied, already outside the LLM allowlist.
- Tests: apply -> serialized_space contains the identifier; snapshot revert
  removes it — via applier.rollback against apply_log["pre_snapshot"], NOT
  integration/revert.py, which is a backend surface whose active-run guard
  rejects mid-run by design [MV-D16]; render_patch output is reviewable.

B. mv_attach + mv_lift phases, ordered AFTER iteration-0 completes and BEFORE
   the first lever patch — INSIDE run_unified_optimization_loop, not before the
   call [MV-D16(b), superseding this prompt's original "BEFORE the unified loop"
   wording, which contradicted step 3 below because iteration-0 lives inside the
   loop at unified_loop.py:2854-2859; attaching before iteration-0 would make the
   baseline corpus post-attach SQL and contaminate the evidence Prompt 6's
   advisor fingerprints]:
1. Read job widgets: mv_attach_views (JSON list of identifiers), mv_consent_id.
   Empty -> skip both phases silently.
2. Validate: the consent row exists in genie_opt_mv_consents with verdict
   SUFFICIENT and reverified_at_trigger set; each identifier exists in
   genie_opt_mv_created_objects with status CREATED and was created_by the
   consent's granted_by. Any mismatch -> persist status row, skip (never
   attach an object the trigger flow didn't just create). [MV-D16] The
   mv_consent_id parameter value IS the probe_id — genie_opt_mv_consents is
   keyed on probe_id and there is no consent_id column.
3. Iteration-0 baseline runs first (existing behavior — do not reorder it).
4. Apply the mv_attach patch(es) via apply_patch_set.
5. mv-lift eval: run_subset over the AFFECTED question ids recorded on the
   proposal via the Prompt 2 seam, labeled "mv_lift"; store lift_report vs
   iteration-0 and both eval_run_ids on genie_opt_mv_created_objects; set
   status ATTACHED. lift_report_json persists LiftReport.to_dict() verbatim.
6. On regression per lift_report: revert to the pre-attach snapshot (detach)
   via applier.rollback against apply_log["pre_snapshot"] [MV-D16], set status
   DETACHED with the report attached. NEVER drop the UC object.
   Sandbox mode is out of scope for the job (sandbox creation/teardown is a
   backend concern under MV-D1).
7. The lever attempts then run on whatever foundation now exists.
Each phase try/except-isolated with Delta status rows; tests cover happy path,
missing consent row, identifier/creator mismatch, regression->DETACHED, and
that a phase failure leaves the loop running.
```

### Prompt 6c — Harvest the curated half of the corpus

*Sequenced after Prompt 7 and not yet run. Drafted during the Prompt 7 review, from a gap the review found rather than from the original plan: POV Part 2 calls "Optimized-space harvest" the highest-value signal and TL;DR bullet 2 calls an optimized space the single highest-precision seed, but Prompt 6's pipeline loads only iteration-0 generated SQL and benchmark SQL. The generated half is mined; the curated half is not. Prompt 7 closed the safety-critical slice of this — trusted assets now reach the CONFLICT gate — which is deliberately a different thing from seeding candidates from them.*

```
Per POV Part 2 ("Optimized-space harvest") and the MV-D9 rule on refreshing what
the gap report quotes. Prompt 6's corpus is iteration-0 generated SQL plus
benchmark SQL; the curated half of the space is unmined. Extend the corpus, do
not add a second pipeline.

SOURCES, all read from the APPLIED serialized_space and the run's patch rows —
never from a fresh Genie GET, and never from in-memory loop state:
- instructions.example_question_sqls (trusted-asset SQL). NOTE: Prompt 7 already
  reads this field for the CONFLICT surface via
  mv_scoring.trusted_asset_definitions. Reuse that reader or extend it; a second
  reader of the same field is how the two start disagreeing about what a
  trusted asset defines.
- GSO join specs.
- TVF / SQL function definitions.
- existing data_sources.metric_views entries.
- the run's genie_opt_patches rows, so measures GSO itself introduced count as
  curated rather than generated.

ROUTE EVERYTHING THROUGH mv_fingerprint's EXISTING EXTRACTORS. extract_measures,
extract_dimensions, extract_filters, extract_join_keys, canonicalize_expr. No
second parser, no second canonicalizer, no local normalization of a column name
or a date-part unit. MV-D10's invariants (a)(b)(c) are established and must not
be re-derived: one measure is one row, and both fingerprint levels have to keep
agreeing about that.

EVIDENCE TIERING — this prompt must DECIDE MV-D17, which is recorded as open.
Y weights every corpus occurrence equally, so harvesting curated sources into the
same corpus makes 60 occurrences of generated SQL outscore 3 occurrences of
trusted-asset SQL — inverting the POV's own evidence hierarchy. Read MV-D17 for
the three options (provenance multiplier inside Y / separate signal with its own
weight / tier floor for curated-sourced candidates) and the cost of each,
including what a separate signal does to the MV-D15 coverage arithmetic. Register
the choice as MV-D17 in the same commit, with the reasoning, and pin it with a
test that a curated-sourced candidate with few occurrences is not outranked by a
high-recurrence generated one.

FIREWALL BOUNDARY — state it in the code, not only here. Benchmark SQL and
trusted-asset SQL are legitimate FINGERPRINT input: canonicalization erases
literals, and MV-D10(b) gives the mechanically-auditable invariant (no quote
character survives a canonical form). They are NOT legitimate content for a
shipped metric view. The BEST FOR echo check in mv_yaml already guards that
boundary and 6c must not weaken it: no harvested text may reach a comment, a
display_name, or a synonym, and provenance travels as ids. Extend
optimization/leakage.py if a new surface needs covering rather than adding a
second scanner. Add a property test over the harvested corpus asserting no quoted
literal survives, mirroring
test_no_corpus_statement_leaks_a_quoted_literal.

VERIFY: every source above reaches corpus_scan through an existing extractor
(asserted by test, not by inspection); MV-D17 decided and registered with its
pinning test; the echo check still fails a candidate whose comment echoes a
benchmark question; gap report refreshed per MV-D9; GSO and backend suites green.
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

### Prompt 6a — Signal producers for L and D

*Sequenced after Prompt 8 and not yet run. Drafted from the signal recon's RECOMMENDATION (docs/design/mv-advisor-signal-recon.md), which proposed splitting the original Prompt 6 into signal producers (6a) and the advisor phase (6b). The phase half shipped as Prompt 6, deliberately against injected fixtures — `mv_scoring` takes `LineageOverlap`, `DemandSignal` and the embedding client as inputs, so the advisor could be built and tested in full before its data existed. Two of the recon's four 6a items shipped along the way (`write_artifact`'s `content_hash` passthrough; `SemanticMatch.status`). What remains under these names is the producers (this prompt) and the wiring (Prompt 6b). The arithmetic that made this the priority after Prompt 8: before 6b wired the producers, with L (0.35) and D (0.15) `UNAVAILABLE`, `evidence_coverage` was 0.50, nothing could exceed MEDIUM, and the advisor's ordering — not just its scores — was an artifact of Y. The recon's bottom line stands: a first version ships without S and without full D, but not without L, because zeroing the largest weight distorts ranking rather than merely lowering scores.*

```
Per the signal recon (Q2, Q3, RECOMMENDATION) and MV-D19, which this prompt must
DECIDE: build the two missing signal producers as standalone, injected inputs.
This prompt does NOT touch mv_advisor.py or run_optimize.py — the producers are
built and tested against warehouse-row fixtures here, and wired in Prompt 6b, so
the seam the advisor was built on stays reviewable.

PLACEMENT: a new module, optimization/mv_signals.py. Both producers read as the
service principal through the existing warehouse seam
(common/warehouse.sql_warehouse_query, warehouse.py:34) — the same identity and
read path every other in-job system-table read uses. No OBO in-job (MV-D1;
POV:266 correction — computed under SP, filtered at presentation).

L PRODUCER — decide MV-D19 first (table grain on the existing
system.access.table_lineage grant, vs column grain requiring a new
WATCH_SYSTEM_GRANTS row and an install upgrade path). Register the choice with
its reasoning in the same commit. The producer reads lineage for the space's
source tables and the candidate's source set, and returns a populated
LineageOverlap (mv_scoring.py:159) — candidate_columns, reference_columns,
reference_kind per candidate type (REPLACE_RAW_TABLE reads a governed MV's
source set; NEW_METRIC_VIEW/ADD_MEASURE read the lineage footprint the space's
queries touch). If MV-D19 lands on table grain, restate
lineage_overlap_score's normalization and every "column" docstring for that
grain in the same commit — do not leave the code claiming a grain it does not
read.

D PRODUCER — a dedicated per-measure query over system.query.history, not a
remap of wide_schema_history (whose SELECT has no duration or user column — the
recon's contradiction #1, already corrected at mv_scoring.py:217). Measure-grain
mapping reuses the one canonicalizer rather than inventing a grain mapper:
fingerprint the history statements through mv_fingerprint.corpus_scan and join
candidate fingerprints against them (MV-D10 — no second parser, by
construction). From the matched statements: frequency = recurrence, distinct
users = distinct executed_by, cost_ms = summed duration, age_days from the most
recent occurrence — a populated DemandSignal for demand_score
(mv_scoring.py:625). Scope the history read to the space's warehouse and a
bounded window; make the window a config constant.

FAILURE SEMANTICS (MV-D15): a read that cannot run — missing grant, missing
table, empty statement_text under CMK, retention window exceeded — returns
UNAVAILABLE with the reason recorded on the payload, never a silent zero. A
read that ran and found nothing is EMPTY. Every producer result carries its
status; the advisor must never have to infer why a signal is missing.

FIREWALL: system.query.history statement_text is raw user SQL and enters ONLY
as fingerprint input — canonicalization erases literals (MV-D10(b)) and no
history text may reach a comment, display_name, synonym, or any shipped
surface. Extend the quoted-literal property test to the history-derived corpus.

VERIFY: producer unit tests against fixture warehouse rows (populated, EMPTY,
and each UNAVAILABLE reason); the MV-D19 pinning test (grant row present if
column grain; normalization restated if table grain); firewall property test
green; both suites via ./scripts/test.sh; mv_advisor.py and run_optimize.py
untouched (assert via git diff, not inspection).
```

### Prompt 6b — Wire the producers into the advisor

*Sequenced after Prompt 6a. A naming note so the recon and this playbook stay reconcilable: the recon's "6b" meant the advisor phase itself, which shipped as Prompt 6 — what this prompt carries under that name is the remaining half, swapping the advisor's empty defaults for 6a's producers. Kept separate from 6a on the recon's own caveat (the one MV-D14's marker records): fixtures prove the consumer, so the wiring owes an integration assertion that a real producer's output reaches the scorer in the shape the scorer expects.*

```
Per the signal recon's RECOMMENDATION and MV-D15/MV-D19: connect 6a's producers
to the advisor phase. Small diff, three change sites, no scoring changes.

CHANGE SITES:
- jobs/run_optimize.py:445-456 — construct the producers beside
  FoundationModelEmbeddingClient and pass them into run_mv_advisor_phase, same
  injection pattern.
- optimization/mv_advisor.py:664 and :672 — stop constructing empty
  LineageOverlap() / DemandSignal(); consume the producer results per
  candidate.
- optimization/mv_advisor.py:725 — advisor_statuses stops hardcoding
  {"L": UNAVAILABLE, "D": UNAVAILABLE} and reports each producer's actual
  status (COMPUTED / EMPTY / UNAVAILABLE with reason) into the evidence
  payload.

THE INTEGRATION ASSERTION 6a OWES: one test that drives a real producer (over
fixture warehouse rows, not a mocked producer) end-to-end into
candidate_from_measure and asserts the scorer received L and D in the shapes
mv_scoring declares — the recon's point that fixtures prove the consumer, so
the producer-to-consumer shape must be proven separately.

COVERAGE FLIP — refresh every committed sentence that states today's coverage
arithmetic, per MV-D9. The register's MV-D17 coverage note and the 6a preamble
state the three coverage states (0.50 today / 0.65 with D / 1.0 with L and D)
and that "nothing can exceed MEDIUM"; after this prompt those are historical.
Re-verify the pinned tier behavior at full coverage: the POV worked examples
already assume coverage 1.0 and must not move; what changes is that HIGH
becomes reachable, so assert capped_tier grants it only at >= 0.80 coverage
with a genuinely computed L.

DEGRADED MODE STAYS FIRST-CLASS: a workspace where the producers return
UNAVAILABLE (missing grant, CMK, retention) must behave exactly as the advisor
behaves today — same scores, same MEDIUM cap, same legible statuses. Pin that
with a test: producers-all-UNAVAILABLE reproduces today's byte-identical
evidence payloads. The upgrade is additive or it is a regression.

VERIFY: integration assertion green; degraded-mode pin green; coverage-text
sites refreshed (register, gap-report Y/coverage rows) per MV-D9; both suites
via ./scripts/test.sh; grep confirms no empty-default LineageOverlap() /
DemandSignal() construction remains in the advisor path.
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
7. IQ Scan panel, the pre-optimization surface (MV-D23). This is the ONLY
   mockup review on this branch, and 13.5 builds this panel, so design it here
   or pay for a second review: the IQ Scan result screen
   (frontend/src/pages/IQScoreTab.tsx) gains an advisory metric-view section
   below the 12 checks, rendering the SAME proposal card as mockup 4 with no
   run context — no "Lift not measured" label (nothing was run to measure),
   no [Re-run with this metric view], and a primary action that opens the
   consent flow directly. Two states, and the second is the common one on a
   new space: (a) proposals found; (b) the MV-D15 EMPTY state — the corpus was
   read and no measure recurred. Copy for (b) must say the scan looked and
   found nothing recurring, NOT that the feature is unavailable or that the
   scan failed; reuse the empty-table-is-not-a-failed-run framing already
   agreed for the MV tables in docs/docs/features/auto-optimize.md. Include a
   third frame for the not-entitled variant, which reuses mockup 3's denial
   banner unchanged.

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
- Re-run state (approved proposals exist for the space). AMENDED after Prompt 9
  (MV-D23): the condition here is space-scoped but the Prompt 9 route is
  run-keyed, so "of the prior run" would make this panel hunt for a prior
  run_id to stand in for a space-scoped question. Do not do that. The helper
  already answers it — wh_load_mv_candidates takes target_space_id OR run_id
  (warehouse.py) — so add the space-scoped route this panel actually needs,
  GET /api/auto-optimize/spaces/{space_id}/mv-proposals?approved_for_rerun=true,
  alongside the existing run-keyed one rather than replacing it (the output
  screen legitimately wants per-run proposals). Prompt 13.5 reuses this route
  unchanged; a prior-run workaround here would be rewritten there.
  Then: proposal checkboxes,
  target read from the proposal/consent, probe call to
  POST /api/auto-optimize/mv/probe on expand; granted/denied states per the
  mockups; "Create and attach" enabled only on verdict SUFFICIENT;
  [Copy grant request] copies remediation_sql.
- Start records the mv_* fields + consent object into the TriggerRequest via
  optimizationRequest.ts. "Suggest only" sends mode=suggest_only, no consent.
- Materialize: DO NOT SURFACE A CONTROL. Amended at the Prompt 11 PLAN review;
  this prompt body is not yet a frozen transcript, so it is corrected in place
  rather than worked around. The gap report outranks the POV and playbook
  (MV-D9) and says plainly at mv-advisor-gap-report.md:1526 that Prompt 11
  must not surface a live materialize toggle "or it will offer a control that
  does nothing". Verified against the code: mv_create.py:230-235 logs the
  request and installs a NON-materialized view, and the EXPLAIN CREATE
  MATERIALIZED VIEW precheck MV-D1 requires exists nowhere in the repo — the
  path is unbuilt, not merely unwired.
  Do NOT render it disabled-with-rationale either. That idiom is right for the
  first-run "Create and attach" state, because there the control becomes
  enabled through something the user can do; a disabled control for an unbuilt
  feature advertises vapor instead.
  DO still plumb the field: mv_materialize stays in GSOTriggerRequest and
  buildOptimizationTriggerRequest keeps clearing it with every other mv_* field
  when the toggle is off, so the later materialization prompt adds a control
  and nothing else. Leave the mockup's "Also materialize" checkbox in place as
  that prompt's design, but annotate it in MvRunConfigMockups.tsx as not
  shipped by Prompt 11, citing the gap-report line — an un-annotated mockup and
  a shipped panel that disagree is how the next reader concludes one of them is
  a bug.
- Component tests (vitest, matching existing frontend tests): probe
  loading/granted/denied, first-run vs re-run gating, consent payload shape,
  and that toggling off clears every mv_* field from the request (mv_materialize
  included, even though no control sets it).
- Mockup disposal, corrected here: MvRunConfigMockups.tsx's header claims
  "Prompt 11 deletes this file". That is wrong and must be fixed, not obeyed.
  Its DenialBanner is reused by frames 7c and 8, which Prompt 13.5 implements,
  and frames.tsx plus mockups.test.tsx pin frames 1-3. Deleting it here breaks
  the suite and removes a component two later frames depend on. Correct the
  header to the real disposal point — after Prompt 13.5 wires the last frame —
  and leave the scaffold standing this commit.
- Add the space-scoped route this panel needs as a SIBLING response model
  (MvSpaceProposalsResponse{space_id, proposals}), not by making run_id
  nullable on MvProposalsResponse. The element type must stay the SAME
  MvProposal in both, so Prompt 13 and 13.5 render one card from one shape.
```

### Prompt 12.0 — Model tab mockups (review checkpoint)

*Inserted before Prompt 12 at review. Prompt 10 was the branch's mockup checkpoint,
but its frame 6 predates the Prompt 12 amendment: it reviewed one proposal-overlay
frame, while the amended Prompt 12 ships a whole new surface — a fourth SpaceDetail
tab, a layered-column layout, a governance-ladder color language, and empty states
none of which any reviewed mockup shows. A new tab and a new visual vocabulary
warrant the same cheap gate the other panels got; the scaffold and emitter already
exist, so this is hours, not days.*

```
Using the existing mockup scaffold (frontend/src/components/auto-optimize/
mockups/ + the emitter registry — same fixtures, same test idiom, both themes),
add the Model tab frames and STOP for review before Prompt 12 implements:

9a. Model tab, populated: layered columns (source/fact -> dims -> MVs ->
    field chips), join edges labeled with ON predicate + relationship type +
    SCD2 flag, governance-ladder coloring on measure concepts. CORRECTED at
    the 12.0 PLAN review — the original "reuse the Prompt 10 card tier
    tokens" was wrong: those are the CONFIDENCE trio (high/medium/low =
    danger/warning/info) and governance is a maturity axis, not a confidence
    axis. The ladder is a traffic light on the theme's semantic tokens,
    rhyming with the IQ scan's own passed/warning/failed split (the surface
    the ladder will live on): governed = success, curated = warning,
    ungoverned = danger. Ungoverned is the state the whole feature exists to
    fix — it must draw the eye, never render muted. Every chip also carries a
    NON-COLOR discriminator (icon or label text) so the ladder never relies
    on hue alone. Include the tab strip showing
    Score | Model | Optimize | History so placement is reviewed too.
9b. Model tab, never-optimized/empty: tables unconnected, and the
    config-scoped empty line, verbatim as approved at the 12.0 review —
    "This Agent's configuration defines no joins, SQL snippets, or metric
    views yet — the graph shows the config as it is now. Connect it by adding
    join specs and snippets yourself, or let an optimization run discover and
    apply them. Metric view suggestions don't require a run." The ladder shows
    nothing green (frame-7b honesty rule) and, cutting both ways, nothing red
    either — an empty space has surfaced no ungoverned measure to alarm about.
    This is the frame to review hardest; it is most users' first sight of the
    tab.
9c. Proposal overlay ON: ghosted proposed MV, dashed replaces edges, the
    default-off toggle visible.
9d. Node detail panel: one measure (expr, synonyms, format, evidence) and one
    join (cardinality, reachable join_strategy only).
Upgrade MvSemanticModelFrame (frame 6) into these rather than duplicating it —
frame 6 is 9c's ancestor. No backend wiring; fixture data only.
```

**Your job after Prompt 12.0:** review the four frames — especially 9b, and
whether the governance ladder reads at a glance without a legend — and paste
corrections before Prompt 12 runs, exactly as after Prompt 10.

### Prompt 12 — Semantic model visualization

*Amended after Prompt 11, before execution (a live claim, not a frozen transcript).
Two of the original body's choices were overturned at review, with reasons recorded
so they are not silently re-derived. **Scope:** the original body was proposal-scoped
— a graph of one MV proposal per run. That builds the component for one of its four
consumers and rebuilds it for the rest. The view is now SPACE-scoped with the
proposal as an overlay, the same build-once-two-callers shape as MV-D23: SpaceDetail
gets a lens on what exists — SHIPPED BY THIS PROMPT as a fourth "Model" tab, not
deferred to a later consumer — Prompt 13's output screen and 13.5's IQ Scan panel get
the same view with a proposal overlaid, and the create-agent branch inherits it
later. **Rendering:** the original body said react-force-graph-2d. Overturned for
this view on four grounds: (1) a semantic model is a layered DAG — fact → dims → MV
→ fields — the same left-to-right visual language as Catalog Explorer's ERD and
lineage graph, and force layouts are non-deterministic: two renders of one space
produce two pictures, which is poison under a diff overlay; (2) the repo's test
idiom is renderToStaticMarkup in a node environment — a canvas graph asserts
nothing, an SVG graph tests like every other component; (3) rich nodes (measure
chips, badges, governance colors) need DOM, which canvas cannot give; (4) mockup
frame 6 is already a positioned SVG — the real component is that plus data and
pan/zoom, not a different technology. ForceGraph2D stays where it already earns its
keep: the exploratory, unbounded Watch resource graph
(frontend/src/watch/pages/ResourceGraphView.tsx). At this scale (≤30 tables, one
MV, ≤20 fields) no layout library is needed either — a hand-rolled 3-4 column
layered layout is ~100 lines and deterministic, so the "no new graph dependency"
rule survives the change.*

```
Implement the SPACE-scoped semantic model view, with proposals as an overlay,
per the mockups approved at Prompt 12.0 — do not run this prompt before that
review has happened.
Rendering is a deterministic layered SVG component (see the amendment note
above — do NOT use react-force-graph-2d for this view, and do NOT add a graph
or layout dependency). Keep react-diff-viewer-continued for the space-config
diff mode and prism-react-renderer for any inline SQL.

Base graph (no proposal needed — this must render for a space that has never
been optimized):
- Columns, left to right: source/fact tables -> joined dimension tables ->
  metric views -> their dimension/measure chips. Tables the space uses that
  join nothing render in the first column.
- Join edges from instructions.join_specs, labeled with the ON predicate and
  relationship type, with an SCD2 flag when the predicate carries an
  is_current guard.
- Governance ladder coloring on every MEASURE CONCEPT the space knows about —
  the advisor's story made visible: governed (defined in an attached metric
  view) / curated (defined in sql_snippets.measures — structured name+expr
  fields, no parsing needed; example-SQL-derived curated concepts are Prompt
  12b's, because extracting a measure from SQL text is exactly the parsing
  this prompt defers) / ungoverned (recurs only in proposal evidence).
  Concept identity FOR THIS PROMPT is exact name match only — an MV measure,
  a snippet measure, and a proposal sharing a name are one concept at its
  highest rung; anything subtler (canonicalized-expr matching) is server-side
  12b work via the one sanctioned parser. State the name-match rule in a
  comment where it is implemented, so 12b replaces it knowingly. One glance answers "how
  governed is this space?". Derivation, stated precisely because the 12.0
  review got it wrong TWICE (first the mockup, then the reviewer): the ladder
  reads CONFIG — attached metric_views (governed), sql_snippets / example SQL
  (curated) — plus proposals (ungoverned). Config is populated three ways: the
  user's own edits, the create agent, AND optimization runs, whose patch
  allowlist includes add_join_spec / update_join_spec (unified_loop.py:91-92,
  applied at applier.py:3173) and snippet-measure patches. So a run is one
  legitimate way to populate this tab — never present it as the ONLY way (a
  never-optimized space with snippets already shows green/amber, and metric
  view suggestions need no run per Delta 9), and the tab must render whatever
  the config holds regardless of how it got there. Colors per the 12.0
  correction: a traffic light on
  the theme's semantic tokens (governed=success, curated=warning,
  ungoverned=danger), rhyming with the IQ scan's passed/warning/failed split —
  NOT the Prompt 10 confidence trio, which is a different axis. Non-color
  discriminator on every chip. Do not invent a new palette.

Proposal overlay (frame 6's content, on top of the base graph):
- The proposed MV as a ghosted node with distinct styling; dashed "replaces"
  edges from the raw space tables it would cover (the tables_freed story).
- Node detail panel on click: for a measure, the expr, synonyms, format, and
  the evidence (recurrence count, contributing benchmark question ids); for a
  join, cardinality if known. join_strategy chips show only reachable states
  (the mockup fixture type already enforces this — MV-D14/D15).
- Diff mode toggle: current space data sources vs post-attach state. The
  post-attach side is SYNTHESIZED client-side — current data_sources with the
  proposal's proposed_object appended to metric_views[] (and its replaced raw
  tables marked) — because no attach has happened and the run-keyed DDL
  artifact is not a dependency of this space-scoped tab. No new endpoint for
  this.

Data:
- Add GET /api/auto-optimize/spaces/{space_id}/semantic-graph returning
  nodes/edges JSON (schema in the OpenAPI spec), assembled from
  serialized_space (data_sources, join_specs, sql_snippets) plus the
  space-scoped proposals read Prompt 11 added. Space-scoped, not run-scoped
  (MV-D23: run_id presentational only). Read-only route: the SP-tolerant
  get_workspace_client is fine here per MV-D20 (only UC writes require the
  hard-fail OBO client), but MV-D1's presentation rule applies — do not
  invent per-user evidence filtering here; surface proposals exactly as the
  run-keyed route already does, and note any gap it has rather than fixing
  it silently. Any SQL parsing happens SERVER-side
  with the sqlglot machinery that already exists (mv_fingerprint's
  extractors) — never a second parser in the browser. For THIS prompt the
  endpoint does not parse SQL at all: config fields and proposal evidence
  suffice for the base graph and overlay; SQL-derived edges are Prompt 12b.
- The overlay consumes the same MvProposal shape as the cards. No new
  proposal payload.

Surface — this prompt SHIPS a user-facing view, not just a component for
later prompts to mount:
- Add a fourth SpaceDetail tab, "Model": extend the SpaceTab union and the
  isSpaceTab guard (frontend/src/lib/navigation.ts:2), the tabs array
  (frontend/src/pages/SpaceDetail.tsx:153), and the App routing that consumes
  them (frontend/src/App.tsx:122, :148), matching the existing three tabs'
  icon/label/onNavigate pattern. The tab renders the base graph for the CURRENT
  state of the Genie Agent: config fetched live on tab entry via the existing
  fetchSpace path (lib/api.ts:156 -> /space/fetch), never from a run artifact
  or cache — the tab must reflect what the space is NOW, including edits made
  outside the workbench since the last scan or run.
- When the space-scoped proposals read (Prompt 11) returns candidates, offer
  the proposal overlay on this tab too — ghosted, clearly labeled proposed,
  default off. The base graph never requires it.
- Empty states are first-class, same honesty rule as frame 7b: a space with
  no join_specs renders its tables unconnected with a line saying no joins
  are defined (not an error); no snippets/no MVs means no measure chips and
  the governance ladder simply has nothing green — never a blank panel, never
  a spinner that resolves to nothing.
- A refresh affordance on the tab (the config can change under it); loading
  and fetch-failure states per the repo's existing tab patterns.
- Overflow: the SVG lives in a scroll/zoom container — a 30-table space must
  not squash into an unreadable viewBox, and the page body must never scroll
  horizontally. Simple pan/zoom (wheel + drag, or fit/actual-size buttons) is
  in scope; do not add a library for it. Edge-label decluttering: midpoint ON
  labels collide on crossing edges (12.0 carry-forward) — show full labels on
  hover/selection and abbreviate at rest.

Tests: render-level with fixture spaces (never-optimized space, space with
joins + snippets, space with an attached MV, proposal overlay on each; single
MV, multi-join MV, conflict state; each empty state above), per the repo's
renderToStaticMarkup idiom
— there is no Storybook in this repo (Prompt 10 finding); add the frames to
the existing mockup emitter's registry instead so the HTML export covers them.

Deliberately OUT of this prompt: column-level ERD (Catalog Explorer already
draws it — deep-link table nodes there instead, the frame-5 idiom), and any
query-history heat (no corpus producer exists; same honesty rule as frame 7b's
copy). Both stay out until a decision entry says otherwise.
```

### Prompt 12b — Coverage lenses on the semantic model (after Prompt 13.5)

*Stub, sequenced after 13.5 because both lenses read curated SQL through the
space-scoped machinery 13.5 builds. Do not run before it.*

```
Extend the Prompt 12 semantic-graph endpoint and view with two lenses:
- SQL coverage: which example SQLs and benchmark questions touch which tables
  and measure concepts, computed server-side by reusing corpus_scan /
  shapes_in_statement from mv_fingerprint (sqlglot==30.0.3) — one parser, the
  one the advisor already trusts. Renders as edge weight / a per-node coverage
  badge.
- Benchmark evidence overlay on proposals: evidence.benchmark_questions ids as
  weighted edges from questions to the proposed measure.
Same component, same endpoint, additive response fields only — a Prompt 12
client that never learns about the lenses must keep working unchanged.
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
- Per MV-D23: run_id is presentational in every component added here. Do not
  key component state, cache identity, or a fetch on it — 13.5 renders these
  same cards from a space-scoped source, and a structural dependency on run_id
  means rebuilding them.
```

### Prompt 13.5 — Suggest metric views without a run (decides MV-D23)

*Sequenced after Prompt 13 and BEFORE Prompt 14, deliberately. The three prompts that follow are closeout — 14 audits "everything added on this branch", 15 is the E2E pass, 16 writes the docs, changelog and PR — so landing this after 16 pays for all three twice and ships a changelog describing an entry point that changes one prompt later. It is placed after 13 rather than before 10 because its UI is Prompt 13's proposal card with a different data source: build the card once, then feed it.*

*Why this is a prompt at all, rather than a follow-on: the feature as built through Prompt 13 can only advise a Genie Agent that has already completed an optimization run, because the advisor's corpus gate requires iteration-0 generated SQL. That inverts the value proposition — the spaces most in need of a governed measure are the ones nobody has optimized yet — and it is the reason MV-D23 was raised the moment Prompt 9 landed.*

```
DECIDE MV-D23 AND MV-D24 FIRST and record both decisions in the playbook
register before writing code. The three options and the recorded leaning are in that entry; the
four run_id couplings it enumerates are the work. Do not start from the leaning
without re-reading the couplings against the repo — MV-D23 was written from a
review, not from an implementation attempt, and the anchors may have moved.

Scope, in dependency order:

1. GSO — make the corpus reachable without a run (coupling 1).
   - mv_advisor._advise currently returns SKIPPED on `not load.usable` before
     the curated harvest runs. Restructure so the iteration-0 corpus is one
     contributor rather than a gate: a usable corpus is a non-empty union, and
     "no iterations" is a recorded contribution reason, not a phase skip.
   - curated_corpus_entries consumes load.applied_config. Outside a run the
     config comes from get_space(include_serialized_space=True) — which is
     where the iteration row's copy originated anyway. Route both callers
     through one accessor so the two paths cannot disagree about what the
     applied config is.
   - Preserve every existing skip reason and its meaning. The empty-corpus trap
     documented in the module docstring is unchanged: an eval run that produced
     no SQL is still SKIP with a reason, and must not become "no recurring
     measures". MV-D15 vocabulary throughout — EMPTY is a measurement,
     UNAVAILABLE is a read that could not run.
   - Do NOT weaken the MV-D16(b) contamination rule: nothing generated by this
     standalone path may re-enter the corpus as recurrence evidence.

2. GSO — a corpus-agnostic entry point callable without a SparkSession.
   Extract the orchestration _advise performs into a function whose inputs are
   the corpus, the estate, profiling, capabilities and the signal readers, and
   whose persistence is injected. run_mv_advisor_phase keeps its signature and
   becomes one caller (Spark, in-job); the backend becomes the other (warehouse
   path). The 6a/6b producers already read through sql_warehouse_query, so no
   second producer is needed. Persistence for the backend caller goes through
   the wh_* helpers Prompt 9 built (MV-D21) — extend that set if a writer is
   missing, and extend the MV-D21 column-contract pin along with it. Do NOT
   import genie_space_optimizer.optimization into the backend.

3. Backend — the route.
   POST /api/auto-optimize/spaces/{space_id}/mv/suggest, OBO, returning the
   same ScoredProposal-shaped payload the run-keyed route returns. Reads may
   use the SP-tolerant client; anything that writes a UC object stays on
   require_obo_workspace_client per MV-D20. The create path is the Prompt 9
   mv_create service — reuse it, do not fork it. Whatever MV-D23 decides about
   couplings 3 and 4 lands here: the create needs a body to replay (MV-D22) and
   a ledger row to write, and neither has a run.

3b. Backend — bring-your-own registration (MV-D24).
   The route the copied-DDL path needs to stop being a one-way exit: the user
   reports an identifier; the backend verifies it under OBO (DESCRIBE EXTENDED
   asserts Type: METRIC_VIEW; view_text via DESCRIBE ... AS JSON;
   mv_yaml.validate; fingerprint compared when the user claims it implements a
   specific proposal); on success, a ledger row with provenance=USER_CREATED
   (additive column via ADDITIVE_COLUMN_MIGRATIONS — extend the MV-D21 column
   pin in the same change). Relax the mv_attach.py:484 identity guard for
   USER_CREATED rows ONLY, narrowly: a verified registration is the consent
   coverage that guard exists to require. The two MV-D24 invariants are
   non-negotiable: drop refuses USER_CREATED on provenance (not merely on
   status), and an unverifiable identifier is refused with the reason, never
   recorded provisionally. Reuse mv_entitlement.probe + _remediation_sql for
   the self-create permission guidance; do not build a second probe.

4. Frontend — the IQ Scan panel from Prompt 10 mockup 7, reusing Prompt 13's
   proposal card. Both states, EMPTY included, plus the denial variant. Plus
   the frame-8 registration affordance (MV-D24): [I created this myself] on
   the suggest-only card and the free-standing identifier input, with the
   verified and refused states from the mockup review.

VERIFY:
- A test proving a space with NO iterations and a non-empty curated corpus
  produces candidates. This is the whole point of the prompt; if it does not
  exist, nothing else here matters.
- A test proving an eval run that produced no SQL still SKIPs with its existing
  reason, so (1) did not collapse two outcomes into one.
- A test proving a genuinely empty corpus reports EMPTY, not a failure.
- The MV-D21 pin still holds after any wh_* addition.
- The in-job path is byte-unchanged in behaviour: run the existing
  test_mv_advisor.py suite unmodified, and do not relax an assertion in it.
- Route tests per the repo's OBO fixture pattern, including a create refused
  when no OBO token is present (MV-D20).
- MV-D24: registering a real metric view succeeds and writes USER_CREATED;
  a regular VIEW at the identifier is refused with the reason; an identifier
  the caller cannot see is refused; drop on a USER_CREATED row is refused on
  provenance even when status is DETACHED; the attach phase accepts a
  verified USER_CREATED row and still skips an unregistered identifier.
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
- Standalone path (Prompt 13.5, MV-D23): the no-iterations-plus-curated-corpus
  case; the run-produced-no-SQL case still SKIPping with its own reason; EMPTY
  vs UNAVAILABLE reported distinctly; the space-scoped proposals route; and
  that the in-job advisor behaviour is unchanged by the corpus restructure.
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

Scenario D — suggest with no run at all (Prompt 13.5, MV-D23):
  Pick a Genie Agent in the dev workspace that has NEVER been optimized and has
  curated SQL (example_question_sqls or sql_snippets.measures). Call the
  space-scoped suggest route. Assert: candidates are produced with no run
  present; every candidate's evidence cites curated provenance; the IQ Scan
  panel renders them. Then repeat against a space with no curated SQL and no
  query history and assert the response is EMPTY with a reason — not an error,
  not a 500, and not silence. That second half is the case a customer demo hits
  first, so it is not optional.
  BYO leg (MV-D24): create a metric view manually in the scratch schema from
  the copied DDL, register it through the route, assert the ledger row says
  USER_CREATED with the registering user, and that the next run attaches and
  measures it. Then assert the drop route refuses it. Teardown drops it
  manually — the app must never have.

Also include a manual smoke checklist for the UI (10 items max) covering the
consent panel, denial banner, output panels, the semantic model graph, and the
IQ Scan panel in both its populated and empty states.
```

### Prompt 16 — Docs, changelog, PR

```
Finish the branch:

- Update the repo docs (per its docs structure) with: feature overview, the
  consent model, mode table, how to enable, and screenshots exported from the
  Storybook/mockup states. Document BOTH entry points (MV-D23): the IQ Scan
  surface for a space that has not been optimized, and the optimization-run
  surface for one that has — and say plainly which evidence each rests on, so a
  reader does not expect history-grade confidence from a space with no history.
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
