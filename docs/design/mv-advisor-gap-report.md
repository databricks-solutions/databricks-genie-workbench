# Metric View Advisor — Repo Gap Report

**Purpose.** `docs/design/metric-view-suggestion-engine-pov.md` (the POV) describes an
architecture. This document describes the repository *as it exists today*, quotes the
code verbatim, and states for every Part 7 assumption in the POV whether it MATCHES,
CONFLICTS, or DOES-NOT-EXIST-YET.

Where the POV and this report disagree, **this report is the factual record and the POV
is the aspiration**. Implementation work must reconcile the two before any code is
written — see [§3 Decisions needed](#3-decisions-needed).

| | |
|---|---|
| Read against | `feature/metric-view-advisor` @ `a73bb0b8` (branched from `main` @ `ae8c4367`, "GSO optimizer-v2: engine rewrite + Genie Agent rename (#315)") |
| Survey / reconciliation | Original repo survey: 2026-08-23. Current-position reconciliation: **2026-08-27**. |
| Pre-17.0 drift check | **Clean as of 2026-08-27** (branch @ `a73bb0b8`). Against the current local remote-tracking ref, `git rev-list --count HEAD..origin/main` = **0**, and both `git merge-base HEAD origin/main` and `git rev-parse origin/main` resolve to `ae8c4367`. No upstream commit has moved under this branch since the original survey, so Prompt 17.0 does not need a Prompt 0 diff-mode re-run on the present refs. Re-run this check if `origin/main` advances before 17.0 starts. |
| Method | Original evidence is preserved at its measured commit. Current status below was reconciled from live files plus branch history through `a73bb0b8`; current anchors are called out explicitly rather than silently rewriting historical observations. |
| Scope | Documentation-only reconciliation. No feature code was written or modified. |
| Current reconciliation | Advisor implementation and hardening are complete in code through the create-and-attach / Genie-v2 round-trip work and the current Semantic Blueprint. Earlier live E2E and deployed human-review rounds remain verified historical evidence, including round 10's controlled round-trip; they do **not** establish deployment review of current HEAD. The round-10 follow-up ends `Deploying…`, and the later Semantic Blueprint revisions have code/local-test evidence but no recorded deployment confirmation. **Next = Prompt 17.0.** Prompt 17.0's `Ontology surfaces` inventory is intentionally not present yet. |
| Last MV-D9 refresh | 2026-08-25, in the Prompt 15.8 commit (create-at-approval + facts-lead display: MV-D34/MV-D35). Refreshed here: four third-look findings appended below — the dead acceptance CUJ (RESOLVED, MV-D34), the "% confidence" category error (RESOLVED, MV-D35), the four presentation defects (RESOLVED), and finding 1's semantic-canvas fidelity gap (OPEN, tracked to Prompt 12f in the same redeploy); the baseline-count line in both rules copies moved to the measured floor 673 backend + 1512 GSO. No fenced code quotes moved (`checks`/`audience_grantees` are computed/derived, not persisted columns, so the exposure matrix and the generated package-layout block are unchanged). Previously, 2026-08-25, in the Prompt 15.5 commit (Tier-2 read-back integrity: the bundle body and the downgrade reason). Refreshed here: the two 2026-08-25 Tier-2 findings (Scenario A in-job `mv-ddl` 404; Scenario B downgraded-run `downgrade_reason` NULL) flipped OPEN → RESOLVED with root-cause resolutions appended; the generated package-layout block was rewritten by `scripts/gap_report_counts.py --write` (`mv_yaml.py` grew by the `render_components` render path); no fenced code quotes moved. Previously, 2026-08-24, in the Prompt 12b commit (semantic-graph coverage lens + deferred parsing: the three debts Prompt 12 signed as swap-point comments are paid). Refreshed here: the semantic-graph API row (§ API surface) re-anchored `auto_optimize.py:1869 → :2236` (`get_space_semantic_graph`, shifted by the assembler rewrite and the new `_build_semantic_graph` at `:1990`) and `models.py:323 → :377` (`MvSemanticGraph{,Node,Edge}`, additive lens fields `coverage`/`weight`/`coverage_status`/`coverage_reason`), status moved **LANDED → LANDED+EXTENDED**; the deleted `_is_measure_column` speculative probe and the exact-name concept identity it described are gone from the code and from that row's prose (governed chips now read `DESCRIBE ... AS JSON` via `metric_view_catalog`, concept identity is `canonicalize_expr`). No fenced code quotes moved. Previously, 2026-08-23, in the Prompt 7 review commit (MV-D18 champion-record fix, end-of-run reconciliation, trusted-asset conflict surface). Refreshed there: **two fenced quotes this commit moved** (`unified_loop.py` `80-94` → `83-97` and `2862-2867` → `2894-2899`, both shifted by the reconciliation import and helper) and **six line anchors** — `unified_loop.py:3287-3292` → `:3373-3378` (candidate eval), `:2854-2859` → `:2894-2899` (Appendix baseline row), `:133-146` → `:137-150` (`_TRANSIENT_EVAL_STATUSES`, which had drifted onto the leak-drop frozenset), `state.py:923-966` → `:889` + `:983-1004` (`write_iteration` and its `eval_run_status` columns), `state.py:1101-1108` → `:1168-1175` (`mark_patches_rolled_back`), and `mv_scoring.py:150` → `:158` (`LineageOverlap`, shifted by the trusted-asset docstring). **The two fences that were already stale at HEAD are now clean**: `ddl.py:182` byte-matches (the `artifact_kind` enum gained `mv_candidate_ddl` in Prompt 6) and `test_phase7_job_dag.py:397-405` matches. The generated package-layout block was rewritten by `scripts/gap_report_counts.py --write` (`unified_loop.py` 3674 → 3727 L, `state.py` 1804 → 1871 L, `mv_scoring.py` 1327 → 1498 L). All 48 fenced quotes byte-match live source as of this row; the third staleness class this exposed is recorded in MV-D9. Previously, in the Prompt 7 commit (attach patch type + lift phase). Refreshed there: **six fenced quotes whose line numbers this commit moved** (`unified_loop.py` `79-93` → `80-94` and `2854-2859` → `2862-2867`; `applier.py` `3914-3917` → `3974-3977` and `4550-4560` → `4617-4627`; `ddl.py` `280-290` → `281-291` and `292-297` → `293-298` — the `_ALL_DDL` and migrations fences shifted by the `lift_report_json` column, exactly as MV-D7 anticipated); the `PATCH_TYPES` count (50 → 51) and the now-accurate section comment; the patch-path anchor table (`_apply_action_to_config` `:3409` → `:3442`, `_apply_action_to_uc` `:3942` → `:4002`, `apply_patch_set` `:4025` → `:4091`); the "critical gap — nothing attaches a metric view" paragraph, which this commit closed; the §2.2 parameter preamble (three widget reads landed ahead of Prompt 8's mirrors); five §2.5/§2.6 rows moved off DOES-NOT-EXIST-YET (attach, rollback inheritance, `DETACH_ONLY_NEVER_DROP`, `mv_baseline`, raw-table companion); §3 items 8, 9 and 10 resolved, item 8's `attach_metric_view`/`detach_metric_view` proposal replaced by the shipped `mv_attach_data_source` so two names are not left live; and the `update_mv_yaml` follow-up ([#331](https://github.com/databricks-solutions/databricks-genie-workbench/issues/331)) closed. **Two quotes were already stale at HEAD and are re-quoted in passing** (`test_phase7_job_dag.py:397-406` → `397-405`, unchanged content one line shorter; `ddl.py:182` `artifact_kind`, whose comment gained `mv_candidate_ddl` in Prompt 6 without the fence being refreshed) — neither file was touched by this commit. All 48 fenced quotes byte-match live source as of this row. Previously, in the Prompt 5.5 remediation commit. Line counts are no longer hand-maintained: the package-layout block below is generated between markers by `scripts/gap_report_counts.py`, and `test_gap_report_counts.py` fails on a stale block or a stale `(N L)` claim anywhere else in this file. Refreshed here: the generated block (`config.py` 2632 → 2669 L, `mv_yaml.py` 1643 → 1755 L), the metric-view surface row for `mv_yaml.py`, the §2.5 sole-renderer paragraph (a second guard now covers the DDL wrapper), and a new open-follow-up note for `update_mv_yaml` ([#331](https://github.com/databricks-solutions/databricks-genie-workbench/issues/331)). Byte-matched and unchanged: all 48 fenced quotes. Previously, in the Prompt 5.5 commit `e104a779`: Refreshed for that commit: `config.py` (2545 → 2632 L), the new `mv_yaml.py` row in §1.4's layout and metric-view-surface tables, and the two `WITH METRICS` claims in §2.5 and the patch-path table — that row moves from DOES-NOT-EXIST-YET to PARTIALLY MATCHES now that the statement is rendered but still not executed. Two unrelated counts in §6's test table were found stale by one line each and corrected in passing (`test_phase7_job_dag.py` 406 → 405, `test_four_notebook_architecture.py` 86 → 85); neither file changed in this commit. All 48 fenced code quotes in this report were byte-matched against live source, and every `(N L)` claim re-counted — both clean as of this row. Refreshed just before it, at `7b55df61`: the `config.py` and `mv_scoring.py` counts, the same two `WITH METRICS` claims, the `is_benchmark_leak` anchor in §2.7 (`:414-420` → `:421-427`), and one blank-line anchor in the Appendix leakage list (`:764` → `:756`). Byte-matched and unchanged: every other §1.4 count and both `leakage.py` fences (`286-296`, `291-296`). Still imprecise, intent unverified rather than wrong: `leakage.py:454`, `:927`, `:971` and `applier.py:4045`, `:4063` land on guard, comment, or docstring lines near their subject rather than on a definition. |

The `Last MV-D9 refresh` cell is retained verbatim as the historical review
ledger through Prompt 15.8; it is not the current-position marker. The
`Current reconciliation` row and the section below supersede its now-closed
follow-up wording without erasing the evidence trail.

## Headline

The POV was written against a GSO job that **no longer exists**. `main` @ `ae8c4367`
replaced the multi-stage wheel DAG with a **linear four-task notebook DAG**, and the
replacement is *enforced by passing unit tests* that specifically assert the POV's task
names are gone and that condition tasks and task values are forbidden.

This is not a naming mismatch that can be papered over. Three structural mechanisms the
POV depends on — `condition_task` gates, `{{tasks.X.values.Y}}` handoff, and
`python_wheel_task` entry points — are each individually prohibited by a test that
passes today.

### Current position (2026-08-27)

The original recon and live-review ledger below remain the historical evidence trail.
The current branch has completed the metric-view advisor implementation track in code
through HEAD `a73bb0b8`:

- Approval now creates **and attaches** the selected metric view in one OBO request;
  the route states that the live Agent config is the source of truth
  (`backend/routers/auto_optimize.py:2556-2576`), and
  `backend/services/mv_create.py:829-868,982-1023` records the attach result honestly.
- The 2026-08-26 controlled round-trip proved this Genie v2 deployment persists metric
  views under `data_sources.tables`. The attach helper therefore writes there and
  remains idempotent across both buckets
  (`backend/services/mv_create.py:752-818`). Workbench reads normalize UC-confirmed
  metric views back into the documented `metric_views` shape
  (`backend/services/genie_client.py:57-122,258-282`).
- Proposal reads mark an object attached from the union of raw `_tables` and
  `_metric_views` (`backend/routers/auto_optimize.py:2038-2071`); the IQ surface removes
  those rows from the active proposal cards/count and reports them separately
  (`frontend/src/components/auto-optimize/MvIqScanAdvisorySection.tsx:374-385,572-578`).
- The Model tab now renders the live, normalized config through the single
  `SemanticBlueprint` canvas, with governed/curated/proposed measures, metric-view
  definitions, per-measure lineage, and the Join Advisor's advice-only path
  (`backend/routers/auto_optimize.py:2890-3305,3370-3440`;
  `frontend/src/components/model/SemanticModelTab.tsx:3-24,496-569`).

Deployment-review status is narrower than branch status. Earlier live E2E and
deployed human-review rounds are verified, including round 10's controlled
round-trip. The round-10 follow-up is recorded only as `Deploying…`; the subsequent
Semantic Blueprint commits have code and local-test evidence but no documented
deployment confirmation. This report therefore makes no claim that current HEAD or
the current Blueprint revision was deployed-reviewed.

The next work item is **Prompt 17.0**. MV-D26, MV-D27, and MV-D28 remain OPEN and owned
by Prompt 17; MV-D25 remains OPEN and deferred to Prompt 18. No Prompt 17.0 deliverable
is part of this reconciliation.

---

## 1. Repo map

### 1.1 The GSO job definition

The job is defined in **three places that must stay in lockstep** (see
[§1.2](#12-the-four-place-parameter-lockstep-requirement)). All three describe the same
DAG.

#### Root bundle — `databricks.yml`

Resource key `gso-optimization-runner`, job name `gso-optimization-job`:

```49:59:databricks.yml
    gso-optimization-runner:
      name: "gso-optimization-job"
      description: >-
        GSO v2 bounded hill-climbing runner managed by Genie Workbench
        (intake_and_snapshot -> benchmark_qc_and_repair ->
        optimize -> publish_and_audit).
        Linear 4-task serverless DAG; the baseline eval and hill-climb run as an
        in-process native Benchmark API loop inside optimize. No condition tasks, no dbutils.notebook.run,
        no inter-task task-value plumbing — every task reads job parameters +
        durable Delta state by run_id (D9). SP executes with granted privileges
        on user schemas.
```

**The actual DAG — four tasks, strictly linear, all `notebook_task`:**

| # | `task_key` | Task type | Entrypoint | `depends_on` |
|---|---|---|---|---|
| 1 | `intake_and_snapshot` | `notebook_task` | `./packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_intake_and_snapshot.py` | — |
| 2 | `benchmark_qc_and_repair` | `notebook_task` | `.../jobs/run_benchmark_qc_and_repair.py` | `intake_and_snapshot` |
| 3 | `optimize` | `notebook_task` | `.../jobs/run_optimize.py` | `benchmark_qc_and_repair` |
| 4 | `publish_and_audit` | `notebook_task` | `.../jobs/run_publish_and_audit.py` | `optimize` |

Every task carries `environment_key: default`, `timeout_seconds: 14400`,
`max_retries: 0`. Task 1 verbatim:

```116:137:databricks.yml
        - task_key: intake_and_snapshot
          notebook_task:
            notebook_path: ./packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_intake_and_snapshot.py
            base_parameters:
              run_id: "{{job.parameters.run_id}}"
              space_id: "{{job.parameters.space_id}}"
              domain: "{{job.parameters.domain}}"
              catalog: "{{job.parameters.catalog}}"
              schema: "{{job.parameters.schema}}"
              apply_mode: "{{job.parameters.apply_mode}}"
              levers: "{{job.parameters.levers}}"
              max_attempts: "{{job.parameters.max_attempts}}"
              target_accuracy: "{{job.parameters.target_accuracy}}"
              benchmark_repair_max_tries: "{{job.parameters.benchmark_repair_max_tries}}"
              benchmark_policy: "{{job.parameters.benchmark_policy}}"
              triggered_by: "{{job.parameters.triggered_by}}"
              warehouse_id: "{{job.parameters.warehouse_id}}"
              workload_warehouse_ids: "{{job.parameters.workload_warehouse_ids}}"
              llm_model: "{{job.parameters.llm_model}}"
          environment_key: default
          timeout_seconds: 14400
          max_retries: 0
```

Note the handoff mechanism: **`base_parameters` referencing `{{job.parameters.*}}`**, not
`{{tasks.*.values.*}}`. The bundle states the rule explicitly:

```110:114:databricks.yml
      # Every task carries its job-parameter subset as base_parameters so there
      # is NO inter-task task-value plumbing (D9). run_id/catalog/schema
      # bootstrap each notebook; all other handoff state is read from Delta by
      # run_id. This mirrors the package bundle; `llm_model` is added to every
      # task (Workbench-specific — see the parameter note above).
```

**The 15 declared job parameters** (`databricks.yml:74-109`):

| Parameter | Default |
|---|---|
| `run_id` | `""` |
| `space_id` | `""` |
| `domain` | `"default"` |
| `catalog` | `""` |
| `schema` | `""` |
| `apply_mode` | `"genie_config"` |
| `levers` | `"[1,2,3,4,5,6]"` |
| `max_attempts` | `"3"` |
| `target_accuracy` | `"0.90"` |
| `benchmark_repair_max_tries` | `"3"` |
| `benchmark_policy` | `"repair_allowed"` |
| `triggered_by` | `""` |
| `warehouse_id` | `""` |
| `workload_warehouse_ids` | `"[]"` |
| `llm_model` | `${var.llm_model}` |

#### Package bundle — `packages/genie-space-optimizer/databricks.yml`

Resource key `genie-space-optimizer-runner`, job name `genie-space-optimizer-job`. Same
four task keys, same order, same notebook stems, same parameter set **minus `llm_model`**
(the root bundle adds it as a Workbench-specific extra). This file is the shape that
`test_phase7_job_dag.py` validates.

#### Notebook installer — `scripts/deploy_lib/gso_job.py`

A third, hand-rolled mirror used by the `notebooks/install.py` path, built through the
Jobs REST API rather than DABs:

```23:30:scripts/deploy_lib/gso_job.py
# GSO v2 linear 4-task DAG. This MIRRORS the package bundle job at
# packages/genie-space-optimizer/databricks.yml (the validated source of truth,
# guarded by tests/unit/test_phase7_job_dag.py): same task keys, order,
# entrypoints, linear dependencies, and per-task base_parameters. There is NO
# condition task and NO `deploy` task (deploy is out of scope — D7). Each entry
# is (task_key, notebook_stem, depends_on, base_param_keys). `base_param_keys`
# lists the job parameters passed to that task as base_parameters (every task
# reads job params + durable Delta state by run_id — no task-value plumbing, D9).
```

```87:103:scripts/deploy_lib/gso_job.py
JOB_PARAMETERS = {
    "run_id": "",
    "space_id": "",
    "domain": "default",
    "catalog": "",
    "schema": "",
    "apply_mode": "genie_config",
    "levers": "[1,2,3,4,5,6]",
    "max_attempts": "3",
    "target_accuracy": "0.90",
    "benchmark_repair_max_tries": "3",
    "benchmark_policy": "repair_allowed",
    "triggered_by": "",
    "warehouse_id": "",
    "workload_warehouse_ids": "[]",
    "llm_model": "",
}
```

#### Verdict on the POV's claimed sequence

The POV's `preflight → baseline → enrichment → lever_loop → finalize → deploy` is
**CORRECTED to** `intake_and_snapshot → benchmark_qc_and_repair → optimize →
publish_and_audit`.

The old names are not merely absent — a passing test asserts they must stay absent:

```54:76:packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py
def test_deploy_and_legacy_tasks_removed():
    job = _load_job()
    keys = {t["task_key"] for t in job["tasks"]}
    for retired in (
        "deploy",
        "preflight",
        "baseline_eval",
        "02_baseline_eval_and_triage",
        "enrichment",
        "lever_loop",
        "finalize",
        "03_optimize",
    ):
        assert retired not in keys, f"retired task {retired} must be gone"


def test_no_condition_tasks():
    job = _load_job()
    for t in job["tasks"]:
        assert "condition_task" not in t, (
            f"task {t['task_key']} must not be a condition task (D9 linear DAG)"
        )
        assert "notebook_task" in t, f"task {t['task_key']} must be a notebook_task"
```

Where the POV's stages went:

| POV stage | Where the work lives now |
|---|---|
| `preflight` | Split: snapshot + manifest → `intake_and_snapshot`; benchmark QC → `benchmark_qc_and_repair`. The module `optimization/preflight.py` still exists and is imported by both. |
| `baseline` (eval task) | **In-process**, iteration 0 inside `optimize`. Not a task. See [§1.4](#14-genie-benchmark-eval-api). |
| `enrichment` | Absorbed into `optimize` (lever 0, "Proactive Enrichment", always runs, not user-selectable — `integration/levers.py:22-24`). |
| `lever_loop` | `run_unified_optimization_loop` in `optimization/unified_loop.py`, called in-process from `run_optimize.py:337`. |
| `finalize` | `publish_and_audit` |
| `deploy` | **Removed — out of scope (D7).** No task, no `deploy_target` parameter. |

---

### 1.2 The four-place parameter lockstep requirement

**Adding a single job parameter requires coordinated edits in four files.** Miss any one
and the failure mode differs — sometimes silent, sometimes a hard runtime rejection.

| # | File | What it declares | Failure if missed |
|---|---|---|---|
| 1 | `databricks.yml` (root, `resources.jobs.gso-optimization-runner`) | Parameter + default, **and** the per-task `base_parameters` entry | Local-terminal deploys (`./scripts/deploy.sh`) produce a job that rejects the parameter at `run_now` |
| 2 | `packages/genie-space-optimizer/databricks.yml` | Same, minus `llm_model` | `test_phase7_job_dag.py` drifts from reality; the package bundle is the shape the tests validate |
| 3 | `scripts/deploy_lib/gso_job.py` — `JOB_PARAMETERS` **and** the per-task `base_param_keys` in `TASKS` | Same | Notebook installs (`notebooks/install.py`) produce a job that rejects the parameter |
| 4 | `packages/.../backend/job_launcher.py` — `submit_optimization` | The `job_parameters={...}` dict sent to `run_now` | The value never reaches the job; the job default silently wins |

Plus a fifth, non-optional consumer: **each notebook must declare the widget**, or the
parameter arrives and is ignored.

The constraint is documented in the launcher itself:

```98:105:packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py
            # Only send parameters the 4-task job declares. Every key here must
            # be a job parameter on the runner (run_now rejects undeclared
            # keys), so this set MUST stay a subset of the declared params in
            # BOTH job definitions: the root bundle
            # (databricks.yml resources.jobs.gso-optimization-runner) and the
            # notebook installer (scripts/deploy_lib/gso_job.py) — which in turn
            # mirror the package bundle. `benchmark_repair_max_tries` is declared
            # by the job but not overridden here, so it uses the job default.
```

**`run_now` rejects undeclared keys.** That is the hard edge: place 4 must be a *subset*
of places 1–3. The existing set is 14 keys — one fewer than the 15 declared, because
`benchmark_repair_max_tries` is deliberately left to its job default:

```106:125:packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py
            job_parameters={
                "run_id": run_id,
                "space_id": space_id,
                "domain": domain,
                "catalog": catalog,
                "schema": schema,
                "apply_mode": apply_mode,
                "levers": levers,
                # GSO v2 loop knobs. The job declares these in
                # databricks.yml; passing them here lets a user-chosen value
                # override the job default. target_accuracy is the 0–1 stop
                # target; max_attempts bounds the patch/eval loop.
                "target_accuracy": target_accuracy,
                "max_attempts": max_attempts,
                "triggered_by": triggered_by,
                "warehouse_id": warehouse_id,
                "llm_model": llm_model or os.getenv("LLM_MODEL", ""),
                "workload_warehouse_ids": workload_warehouse_ids,
                "benchmark_policy": benchmark_policy,
            },
```

**Widget declaration (the fifth place).** Notebooks read job parameters as widgets, with
their own inline defaults:

```81:95:packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_intake_and_snapshot.py
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("levers", "[1,2,3,4,5,6]")
dbutils.widgets.text("max_attempts", "3")
dbutils.widgets.text("target_accuracy", "0.90")
dbutils.widgets.text("benchmark_repair_max_tries", "3")
dbutils.widgets.text("benchmark_policy", "repair_allowed")
dbutils.widgets.text("triggered_by", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("llm_model", "")
dbutils.widgets.text("workload_warehouse_ids", "[]")
```

Note the **three-layer default duplication** for every parameter: bundle default, installer
default, and widget default. `benchmark_policy` additionally validates at the notebook:

```107:109:packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_intake_and_snapshot.py
benchmark_policy = dbutils.widgets.get("benchmark_policy").strip() or "repair_allowed"
if benchmark_policy not in {"review_only", "repair_allowed"}:
    raise RuntimeError(f"Unsupported benchmark_policy: {benchmark_policy}")
```

**Full end-to-end chain for one new parameter.** The POV proposes seven
(`enable_metric_view_suggestions`, `mv_action_mode`, `mv_target_catalog`,
`mv_target_schema`, `mv_materialize`, `mv_consent`, `mv_min_confidence`). Each one
requires:

1. `frontend/src/types/index.ts` — add to `GSOTriggerRequest`
2. `frontend/src/components/auto-optimize/optimizationRequest.ts` — add to
   `buildOptimizationTriggerRequest`
3. `frontend/src/components/auto-optimize/OptimizationConfig.tsx` — add the control
4. `backend/routers/auto_optimize.py` — add to `TriggerRequest`, pass through `trigger()`
5. `packages/.../integration/trigger.py` — stringify, thread into `submit_optimization`
6. `packages/.../backend/job_launcher.py` — add to `job_parameters`
7. `databricks.yml` — declare + add to each consuming task's `base_parameters`
8. `packages/genie-space-optimizer/databricks.yml` — same
9. `scripts/deploy_lib/gso_job.py` — `JOB_PARAMETERS` + `TASKS[n].base_param_keys`
10. The consuming notebook(s) — `dbutils.widgets.text(...)` + read + validate
11. `test_phase7_job_dag.py` — extend `test_new_job_parameters_present_with_defaults`
12. `backend/tests/test_deploy_lib.py` — the installer mirror assertions

Existing tests already pin the launcher end of this contract:

```348:376:packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py
def test_submit_optimization_threads_loop_knobs_into_job_parameters():
    """GSO v2 Phase 10 (item 4): an app-chosen target_accuracy / max_attempts
    rides into the Jobs run_now job_parameters so a user override beats the
    databricks.yml default."""
    from unittest.mock import MagicMock

    from genie_space_optimizer.backend.job_launcher import submit_optimization

    ws = MagicMock()
    ws.jobs.run_now.return_value = MagicMock(run_id=4242)
    job_run_id, resolved_job_id = submit_optimization(
        ws,
        job_id=99,
        run_id="r1",
        space_id="s1",
        domain="d",
        catalog="c",
        schema="sch",
        target_accuracy="0.85",
        max_attempts="5",
        benchmark_policy="review_only",
        workload_warehouse_ids='["wh-a","wh-b"]',
    )
    assert resolved_job_id == 99
    params = ws.jobs.run_now.call_args.kwargs["job_parameters"]
    assert params["target_accuracy"] == "0.85"
    assert params["max_attempts"] == "5"
    assert params["benchmark_policy"] == "review_only"
    assert params["workload_warehouse_ids"] == '["wh-a","wh-b"]'
```

> **Recommendation.** Seven parameters × twelve places is 84 coordinated edits with no
> automated drift detection across all four job-definition sites. Strongly prefer a
> **single JSON-encoded `mv_config` parameter** (one string, parsed in the notebook and
> validated by a Pydantic model) over seven scalars. This also sidesteps the fact that
> `mv_consent` is already specified as JSON. If seven scalars are kept, add a test that
> loads all three job definitions and asserts their parameter sets are identical modulo
> `llm_model`.

---

### 1.3 The optimizer Python package

| Property | Value | Evidence |
|---|---|---|
| Distribution name | `genie-space-optimizer` | `packages/genie-space-optimizer/pyproject.toml:2` |
| Import name | `genie_space_optimizer` | `src/genie_space_optimizer/` |
| Version | Dynamic, from git via `uv-dynamic-versioning`; built wheel is renamed to the fixed `genie_space_optimizer-0.0.0-py3-none-any.whl` | `pyproject.toml` `[tool.hatch.version]`; `databricks.yml:38` |
| Python | `>=3.10` (root app requires `>=3.11`) | `pyproject.toml:5` |
| Test config | `pythonpath=["src"]`, `testpaths=["tests"]` | `pyproject.toml` `[tool.pytest.ini_options]` |

**Relevant existing dependencies** — `sqlglot` is already pinned and in use, so the POV's
AST-fingerprinting layer needs no new dependency:

```6:15:packages/genie-space-optimizer/pyproject.toml
dependencies = [
    "databricks-sdk==0.117.0",
    "mlflow[databricks]==3.11.1",
    "openai==2.30.0",
    "litellm==1.82.6",
    "pyyaml==6.0.3",
    "pydantic==2.12.5",
    "pandas==2.3.3",
    "sqlglot==30.0.3",
]
```

`sqlglot` is imported by `optimization/{benchmarking,mv_fingerprint,unified_loop,wide_schema,wide_schema_history}.py`.
Every one of those except `mv_fingerprint.py` imports it lazily inside the calling function;
`mv_fingerprint.py` imports at module scope because it is nothing but sqlglot. `applier.py`
does **not** import it — it only mentions it in a comment (`applier.py:1534`, "A smarter
extractor (sqlglot) would be more…"), so an earlier revision of this report listing it as an
importer was wrong.

#### How entrypoints are registered — they are not

**There is no `[project.scripts]` block and no wheel entry point.** The four entrypoints
are **notebook files** referenced by path:

```
src/genie_space_optimizer/jobs/
  __init__.py
  _helpers.py                      # shared _banner / _log / _diagnostic
  run_intake_and_snapshot.py       # task 1
  run_benchmark_qc_and_repair.py   # task 2
  run_optimize.py                  # task 3
  run_publish_and_audit.py         # task 4
```

Each begins with `# Databricks notebook source` and is uploaded either by
`databricks sync` (bundle path, `databricks.yml:22-23` sync include) or by
`upload_source_notebook` (installer path, `scripts/deploy_lib/gso_job.py:114-124`). The
wheel is attached as a serverless environment dependency, not as an executable:

```198:203:databricks.yml
      environments:
        - environment_key: default
          spec:
            environment_version: "4"
            dependencies:
              - .build/genie_space_optimizer-0.0.0-py3-none-any.whl
```

The POV's `python_wheel_task: {package_name: genie_space_optimizer, entry_point: metric_view_advisor}`
has **no precedent in this repo** and would be the first wheel task.

#### How a task reads job parameters and task values

**Job parameters: `dbutils.widgets`.** See [§1.2](#12-the-four-place-parameter-lockstep-requirement).

**Task values: forbidden.** Cross-task state is Delta-by-`run_id` only, enforced by test:

```397:405:packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py
def test_no_dbutils_notebook_run_or_task_values_in_new_notebooks():
    """D9: the new notebook entrypoints must not use dbutils.notebook.run or
    inter-task task values."""
    jobs_dir = _PKG_ROOT / "src" / "genie_space_optimizer" / "jobs"
    for nb in _TASK_NOTEBOOK.values():
        src = (jobs_dir / nb).read_text()
        assert "dbutils.notebook.run" not in src, f"{nb} uses dbutils.notebook.run"
        assert "taskValues.set" not in src, f"{nb} sets task values"
        assert "taskValues.get" not in src, f"{nb} reads task values directly"
```

The replacement pattern is the **artifact table** — `write_required_artifact` upstream,
`load_latest_artifact_record` downstream, with a missing-record gate. The QC handoff is
the worked example, and its ordering is itself pinned by test:

```186:211:packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py
def test_benchmark_qc_is_a_required_verified_handoff():
    jobs_dir = (
        _PKG_ROOT
        / "src"
        / "genie_space_optimizer"
        / "jobs"
    )
    repair_src = (jobs_dir / "run_benchmark_qc_and_repair.py").read_text()
    assert (
        'write_required_artifact(\n'
        '    spark, run_id, "benchmark_qc", _qc_payload,'
    ) in repair_src

    for notebook_name in ("run_optimize.py", "run_publish_and_audit.py"):
        src = (jobs_dir / notebook_name).read_text()
        load = src.index("_benchmark_qc_record = load_latest_artifact_record(")
        missing_gate = src.index("if _benchmark_qc_record is None:", load)
        verified_payload = src.index(
            '_benchmark_qc = _benchmark_qc_record["payload"]',
            missing_gate,
        )
        eligibility_gate = src.index(
            'if _benchmark_qc.get("optimization_eligible") is False:',
            verified_payload,
        )
        assert load < missing_gate < verified_payload < eligibility_gate
```

**This is the pattern an MV advisor must follow**: write an `mv_candidates` artifact,
read it downstream by `run_id`.

#### Package layout

<!-- BEGIN GENERATED: package-layout (scripts/gap_report_counts.py) -->
```text
src/genie_space_optimizer/
  _telemetry.py, _version.py, _workspace_client.py
  backend/        job_launcher.py, utils.py          # shared with Workbench
  common/         config.py (2894 L), genie_client.py, metric_view_catalog.py,
                  asset_semantics.py, delta_helpers.py, warehouse.py, uc_metadata.py, ...
  integration/    trigger.py, apply.py, discard.py, revert.py, levers.py, types.py
  iq_scan/        scoring.py, context.py, rls_audit.py
  jobs/           the four notebooks + _helpers.py
  optimization/   applier.py (4788 L), benchmarking.py (4594 L), unified_loop.py (3877 L),
                  preflight.py (3461 L), state.py (1871 L), publish.py, ddl.py,
                  eval_runner.py, leakage.py, models.py, champion.py,
                  wide_schema*.py, genie_eval_taxonomy.py,
                  mv_fingerprint.py (1474 L), mv_scoring.py (1589 L),
                  mv_state.py (871 L), mv_yaml.py (1997 L), ...
```
<!-- END GENERATED: package-layout -->

**Existing metric-view surface** (relevant — the advisor is not starting from zero):

| Module | Role |
|---|---|
| `common/metric_view_catalog.py` (690 L) | `detect_metric_views_via_catalog_with_outcomes(...)` / `detect_metric_views_via_catalog(...)` / `summarize_outcomes(...)` — DESCRIBE-based MV detection and YAML extraction |
| `common/asset_semantics.py` | `is_metric_view`, table/MV shelf split, measure maps |
| `optimization/preflight.py` | `_profile_metric_view`, reclassification into `_metric_view_yaml` |
| `optimization/benchmarking.py` | MV join precheck/repair, `MEASURE()` wrapping, `build_metric_view_measures` |
| `optimization/wide_schema_history.py` | `system.query.history` mining (`:248`), warehouse-history fallback (`:357`) |
| `optimization/mv_yaml.py` (1997 L) | `generate` / `validate` / `validate_registered` / `create_ddl` — the only renderer of metric view YAML and of its `CREATE VIEW` wrapper, plus the static validator (unsupported-field, format-type, transitive-join, synonym, comment, echo and capability checks) and its MV-D24 bring-your-own twin `validate_registered` (the safety subset only — generation conventions become warnings, not errors). `CapabilityRow` is the Protocol the backend's `MvCapabilityRow` satisfies structurally |

Note the last row: **query-history demand signal (the POV's **D** component) already
exists**, including the CMK/unavailable fallback path.

---

### 1.4 Genie Benchmark Eval API

**This is the one area where the repo is ahead of the POV.** POV Recommendation 2 asks
for "a thin adapter so a contract change costs you one file." That adapter exists.

**Single seam:** `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py`
(890 L). The `EvalRunner` Protocol (`:305-330`) is the only interface the optimizer
evaluates through; `OfficialBenchmarkRunner` (`:468-518`) is the sole implementation.
Additive MV-advisor methods on that same class: `run_subset` (`:520-530`),
`list_eval_runs` (`:532-544`), and `lift_report` (`:232-301` module function /
`:546-552` method). There is no second adapter.

```1:19:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py
"""EvalRunner seam over the official Databricks Genie Benchmark (Eval-Run) API.

Phase 1 of GSO Optimizer v2 (see ``GSO_OPTIMIZER_V2_TODO.md`` §4 Phase 1).

This module introduces the :class:`EvalRunner` protocol — the single seam the
optimizer evaluates through — and :class:`OfficialBenchmarkRunner`, the
implementation that drives the native eval-run methods on the Databricks SDK
``GenieAPI`` (databricks-sdk v0.102.0)::

    w.genie.genie_create_eval_run(space_id, benchmark_question_ids=None)  # None ⇒ all
    w.genie.genie_get_eval_run(space_id, eval_run_id)                     # poll status
    w.genie.genie_list_eval_runs(space_id, page_size, page_token)         # cross-run history
    w.genie.genie_list_eval_results(space_id, eval_run_id, page_size, page_token)
    w.genie.genie_get_eval_result_details(space_id, eval_run_id, result_id)

Decision **D1**: the official API is the SOLE eval runner in v2 — scoring is the
server-side ``assessment`` (GOOD/BAD/NEEDS_REVIEW), accuracy is
``num_correct / num_questions``, and lever routing reads ``assessment_reasons``.
We never double-run the retired in-process scorer path.
```

| POV API | Repo call | Called via | Line |
|---|---|---|---|
| Create eval run | `genie_create_eval_run(space_id, benchmark_question_ids=qids)` | SDK method on `w.genie` | `eval_runner.py:583-585` |
| Get / poll | `genie_get_eval_run(space_id, eval_run_id)` | SDK | `eval_runner.py:638` |
| List eval runs | `genie_list_eval_runs(space_id, page_size, page_token)` | SDK, paginated via `OfficialBenchmarkRunner.list_eval_runs` | `eval_runner.py:544-546` |
| List results | `genie_list_eval_results(space_id, eval_run_id, page_size, page_token)` | SDK, paginated | `eval_runner.py:662-667` |
| Result details | `genie_get_eval_result_details(space_id, eval_run_id, result_id)` | SDK | `eval_runner.py:669-671` |

All calls are **SDK methods, not raw REST**. Neither `optimization/benchmarking.py` nor
`common/genie_client.py` touches the eval-run APIs.

**Status handling** — every enum the POV names is present:

```59:63:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py
# ``EvaluationStatusType`` terminal values. Only ``DONE`` is a success.
_SUCCESS_STATUS = "DONE"
_TERMINAL_STATUSES = frozenset(
    {"DONE", "EVALUATION_CANCELLED", "EVALUATION_FAILED", "EVALUATION_TIMEOUT"}
)
```

One correction to the POV: the SDK field is **`eval_run_status`, not `status`** — flagged
in the module docstring at `:29-31` as a planning-note error already caught once.

**Assessment reasons.** All four the POV names (`EMPTY_RESULT`, `RESULT_MISSING_ROWS`,
`RESULT_EXTRA_ROWS`, `RESULT_MISSING_COLUMNS`) exist in
`optimization/genie_eval_taxonomy.py:8-20`, alongside `RESULT_EXTRA_COLUMNS`,
`SINGLE_CELL_DIFFERENCE`, `EMPTY_GOOD_SQL`, `COLUMN_TYPE_DIFFERENCE`, and a family of
`LLM_JUDGE_*` keys, exported as `ASSESSMENT_REASON_CODES` (`genie_eval_taxonomy.py:87`).
Per-question assessment is `GOOD` / `BAD` / `NEEDS_REVIEW`
(`eval_runner.py:416-439`).

**Retry and polling.**

```122:133:packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py
EVAL_RUN_POLL_INTERVAL_SECONDS: int = int(
    os.getenv("GSO_EVAL_RUN_POLL_INTERVAL_SECONDS", "20")
)
"""Seconds between ``genie_get_eval_run`` status polls."""

EVAL_RUN_TIMEOUT_SECONDS: int = int(
    os.getenv("GSO_EVAL_RUN_TIMEOUT_SECONDS", "2700")
)
"""Per-run terminal-status timeout (~45 min — a single 30–40 Q run is ~17 min)."""

EVAL_RUN_PAGE_SIZE: int = int(os.getenv("GSO_EVAL_RUN_PAGE_SIZE", "100"))
"""Page size for the paginated ``genie_list_eval_results`` sweep."""
```

Fixed 20 s interval (no backoff), 2700 s deadline, never sleeping past the deadline
(`eval_runner.py:633-655`). An **outer** transient retry sits in `unified_loop.py`: up to
`_MAX_TRANSIENT_EVAL_RETRIES = 2` extra attempts, but only for
`_TRANSIENT_EVAL_STATUSES = {"EVALUATION_TIMEOUT", "EVALUATION_CANCELLED"}` —
`EVALUATION_FAILED` is returned on the first attempt (`unified_loop.py:137-150`,
`:1278-1298`).

`tests/unit/test_eval_timeouts.py` covers conversation/statement timeouts, **not** the
eval-run adapter. Seam contract tests (terminal statuses, taxonomy reasons, `run_subset`
serialization, `lift_report`) live in `tests/unit/test_eval_runner.py`.

**Where `eval_run_id` is stored:** `genie_opt_iterations.eval_run_id` /
`.eval_run_status`, written by `state.write_iteration` (`state.py:889`, columns at `:983-1004`). This
directly satisfies POV Recommendation 2 ("store the `eval_run_id`, not a copied score") —
though the accuracy is *also* stored, in `overall_accuracy`.

**Baseline eval is in-process, not a task:**

```2894:2899:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py
    baseline_eval = _native_eval(
        w,
        space_id=space_id,
        benchmarks=benchmarks,
        iteration=0,
    )
```

reached from `jobs/run_optimize.py:337` → `run_unified_optimization_loop(...)`. Candidate
evals run in the same loop at `unified_loop.py:3373-3378`.

---

### 1.5 The patch model

**There is no `field_path` + `new_value` patch model.** Repo-wide grep for `field_path`
in `packages/genie-space-optimizer/src/` returns nothing.

`optimization/models.py` is **not** a patch model — it is champion promotion:

```1:9:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/models.py
"""Champion selection for Genie Agent optimization runs (Delta-only).

GSO v2 (Phase 5, D3/D7): tracking and versioning are Delta-only. There is no
MLflow LoggedModel snapshot, no UC Model Registry version, and no per-mutation
MLflow run. The champion iteration is selected from ``genie_opt_iterations``
(highest accepted ``overall_accuracy``) and marked in Delta via
``mark_champion_iteration``. Rollback stays Delta-based — see
``applier.rollback`` (in-memory ``pre_snapshot`` re-PATCH) and
``integration.discard`` (``genie_opt_runs.config_snapshot`` re-PATCH).
```

**Actual patch shape** — an untyped `dict`:

```2649:2659:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py
        patch_dict: dict = {
            "type": patch_type,
            "target": target,
            "new_text": new_text,
            "old_text": "",
            "lever": lever,
            "risk_level": classify_risk(patch_type),
            "predicted_affected_questions": p.get("questions_fixed", 0),
            "grounded_in": p.get("grounded_in", []),
            "source_proposal_id": p.get("proposal_id", ""),
        }
```

| POV field | Repo equivalent |
|---|---|
| `field_path` | `type` (a `PATCH_TYPES` key) + `target` (asset/column identifier) — a **type-and-target** model, not a path model |
| `new_value` | `new_text` (with `old_text` for the prior value) |
| `operation` | `op` on the *rendered command*, one of `add` / `update` / `remove` / `update_section` / `rewrite` |

**Patch types.** The canonical registry is `PATCH_TYPES` in `common/config.py:2005` — 51
keys. The Lever-2 metric-view subset exists (`add_mv_measure`, `update_mv_measure`,
`remove_mv_measure`, `add_mv_dimension`, `remove_mv_dimension`, `update_mv_yaml`, all
`scope: uc_artifact`) at `config.py:2068-2103`, joined in Prompt 7 by
`mv_attach_data_source` (`scope: genie_config`, `HIGH_RISK`) at `config.py:2104-2115`.

The **live loop allowlist is 11 types and contains no MV type** — and under **MV-D16** the
attach type stays out of it deliberately, because this frozenset is the surface the LLM is
allowed to propose from, not the surface the engine is allowed to apply:

```83:97:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py
_ALLOWED_PATCH_TYPES: frozenset[str] = frozenset(
    {
        "update_description",
        "update_column_description",
        "add_column_synonym",
        "add_instruction",
        "update_instruction_section",
        "add_example_sql",
        "add_join_spec",
        "update_join_spec",
        "add_sql_snippet_measure",
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
    }
)
```

**Where patches are applied.**

| Function | Signature | Location |
|---|---|---|
| `proposals_to_patches` | `(proposals: list[dict]) -> list[dict]` | `applier.py:2411` |
| `render_patch` | `(patch: dict, space_id: str, space_config: dict) -> dict` | `applier.py:2926` |
| `_apply_action_to_config` | `(config: dict, action: dict) -> bool` | `applier.py:3442` |
| `_apply_action_to_uc` | `(w: WorkspaceClient, action: dict) -> bool` | `applier.py:4002` |
| `apply_patch_set` | `(w, space_id, patches, metadata_snapshot, *, apply_mode, deploy_target, force_apply, benchmark_corpus) -> dict` | `applier.py:4091` |

**This was the report's "critical gap — nothing attaches a metric view to a space", and
Prompt 7 closed it.** `add_table` / `remove_table` mutate `data_sources.tables` only
(`applier.py:3905-3919`); the attach now has its own `metric_views` branch beside them
(`applier.py:3921-3946`), which appends the entry and re-sorts the collection the way the
Genie API requires. The Lever-2 `uc_artifact` MV sections remain explicit config-level
no-ops, and that is still correct — they describe an already-attached view's measures and
dimensions, which live in UC and not in the space config:

```3974:3977:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py
    # ── TVF / MV operations (config-level no-ops for uc_artifact patches) ──
    if section in ("tvf_parameters", "tvf_definition", "tvfs", "mv_measures", "mv_dimensions", "mv_yaml"):
        if section == "tvfs":
            funcs = config.setdefault("instructions", {}).setdefault("sql_functions", [])
```

Only the `tvfs` branch does anything (it appends to `instructions.sql_functions`); the
three `mv_*` sections fall straight through to `return True` at `applier.py:3993`.
`mv_attach_data_source` is routed nowhere near this region — it has its own `metric_views`
branch above, which is the point of MV-D16's "a real applier action, not the render-only
path".

`_apply_action_to_uc` handles only `update_column_description`, `update_description`, and
`update_tvf_sql` — **no MV DDL**. Repo-wide, `WITH METRICS` now has exactly one hit, and it is
not an executable path — it is the `created_by` column comment on the MV-D7 created-objects
table added by Commit 1:

```262:262:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
    created_by          STRING        NOT NULL COMMENT 'Identity that executed CREATE VIEW ... WITH METRICS. Always the consenting user under OBO — never the service principal',
```

The apply path still executes no metric-view DDL, and that is unchanged. What did change with
Prompt 5.5 is where the statement is *built*: `optimization/mv_yaml.py` renders both the YAML
body and the wrapping `CREATE VIEW … WITH METRICS LANGUAGE YAML` (`mv_yaml.create_ddl`), and is
the only module in the package that renders either — pinned by two guards:
`test_mv_yaml_is_the_only_module_that_renders_yaml`, which fails on a `yaml.dump` anywhere else,
and `test_mv_yaml_is_the_only_module_that_builds_metric_view_ddl`, which fails on `CREATE VIEW`
or `WITH METRICS` in any executable string outside it — the `ddl.py:262` comment above is
allowed by exact `(path, line)` pin, not by a substring exemption, so a new f-string assembly
site fails even if it copies that wording. Execution remains the backend's under OBO (MV-D1);
the job, which runs as the service principal, still never issues the statement.

**Open follow-up — the L signal needs a `WATCH_SYSTEM_GRANTS` addition, and Prompt 6a owns it.**
Recorded during Prompt 6 so the grant question is visible now rather than discovered when 6a
tries to build a lineage producer. `WATCH_SYSTEM_GRANTS` (`scripts/deploy_lib/uc.py:30-43`) is
the single source of truth for the app service principal's system-table reads, and it grants
`SELECT` on `system.access.table_lineage` (`:40`) — **table-level lineage only**. The **L**
signal is a Jaccard over *column* sets (`mv_scoring.py:158`, `LineageOverlap`), which needs
`system.access.column_lineage`, and that table appears nowhere in the list. So L is not a
code-only change: it needs a grant addition in `uc.py`, which both install paths pick up, plus
a re-run of `scripts/grant_permissions.py` against existing deployments — and the grant is
admin-only and best-effort, so 6a must handle its absence rather than assume it.

Two consequences that shape 6a rather than merely inform it. The grant is workspace-scoped and
issued at install time, so an existing deployment upgrading into 6a will have the code and not
the grant until an admin re-runs the script — meaning L must report `UNAVAILABLE` on a
permission failure (MV-D15) rather than an empty overlap, or every candidate in a
not-yet-regranted workspace would score as having *disjoint* lineage. And these reads are
service-principal reads by design (system tables are not OBO-readable), which is why POV Part 5's
"already scoped" sentence needed the correction recorded in this same change: SP-computed
lineage evidence is filtered at presentation, not at read.

**Resolved in Prompt 6a — column-grain L (MV-D19 = (b)) with the grant landed, one deploy-time
probe still open.** MV-D19 settled on **column grain** as a *correctness* finding, not a cost
preference: at table grain the dominant footprint case (`NEW_METRIC_VIEW`) degenerates to
`|candidate_tables| / |footprint_tables|` and inverts the signal — penalizing the clean
single-table measure and rewarding sprawl — so a table-grain L would report `COMPUTED` on a
number that is noise-or-inverted at 0.35 weight, the exact `COMPUTED`-vs-`UNAVAILABLE` confusion
MV-D15 exists to prevent (full reasoning in the playbook's MV-D19 record). The `SELECT` on
`system.access.column_lineage` is now the eighth row of `WATCH_SYSTEM_GRANTS`
(`scripts/deploy_lib/uc.py`), so both install paths grant it and `grant_permissions.py` re-runs
it into existing deployments — additive, the table-grain row stays. The producers live in
`optimization/mv_signals.py` (`lineage_signal`, `demand_signal`) behind an injected `run_query`
seam, read as the SP, and honor MV-D15: a permission/absence failure is `UNAVAILABLE` with the
missing grant named, a resolved-but-disjoint read is `EMPTY`. They were **wired into the advisor
in Prompt 6b** (see the next note).
*The empirical item is now probed (favorable); only the SP grant application remains.* The Prompt 6a
probe ran against workspace `fevm-serverless-stable-6t92c3`: `system.access.column_lineage`
**carries `entity_metadata.genie_space_id`** (in its `entity_metadata` struct, like `table_lineage`),
is **heavily populated** (~117M rows/90d; ~458K Genie-attributed across 2,393 spaces), and the exact
`_footprint_sql` shape executes and returns real `(source_table_full_name, source_column_name)` rows
for a live space (column names arrive upper-case — the producer lower-cases both sides). So the
`genie_space_id` fallback contemplated here is **not needed** on this schema, and L will contribute
rather than sit `UNAVAILABLE`. The probe ran as an interactive user with grants; the remaining deploy
step is applying the new `WATCH_SYSTEM_GRANTS` row to the **app service principal** via
`grant_permissions.py`. Until that runs in a given workspace, L reports `UNAVAILABLE` with the grant
named (MV-D15) — never a silent zero.

**Wired in Prompt 6b — the advisor scores on all four signals.** `run_optimize.py` constructs
`warehouse_reader(w, warehouse_id)` beside the embedding client and injects it as `signal_reader`
into `run_mv_advisor_phase`; `_advise` runs `lineage_signal` and `demand_signal` per candidate
(`_candidate_signals` in `mv_advisor.py`), passes their payloads into `candidate_from_measure` (the
`LineageOverlap()` / `DemandSignal()` empty defaults are now optional fallbacks, not the only
value), reports each producer's real status through `advisor_statuses(lineage, demand)`, and folds
the status + UNAVAILABLE reason into the proposal evidence (`_with_signal_evidence`, under
`evidence["signal_status"]`). Coverage is now a per-workspace fact: where the grant and data are
present L and D lift `evidence_coverage` toward 1.0 and HIGH (`>= 0.80`) is reachable; where they
are absent the producers report `UNAVAILABLE`-with-reason and the advisor is byte-identical to its
pre-6b behaviour (pinned by `test_a_reader_that_always_raises_reproduces_the_no_reader_baseline`).
The integration shape — a real producer over fixture rows reaching the scorer — is pinned by
`test_the_signal_reader_lifts_coverage_and_unlocks_high`, and the firewall (no history literal on a
shipped surface) by `test_demand_history_text_never_reaches_the_evidence`.

**Deferred, bounded cost — D re-reads and re-scans the space history per candidate.**
`demand_signal` reads the whole space `system.query.history` and re-fingerprints it (`corpus_scan`,
one sqlglot parse per statement) on every call, applying the per-candidate fingerprint filter only
at the end; called once per candidate it repeats that identical read and scan up to
`MV_ADVISOR_MAX_CANDIDATES` (10) times. Magnitude from the 6a probe: ~190 Genie rows/space over 90d,
so a few hundred statements parsed ~10× plus 10 warehouse round-trips — a second or two of CPU
inside a phase that already runs a full optimization loop. Shipped as-is deliberately, **not**
worked around by duplicating the scan into the advisor (which would fork the canonicalizer) nor by
touching the committed 6a producer. The named fix, additive to `mv_signals.py` if that read ever
shows up hot, is a batch `demand_signals` that reads-and-scans once and returns a per-fingerprint
`dict[str, SignalResult]`. **The 10-candidate cap is load-bearing for this deferral**: raising
`MV_ADVISOR_MAX_CANDIDATES` scales this cost linearly, so that change should land the batch variant
with it. (L is genuinely per-candidate — its footprint read is scoped by each candidate's
`source_tables` — so it is not the same waste and stays per-candidate.)

**Closed in Prompt 7 — `update_mv_yaml` is validated**
([#331](https://github.com/databricks-solutions/databricks-genie-workbench/issues/331)).
`update_mv_yaml` (`config.py:2098`) was the one path by which LLM-authored metric view YAML
entered the system, and it transported `new_text` verbatim while every engine-generated path
was checked by `mv_yaml.validate`. `render_patch` now runs that same validator on the
incoming YAML (`applier.py:3392-3404`) and raises `RuntimeError` when it fails, which is the
existing refusal signal: `apply_patch_set` records the patch as `dropped_validation` and
keeps applying the rest of the set rather than aborting the attempt. Pinned by
`test_update_mv_yaml_refuses_yaml_that_fails_validation` and
`test_update_mv_yaml_accepts_engine_valid_yaml`, the second built from `mv_yaml.generate`
output so the gate is proven not to reject the engine's own YAML.

Existing patches *can* modify an **already-attached** MV's column metadata, because lookup
searches both shelves:

```2208:2215:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py
def _find_table_in_config(config: dict, table_id: str) -> dict | None:
    """Find a table or metric view in data_sources by identifier."""
    ds = config.get("data_sources", {})
    for source_list in [ds.get("tables", []), ds.get("metric_views", [])]:
        for t in source_list:
            if t.get("identifier") == table_id:
                return t
    return None
```

That is the extent of Lever 2 today — "Update metric view column descriptions"
(`integration/levers.py:10`).

**Rollback is snapshot-based, not patch-inverse.**

```4617:4627:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py
def rollback(
    apply_log: dict,
    w: WorkspaceClient | None,
    space_id: str,
    metadata_snapshot: dict | None = None,
) -> dict:
    """Restore the Genie Agent config to its pre-patch state.

    Primary mechanism: replace current config with ``apply_log["pre_snapshot"]``.
    Fallback: execute rollback_commands in reverse order (HIGH -> MEDIUM -> LOW).
    """
```

User-facing revert: `integration/revert.py:55-63` `revert_optimization(...)` with
`target: RevertTarget = "champion" | "baseline"`; discard:
`integration/discard.py:28-33`. All three restore a **whole serialized config**. Delta
bookkeeping is `state.mark_patches_rolled_back` (`state.py:1168-1175`).

**Consequence for the POV.** POV §7.8 step 6 says the MV attachment "is an ordinary patch,
[so] it inherits the existing versioning, diff, and rollback machinery." Half true: the
machinery carries it, but the patch type and the applier branch had to be built (Prompt 7),
and the DDL execution path is a backend/OBO surface that the job never gains (MV-D1). And
POV §7.8's `DETACH_ONLY_NEVER_DROP` semantics — reverting *one* patch while leaving a UC
object alone — has no analogue in a whole-snapshot rollback model. **MV-D16 resolves that by
placing the attach where the two coincide**: between iteration-0 and the first lever patch,
where the pre-attach snapshot differs from the post-attach one by the attach and nothing
else, so a whole-snapshot revert *is* a targeted detach. The UC object is untouched either
way — dropping it is an explicit backend endpoint, never a consequence of a measurement.

**Leakage firewall.** `optimization/leakage.py:421-427` `is_benchmark_leak(...)` is the
entrypoint. Its runtime field map covers only example-SQL types, preceded by the scoping
comment that says why:

```286:296:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py
_PATCH_TEXT_FIELDS: dict[str, tuple[str, ...]] = {
    "add_example_sql": ("example_question", "example_sql"),
    "update_example_sql": ("example_question", "example_sql"),
}

# Every patch/proposal type that persists a question+SQL pair into the live
# space's Example SQL Queries section. The deterministic scored-benchmark
# hard-block must cover all of them on every write path (v2 §3.6 / D8).
_EXAMPLE_SQL_PATCH_TYPES: frozenset[str] = frozenset(
    {"add_example_sql", "update_example_sql"}
)
```

Per `packages/genie-space-optimizer/AGENTS.md`: *"New patch types that can carry text must
be routed through the same firewall."* An MV proposal carries `comment`, `display_name`,
and `synonyms` — all free text — so a new MV patch type must be added to
`_PATCH_TEXT_FIELDS`.

---

### 1.6 Persistence

#### Delta — ten tables in `_ALL_DDL`, not ~15

`optimization/ddl.py` is the sole definition point:

```304:315:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
_ALL_DDL: dict[str, str] = {
    TABLE_RUNS: _GENIE_OPT_RUNS_DDL,
    TABLE_STAGES: _GENIE_OPT_STAGES_DDL,
    TABLE_ITERATIONS: _GENIE_OPT_ITERATIONS_DDL,
    TABLE_PATCHES: _GENIE_OPT_PATCHES_DDL,
    TABLE_BENCHMARK_MUTATIONS: _GENIE_OPT_BENCHMARK_MUTATIONS_DDL,
    TABLE_ARTIFACTS: _GENIE_OPT_ARTIFACTS_DDL,
    TABLE_MV_CANDIDATES: _GENIE_OPT_MV_CANDIDATES_DDL,
    TABLE_MV_CONSENTS: _GENIE_OPT_MV_CONSENTS_DDL,
    TABLE_MV_CREATED_OBJECTS: _GENIE_OPT_MV_CREATED_OBJECTS_DDL,
    TABLE_MV_SUPPRESSIONS: _GENIE_OPT_MV_SUPPRESSIONS_DDL,
}
```

> **Current:** the final four entries support the metric-view advisor. MV-D7 added
> candidates, consents, and created objects in Prompt 1; Prompt 15.3 added the
> per-measure suppression ledger so a rejected member cannot reappear in a
> membership-changed bundle.

| Table | Grain | Partition | Notes |
|---|---|---|---|
| `genie_opt_runs` | one row per run | `space_id` | holds `config_snapshot` (the revert anchor), `triggered_by`, `llm_model` |
| `genie_opt_stages` | stage transitions | `run_id` | `task_key` comment still lists the **retired** stage names |
| `genie_opt_iterations` | one row per eval attempt | `run_id` | 40 columns; carries `eval_run_id`, `config_json`, `is_champion`, loop state |
| `genie_opt_patches` | one row per applied patch | `run_id` | `patch_json`, `command_json`, `rollback_json`, `rolled_back` |
| `genie_opt_benchmark_mutations` | benchmark change ledger | `run_id` | |
| `genie_opt_artifacts` | generic JSON blob handoff | `run_id` | **the task-to-task handoff table** |
| `genie_opt_mv_candidates` | one row per (`target_space_id`, `dedup_fingerprint`) | `target_space_id` | MV-D7; upserted, so a re-proposing run refreshes rather than duplicates |
| `genie_opt_mv_consents` | one row per `probe_id` | *none* | MV-D7; unpartitioned because `run_id` is NULL until trigger time |
| `genie_opt_mv_created_objects` | one row per (`run_id`, `suggestion_id`) | `run_id` | MV-D7; `CREATED\|ATTACHED\|DETACHED\|DROPPED` lifecycle |
| `genie_opt_mv_suppressions` | one row per (`target_space_id`, `measure_fingerprint`) | `target_space_id` | Prompt 15.3; bundle rejection fans out here so rejected measures stay suppressed when bundle membership changes |

Two more exist outside `_ALL_DDL`: `genie_opt_scan_snapshots` (`scan_snapshots.py:41-58`)
and the per-domain `genie_benchmarks_{domain}` (`benchmarks.py:194-209`).

Names are constants in `common/config.py:1834-1891`. FQN is
`{catalog}.{schema}.<table>`, resolved by string replacement:

```69:70:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py
    for name, ddl in _ALL_DDL.items():
        resolved = ddl.replace("{catalog}", catalog).replace("{schema}", schema)
```

`genie_opt_artifacts.artifact_kind` is the extension point an advisor would use — it is a
documented enum in the column comment:

```182:182:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
    artifact_kind       STRING        NOT NULL COMMENT 'run_manifest | wide_schema_inventory | wide_schema_evidence | wide_schema_selection_plan | wide_schema_profile_telemetry | wide_schema_prompt_telemetry | wide_schema_audit | space_metadata | benchmark_qc | space_quality_enrichment | publish_record | mv_candidate_ddl',
```

#### Migration mechanism

Additive only, via a tuple of `(table, column, "TYPE COMMENT '...'")`:

```293:298:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
ADDITIVE_COLUMN_MIGRATIONS: tuple[tuple[str, str, str], ...] = (
    (TABLE_RUNS, "job_id", "STRING COMMENT 'Databricks Job definition ID'"),
    (TABLE_PATCHES, "provenance_json", "STRING COMMENT 'JSON: full provenance chain from judge verdicts to this patch'"),
    (TABLE_RUNS, "llm_model", "STRING COMMENT 'Databricks Model Serving endpoint selected for this optimization run'"),
    (TABLE_RUNS, "benchmark_policy", "STRING COMMENT 'Benchmark handling policy: review_only|repair_allowed'"),
    (TABLE_RUNS, "benchmark_mutation_count", "INT COMMENT 'Number of live benchmark additions or SQL updates performed by this run'"),
```

Applied as `ALTER TABLE ... ADD COLUMN` by `state._apply_one_migration` (`state.py:167-181`),
driven from `ensure_optimization_tables` → `_migrate_add_columns` (`state.py:54-92`,
`:119-146`). A warehouse-side twin lives at `common/warehouse.py:183-261`.

**To add a new column:** edit the CREATE DDL string, append to
`ADDITIVE_COLUMN_MIGRATIONS`, wire the writer, extend the Workbench API model, add tests —
all in one commit, per the "Persistence rules" section of
`packages/genie-space-optimizer/AGENTS.md`.

**To add a new table:** add a `TABLE_*` constant in `config.py`, a `_GENIE_OPT_*_DDL`
string in `ddl.py`, register it in `_ALL_DDL`. No migration-list entry is needed —
`ensure_optimization_tables` iterates `_ALL_DDL` with `CREATE TABLE IF NOT EXISTS`.
Column migrations only apply to tables already in `_ALL_DDL` (`state.py:126-131`).

> The POV's `candidate_table: "main.genie_workbench.mv_candidates"` implies a *seventh*
> table in a different schema. Prefer either a `genie_opt_mv_candidates` table in
> `_ALL_DDL`, or — cheaper and consistent with the existing handoff idiom — an
> `mv_candidates` `artifact_kind` row in `genie_opt_artifacts`.
>
> **Resolved by `MV-D7`:** the table, plus two more (`genie_opt_mv_consents`,
> `genie_opt_mv_created_objects`), because all three hold mutable state that outlives one
> run. See §3 item 7.

#### No Volumes convention for run artifacts

The POV's `ddl_artifact_path: "/Volumes/main/genie_workbench/runs/5521/metric_views.sql"`
has no basis. Grep for `/Volumes` across `packages/` returns **zero** hits; in `backend/`
it appears only in `tests/test_deploy_lib.py` fixtures. The one real Volumes use is the
GSO **wheel** upload path:

```164:167:scripts/deploy_lib/gso_job.py
    wheel_path = (
        cfg.gso_wheel_path
        or f"/Volumes/{cfg.catalog}/{cfg.gso_schema}/app_artifacts/genie_space_optimizer-0.0.0-py3-none-any.whl"
    )
```

Run artifacts go to Delta (`genie_opt_artifacts`), not Volumes.

#### Lakebase (the app's own Postgres)

Separate from GSO Delta. `backend/services/lakebase.py::_ensure_schema` (`:143-285`)
creates schema `genie` with 10 tables: `scan_results`, `starred_spaces`, `seen_spaces`,
`optimization_runs` (a legacy accuracy history, *not* `genie_opt_runs`),
`hidden_optimization_runs`, and five `watch_*` cache tables.

`backend/services/gso_lakebase.py` contains readers for Lakebase-synced copies of the GSO
Delta tables, but they are **switched off**:

```15:24:backend/services/gso_lakebase.py
# Postgres schema where synced tables appear — matches the UC schema name.
_GSO_PG_SCHEMA = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")

# Synced tables are created with this suffix in the same UC schema.
_SYNCED_SUFFIX = "_synced"

# Disabled until Databricks SDK supports Lakebase Autoscaling synced table
# creation. All reads fall through to Delta table queries via SQL Warehouse.
# Flip to True and redeploy once synced tables are provisioned.
_SYNCED_TABLES_ENABLED = False
```

With the flag false, `_get_pool()` returns `None` and every loader returns empty. The app
reads GSO state over the SQL warehouse instead.

---

### 1.7 The FastAPI app

#### Router layout

```211:227:backend/main.py
# Mount all routers
app.include_router(analysis_router)
app.include_router(spaces_router)
app.include_router(admin_router)
app.include_router(auth_router)
app.include_router(create_router)
app.include_router(auto_optimize_router)

# GenieWatch (observability) — all routes under /api/watch/*
app.include_router(watch_spaces_router)
app.include_router(watch_traffic_gaps_router)
app.include_router(watch_cost_router)
app.include_router(watch_usage_router)
app.include_router(watch_feedback_router)
app.include_router(watch_resources_router)
app.include_router(watch_settings_router)
app.include_router(watch_admin_router)
```

Prefixes are declared on the router objects, not at inclusion: `/api` (analysis, spaces),
`/api/admin`, `/api/auth`, `/api/create`, `/api/auto-optimize`
(`backend/routers/auto_optimize.py:54`), `/api/watch/*`.

#### OBO token extraction

```104:121:backend/main.py
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path.startswith("/api/"):
            token = request.headers.get("x-forwarded-access-token", "")
            if token:
                set_obo_user_token(token)
                logger.info("OBO: using user token for %s", request.url.path)
            else:
                logger.info("OBO: no x-forwarded-access-token, using SP for %s", request.url.path)
            request.state.user_token = token
        else:
            request.state.user_token = ""

        response = await call_next(request)

        is_streaming = getattr(response, "media_type", "") == "text/event-stream"
        if not is_streaming:
            clear_obo_user_token()
        return response
```

Three accessors, and the distinction matters for the POV's security model:

```104:114:backend/services/auth.py
def get_workspace_client() -> WorkspaceClient:
    """Get the WorkspaceClient for the current context.

    Returns the OBO (per-user) client if set, otherwise the default
    singleton. This ensures all SDK calls in the request path use the
    user's credentials when running on Databricks Apps.
    """
    obo = _obo_client.get()
    if obo is not None:
        return obo
    return _get_default_client()
```

```117:127:backend/services/auth.py
def require_obo_workspace_client() -> WorkspaceClient:
    """Return only the request's user-authorized client.

    Unlike :func:`get_workspace_client`, this function never falls back to the
    app service principal. Use it for reads whose visibility is explicitly
    scoped to the current user's permissions.
    """
    obo = _obo_client.get()
    if obo is None:
        raise RuntimeError("This operation requires user authorization")
    return obo
```

`get_service_principal_client()` (`:130-139`) returns the singleton SP client.

> **`require_obo_workspace_client` is the right primitive for the POV's entitlement
> probe.** `get_workspace_client` silently falls back to the SP when no user token is
> present — exactly the privilege-escalation shape POV §5 warns against.

#### The run-start endpoint

`POST /api/auto-optimize/trigger` — **not** `POST /api/auto-optimize/runs` as POV §7.6
states.

```1466:1497:backend/routers/auto_optimize.py
@router.post("/trigger")
async def trigger(body: TriggerRequest, request: Request):
    """Trigger an optimization run for a Genie Agent."""
    if not _is_configured():
        raise HTTPException(status_code=503, detail="Auto-Optimize is not configured. Set GSO_CATALOG and GSO_JOB_ID.")

    ws = get_workspace_client()
    sp_ws = get_service_principal_client()
    selected_llm_model = (body.llm_model or "").strip() or None
    if selected_llm_model:
        try:
            selected_llm_model = validate_chat_model(selected_llm_model, client=sp_ws)
        except ModelValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    config = _build_gso_config(llm_model_override=selected_llm_model)

    try:
        result = trigger_optimization(
            space_id=body.space_id,
            ws=ws,
            sp_ws=sp_ws,
            config=config,
            user_email=request.headers.get("x-forwarded-email"),
            user_name=request.headers.get("x-forwarded-preferred-username"),
            apply_mode=body.apply_mode,
            levers=body.levers,
            target_accuracy=body.target_accuracy,
            max_attempts=body.max_attempts,
            workload_warehouse_ids=body.workload_warehouse_ids,
            benchmark_policy=body.benchmark_policy,
        )
```

**Request model — 8 fields, defined inline in the router, not in `backend/models.py`:**

```174:190:backend/routers/auto_optimize.py
class TriggerRequest(BaseModel):
    space_id: str = Field(..., pattern=r"^[0-9a-zA-Z_-]{1,128}$")
    apply_mode: str = "genie_config"
    levers: list[int] | None = None
    llm_model: str | None = Field(None, max_length=256)
    # GSO v2 loop knobs (arch §13 / D9). Both optional/nullable; when omitted the
    # job's databricks.yml defaults apply (target_accuracy "0.90", max_attempts
    # "3"). target_accuracy is the 0–1 stop-early target; max_attempts bounds the
    # native patch/eval loop. The loop stops at whichever comes first.
    target_accuracy: float | None = Field(None, ge=0.0, le=1.0)
    max_attempts: int | None = Field(None, ge=1, le=20)
    # Backward-compatible API default: callers predating the gate retain the
    # former repair behavior. The Workbench UI sends review_only explicitly.
    benchmark_policy: Literal["review_only", "repair_allowed"] = "repair_allowed"
    workload_warehouse_ids: list[
        Annotated[str, Field(pattern=r"^[0-9a-zA-Z_-]{1,128}$")]
    ] = Field(default_factory=list, max_length=20)
```

None of the POV's seven `mv_*` fields exist.

**Full route inventory** (23 routes, all under `/api/auto-optimize`): `GET /health`,
`GET /permissions/{space_id}`, `POST /mv/probe` (added by Prompt 5 — the OBO
entitlement probe, `auto_optimize.py:1425`), `POST /trigger`, `GET /runs/{run_id}`,
`GET /runs/{run_id}/status`, `GET /levers`, `POST /runs/{run_id}/apply`,
`POST /runs/{run_id}/discard`, `POST /runs/{run_id}/revert`,
`GET /runs/{run_id}/revert-options`, `GET /spaces/{space_id}/current-version`,
`GET /spaces/{space_id}/active-run`, `GET /spaces/{space_id}/runs`,
`DELETE /runs/{run_id}/history-entry`, `GET /runs/{run_id}/iterations`,
`GET /runs/{run_id}/loop-state`, `GET /runs/{run_id}/publish`,
`GET /runs/{run_id}/debug-data`, `GET /runs/{run_id}/eval-results`,
`GET /runs/{run_id}/question-results`, `GET /runs/{run_id}/patches`,
`GET /runs/{run_id}/benchmark-changes`.

#### Which identity launches the job — the SP

```114:115:packages/genie-space-optimizer/src/genie_space_optimizer/integration/trigger.py
        ws: OBO-authenticated ``WorkspaceClient`` for the requesting user.
        sp_ws: Service-principal ``WorkspaceClient`` for job submission.
```

`trigger_optimization` writes run state with the **OBO** client (`wh_create_run(ws, ...)`,
`trigger.py:265`) and submits with the **SP** (`submit_optimization(sp_ws, ...)`,
`trigger.py:281`).

> **This is the single biggest architectural obstacle in the POV.** POV §7.8 step 4 says
> the MV is created *"under OBO"* inside `metric_view_apply`, and POV §5 says *"the SP is
> never a write path for metric views."* But the job runs as the SP — the bundle
> description says so plainly ("SP executes with granted privileges on user schemas",
> `databricks.yml:58-59`), and the app self-heals `run_as` to the SP at startup
> (`backend/main.py:162-181`). **The user's OBO token does not exist inside the job.**
> Either the DDL executes in the app process under OBO before the job starts, or it
> executes in the job as the SP and the POV's security model is violated. See
> [§3 decision 4](#3-decisions-needed).

#### The user-level UC entitlement probe — built by Prompt 5

`GET /api/auto-optimize/permissions/{space_id}` (`auto_optimize.py:1249-1422`) probes the
**service principal** — Genie `CAN_MANAGE` plus SP schema read via
`probe_sp_required_access(sp_ws, ...)` — and returns grant SQL naming the SP. It answers
"can the SP run this job", not "may this user create an object here." It remains
untouched and is not a fallback for the user-level question.

`backend/services/uc_client.py` still has no permission/privilege/grant surface —
`search_tables`, `list_catalogs`, `list_schemas`, `list_tables`, `get_table_columns` only.
`scripts/grant_permissions.py` is a deploy-time CLI that grants to the SP.

POV §7.3.1's probe was built greenfield in `backend/services/mv_entitlement.py`
(`probe`, `record_consent`, `verify`), exposed as `POST /mv/probe`. It reads
`grants.get_effective(..., principal=<user>)` under `require_obo_workspace_client`
so group-inherited privileges count, adds `CAN MANAGE` on the Genie Agent through
the new `user_can_manage_space` (`common/genie_client.py`), and carries the MV-D8
capability rows derived from `current_version()`. It issues no DDL on any path.

---

### 1.8 The React app

| Property | Value |
|---|---|
| Stack | React 19.2.4, TypeScript 5.9.3, Vite 7.3.1, Tailwind 4.2.1 |
| State management | **None.** Local `useState` + prop drilling. No Redux/Zustand/Context store, no react-query. |
| Test runner | Vitest 4.1.4 (14 test files) |
| Path alias | `@` → `frontend/src/` |

**Visualization libraries already in `frontend/package.json`** — the POV needs no new ones:

| Package | Version | Use today |
|---|---|---|
| `recharts` | 3.8.0 | `IterationChart.tsx`, watch charts |
| `react-force-graph-2d` | 1.29.1 | `watch/pages/ResourceGraphView.tsx` — **a lineage/graph renderer already exists** |
| `react-diff-viewer-continued` | 3.4.0 | `SqlDiffView.tsx` — POV §7.5's "patch shown as a diff" |
| `prism-react-renderer` | 2.4.1 | `SqlCodeBlock.tsx` — POV §7.5's copy-ready DDL panel |
| `sql-formatter` | 15.7.2 | SQL pretty-printing |
| `@databricks/aibi-client` | 1.0.3-alpha.0 | embedded AI/BI |
| `lucide-react`, `class-variance-authority`, `tailwind-merge`, `react-markdown`, `remark-gfm` | | UI primitives |

#### The run-config panel

`frontend/src/components/auto-optimize/OptimizationConfig.tsx` (482 L). Two-column
layout: left = Optimization Scope (six lever checkboxes + benchmark-repair consent
checkbox); right = model picker + stopping criteria; below = query-usage warehouse
selection; then the full-width "Suggest metric views" section (`MvSuggestSection`,
Prompt 11); footer = health issues, `PermissionAlert`, Start button.

State is local `useState` hooks — nine run-config (`:58-66`) plus nine MV-advisor
(`:72-80`, added by Prompt 11). The Start button gate:

```470:470:frontend/src/components/auto-optimize/OptimizationConfig.tsx
              disabled={loading || hasActiveRun || selectedLevers.size === 0 || !canStart || !knobsValid}
```

**There is an existing precedent for a consent checkbox with a warning** — the benchmark
repair toggle at `:141-164`, default off, revealing an amber warning when checked. The
POV's "Suggest metric views" toggle should follow this pattern.

Payload assembly is factored into a pure helper (deliberately, for the
`react-refresh/only-export-components` lint rule):

```42:64:frontend/src/components/auto-optimize/optimizationRequest.ts
export function buildOptimizationTriggerRequest(args: {
  spaceId: string
  applyMode: "genie_config" | "both"
  selectedLevers: Set<number>
  selectedModel: string | null
  targetAccuracy: number
  maxAttempts: number
  workloadWarehouseIds?: string[]
  benchmarkPolicy: "review_only" | "repair_allowed"
  mv?: MvTriggerOptions
}): GSOTriggerRequest {
```

Prompt 11 added the optional `mv` arm (`MvTriggerOptions`): when `mv.enabled` is
true the builder emits `enable_metric_view_suggestions` + the `mv_*` fields;
otherwise it emits none of them, so toggling the section off clears every `mv_*`
field (`mv_materialize` included, though no control sets it — the materialization
path is unbuilt, see the `mv_materialize` row below). The sibling helpers `deriveMvTarget` and
`collectMvSourceTables` compute the probe target and SELECT set from the approved
proposals.

Covered by `OptimizationConfig.test.tsx` (457 L) and `contract.test.ts` (126 L).

#### The run output / results screen

`AutoOptimizeTab.tsx` (684 L) is the container. `RunDetailView.tsx` (188 L) composes the
result surface from ~25 components: `ChampionHero`, `ScoreSummary`, `AttemptLadder`,
`AttemptLedger`, `IterationChart`, `PatchesTable`, `SmartPatchCard`, `QuestionList`,
`QuestionDetail`, `QuestionJourney`, `BenchmarkChangesPanel`, `PublishAuditSummary`,
`ResolutionActions`, `TaskRail`, `StageTimeline`, `ActivityLog`, `PipelineDetailsModal`,
`TerminalBanner`, `ResourceLinks`, `ProactiveEnrichmentView`.

Presentation logic is separated into plain-TS modules with their own tests: `cockpit.ts`
(567 L), `runDetail.ts`, `runHistory.ts`, `resolution.ts`, `pipelineDetail.ts`.

A POV §7.5 "suggest-only" DDL panel would be a new component under `auto-optimize/`,
composed into `RunDetailView.tsx`, reading a new field off the run-detail response.

---

### 1.9 Test layout and CI

| Suite | Framework | Location | Count | Command |
|---|---|---|---|---|
| Backend | pytest 9.0.2 + pytest-asyncio 1.3.0 (`asyncio_mode=auto`) | `backend/tests/` | 23 files | `./scripts/test.sh` — which needs the `dev` extra, see below |
| GSO | pytest 9.0.2 (`pythonpath=["src"]`) | `packages/genie-space-optimizer/tests/unit/` | 83 files | `./scripts/test.sh` also covers this suite; or `uv run --frozen --extra dev pytest packages/genie-space-optimizer/tests` from the root |
| Frontend | Vitest 4.1.4 | `frontend/src/**/*.test.{ts,tsx}` | 14 files | `npm test` |

Root config:

```
[tool.pytest.ini_options]
testpaths = ["backend/tests"]
pythonpath = ["."]
asyncio_mode = "auto"
```

**Fixtures.** `backend/tests/conftest.py` provides `full_space_data`, `empty_space_data`,
and — usefully — `metric_view_only_space`. `packages/genie-space-optimizer/tests/conftest.py`
provides `mock_ws`, `mock_spark`, `patch_llm_client`, `sample_run`, `sample_metadata`
(which includes an empty `"metric_views": []`), `sample_benchmarks`, `sample_eval_results`,
`sample_clusters`.

**Architecture-guard tests** — the class of test the POV must satisfy:

| Test | Guards |
|---|---|
| `test_phase7_job_dag.py` (477 L) | The DABs job shape: four linear tasks, no condition tasks, no task values, parameter defaults (incl. the five MV advisor params, Prompt 8), launcher parameter threading |
| `test_four_notebook_architecture.py` (85 L) | AST-walks the import closure of all four notebooks and fails on any import of 17 retired modules |
| `test_debug_prompt_contract.py` | `docs/debug-prompt.md` SQL stays read-only and current |
| `backend/tests/test_deploy_lib.py` | The installer mirrors the bundle |

**Integration harness.** `packages/genie-space-optimizer/tests/integration/` contains only
`__init__.py` — **there is no live-workspace integration harness.** Per
`packages/genie-space-optimizer/AGENTS.md`: *"Integration testing that requires Genie APIs,
SQL warehouses, model serving, or Databricks auth must run in a deployed workspace."*
Repo-root `tests/` holds manual E2E scripts (`test_e2e_local.py`, `test_e2e_deployed.py`,
`test_full_schema.py`) that are not wired into any runner.

**CI.** The only workflow is `.github/workflows/deploy-docs.yml` (build Docusaurus on
`docs/**` push to `main`, deploy to Pages).

> **Two stale statements in `AGENTS.md`.** It says *"there is currently no `gh-pages`
> branch and no docs CI workflow (the GitHub Actions docs-deploy workflow was removed)"* —
> but `.github/workflows/deploy-docs.yml` exists and targets `github-pages`. Separately,
> **no CI runs any test suite.** All three suites are local-only. Any test the MV advisor
> adds is a test somebody must remember to run.

#### Known pre-existing backend failures: none. The 12 async failures were an invocation defect.

There is no standing residue, so any backend failure during MV advisor work is either newly
introduced or an environment fault — never something to wave through as pre-existing.

**Run the suites with `./scripts/test.sh`.** As of the Prompt 7 review it runs both suites
through `uv run --frozen --extra dev`, and the expected baseline is **571 backend + 1315 GSO
= 1886 passed, 0 failed**.

`--extra dev` is not optional, and this is the resolution of a fault this report carried as
"pre-existing" from Prompt 1 through Prompt 7. Root `pyproject.toml` sets
`asyncio_mode = "auto"`, which requires `pytest-asyncio`; that package lives in the `dev`
optional-dependency group (`pyproject.toml:20`). A bare `uv run --frozen pytest` resolves an
environment without it, so pytest emits `PytestConfigWarning: Unknown config option:
asyncio_mode`, every coroutine test is collected but never awaited, and **exactly 12 tests
fail** — 7 in `test_scanner.py::TestScanSpaceGsoSelection`, 4 in
`test_watch_traffic_gap_router.py`, 1 in `test_auto_optimize_router.py`. Identical count,
identical tests, every time.

That reproducibility is what should have given it away: a genuine pre-existing defect does
not fail exactly the async tests and nothing else. **It was never a code defect.** Adding
`--extra dev` takes the same tree from 12 failed to 0 with no source change. The prescription
this section used to carry — force-reinstalling the editable package and the pinned dev deps
into an ambient pyenv interpreter — treated the symptom in the wrong layer and left the next
contributor to rediscover it. Do not report these 12 as pre-existing; check the invocation.

The other Prompt 1 fault was real and is separate:

| Symptom | Count | Root cause |
|---|---|---|
| `ImportError: cannot import name 'preview_revert_options' from 'genie_space_optimizer.integration'`, at collection, in `test_auto_optimize_router.py` and `test_current_version.py` | 2 collection errors | `_genie_space_optimizer.pth` in the interpreter's site-packages pointed at a **different checkout** of this repo, which lacked the symbol. Every backend test was silently running against foreign source. |

`uv run --frozen` also closes that one, because it resolves the workspace member from this
tree rather than from whatever owns a global `.pth`. The provenance check stays worthwhile
whenever pytest is invoked outside the script:

```bash
uv run --frozen --extra dev python -c \
  "import genie_space_optimizer as g; print(g.__file__)"
```

A run against a foreign checkout is void. Note the shared-interpreter hazard behind that
fault: an editable install in a **global** pyenv `site-packages` means exactly one checkout
can own the `.pth` at a time, so two clones on one machine keep stealing it from each other.
Going through `uv run --frozen` avoids the contest entirely.

#### `uv lock --check` fails structurally. It is not drift.

`uv lock --check` reports *"The lockfile at `uv.lock` needs to be updated"` **on a clean
tree at any commit**, including a freshly-checked-out `main`. The cause is not lockfile rot:
`packages/genie-space-optimizer` has a dynamic version derived from `git describe`
(`0.0.5.post1117.dev0+<sha>`), so the resolved version of the workspace member changes with
every commit and re-resolution always differs from what is recorded. Confirmed by running
the check against a stashed, clean tree at `4ddd210b` — same failure, no modified files.

Consequences, so nobody spends this diagnosis twice:

- **Do not treat a `uv lock --check` failure as evidence of dependency drift.** Verify the
  lockfile with `git status -- uv.lock` instead: an unmodified `uv.lock` is the real signal.
- **Do not run `uv lock` to "fix" it.** That writes a version-only churn commit. Per the
  rules file a dirtied `uv.lock` is a finding to report, never a file to commit.
- **Prompt 14 must not wire `uv lock --check` into CI as a gate.** It would fail every build.
  If lockfile enforcement is wanted there, gate on `git diff --exit-code -- uv.lock` after a
  `uv sync --frozen`, or pin the GSO version statically first — the latter is a real decision
  with a real cost (the wheel version stops tracking the commit it was built from) and is not
  in scope for any prompt in this playbook.

---

## 2. Gap table — every POV Part 7 assumption

Legend: **MATCHES** — exists as described. **CONFLICTS** — the repo has something
incompatible, and adopting the POV means changing or deleting existing behaviour.
**DOES-NOT-EXIST-YET** — greenfield, no conflict.

### 2.1 Task keys and DAG structure (POV §7.2, §7.7)

| POV assumption | Status | Evidence |
|---|---|---|
| `preflight` task | **CONFLICTS** | Split into `intake_and_snapshot` + `benchmark_qc_and_repair`; `test_phase7_job_dag.py:57-67` asserts `preflight` absent |
| `baseline` task | **CONFLICTS** | In-process iteration 0 in `optimize` (`unified_loop.py:2894-2899`); `baseline_eval` asserted absent |
| `enrichment` task | **CONFLICTS** | Absorbed into `optimize` as lever 0; asserted absent |
| `lever_loop` task | **CONFLICTS** | `run_unified_optimization_loop` in-process (`run_optimize.py:337`); asserted absent |
| `finalize` task | **CONFLICTS** | Renamed `publish_and_audit`; asserted absent |
| `deploy` task | **CONFLICTS** | Removed, out of scope (D7); asserted absent |
| `mv_gate` (`condition_task`) | **CONFLICTS** | `test_no_condition_tasks` (`:70-76`) forbids all condition tasks |
| `mv_write_gate` (`condition_task`) | **CONFLICTS** | Same |
| `metric_view_advisor` task | **DOES-NOT-EXIST-YET** | No hit outside the POV |
| `metric_view_apply` task | **DOES-NOT-EXIST-YET** | No hit outside the POV |
| `mv_baseline` task | **DOES-NOT-EXIST-YET** | No hit outside the POV |
| `discover_curator` task (§8.6) | **DOES-NOT-EXIST-YET** | No Discover/domain/governed-tag code anywhere |
| `discover_gate` (`condition_task`) | **CONFLICTS** | Same condition-task prohibition |
| `run_if: ALL_DONE` on MV edges | **CONFLICTS** | No `run_if` anywhere; a strictly linear chain is asserted by `test_dependencies_form_a_linear_chain` (`:79-89`) |
| `max_retries: 1` on advisor | **CONFLICTS (minor)** | Every task is `max_retries: 0` |
| `python_wheel_task` with `entry_point` | **CONFLICTS** | All tasks are `notebook_task`; `test_no_condition_tasks:76` asserts `"notebook_task" in t`; no `[project.scripts]` in the package |
| `package_name: genie_space_optimizer` | **MATCHES** | Import name is `genie_space_optimizer` — but see the row above; there is no wheel task to name it in |

### 2.2 Job parameters (POV §7.7)

As of **Prompt 8**, the five shipped advisor parameters are **registered job parameters**
with the four-place lockstep of [§1.2](#12-the-four-place-parameter-lockstep-requirement)
complete: declared in the root bundle (`databricks.yml:115-124`), the package bundle
(`packages/genie-space-optimizer/databricks.yml:81-90`), `gso_job.JOB_PARAMETERS`
(`scripts/deploy_lib/gso_job.py:111-115`), and the launcher's run_now map
(`backend/job_launcher.py:137-141`, with defaulted keyword-only kwargs at `:85-89`; as of
**Prompt 9** the caller `integration/trigger.py` now threads real values through
`trigger_optimization` — the effective `mv_action_mode`, `mv_attach_views` from the OBO create
hook, `mv_consent_id` (probe id), and `mv_min_confidence`). Each is also passed
to the **optimize task's** `base_parameters` in every mirror (`databricks.yml:194-198`,
`packages/genie-space-optimizer/databricks.yml:156-160`, `gso_job.py:70-71` base_param_keys)
— the pass-through step that actually delivers a job value to the `run_optimize.py` widget;
a parameter declared but omitted there silently resolves to the widget default. `space_id`
remains the only name that pre-existed the feature.

The shipped job set diverges from the POV's seven, per the playbook's Prompt 8 scope:
`mv_attach_views` (the MV-D16 attach input) and `mv_consent_id` (the POV's `mv_consent`,
shipped as a **probe id** because `genie_opt_mv_consents` is keyed on `probe_id`) are in the
job; `mv_target_catalog` / `mv_target_schema` (they live on the consent row) and
`mv_materialize` (a backend/OBO concern, MV-D1) are deliberately **not** job parameters. The
three widget reads that landed ahead of registration (`enable_metric_view_suggestions`,
Prompt 6; `mv_attach_views` / `mv_consent_id`, Prompt 7/MV-D16) plus the two Prompt 8 added
(`mv_action_mode` / `mv_min_confidence`) now all receive real job values via the
base_parameters wiring above; before Prompt 8 every one resolved to its widget default.

| Parameter | Default | Status |
|---|---|---|
| `space_id` | `""` | **MATCHES** — `databricks.yml:77-78` |
| `enable_metric_view_suggestions` | `"false"` | **MATCHES (Prompt 8)** — declared `databricks.yml:115`, pkg `:81`, `gso_job.py:111`, launcher `:137`/`:85`; passed to optimize `databricks.yml:194`; consumed as the advisor gate `run_optimize.py:117-118`, `:461` |
| `mv_attach_views` | `""` | **MATCHES (Prompt 8)** — declared `databricks.yml:119`, pkg `:85`, `gso_job.py:113`, launcher `:139`/`:87`; passed to optimize `:196`; consumed by the MV-D16 attach phase `run_optimize.py:126`, `:383`. Shipped attach input, not in the POV's list |
| `mv_consent_id` | `""` | **MATCHES (Prompt 8)** — declared `databricks.yml:121`, pkg `:87`, `gso_job.py:114`, launcher `:140`/`:88`; passed to optimize `:197`; consumed `run_optimize.py:127`, `:384`. Shipped name for the POV's `mv_consent`; value is a **probe id** |
| `mv_action_mode` | `"suggest_only"` | **MATCHES (Prompt 9 threads it)** — declared `databricks.yml:117`, pkg `:83`, `gso_job.py:112`, launcher `:138`/`:86`; passed to optimize `:195`; read at `run_optimize.py:134`. The **effective** mode is now set by the trigger flow: `trigger_optimization` sends the caller's `mv_action_mode` but downgrades it to `suggest_only` when the create hook attaches nothing (MV-D1) |
| `mv_min_confidence` | `"75"` | **MATCHES (Prompt 9 threads it)** — declared `databricks.yml:123`, pkg `:89`, `gso_job.py:115`, launcher `:141`/`:89`; passed to optimize `:198`; read at `run_optimize.py:135`. The trigger flow now forwards the request's `mv_min_confidence` (or the `"75"` default) instead of always sending the default. Job-side consumption as the advisor confidence cutoff remains a `mv_advisor`/`mv_scoring` follow-on |
| `mv_target_catalog` | `""` | **DOES-NOT-EXIST-YET (out of job scope)** — the target lives on the consent row, not a job parameter (playbook Prompt 8) |
| `mv_target_schema` | `""` | **DOES-NOT-EXIST-YET (out of job scope)** — same; note `schema` already means the GSO state schema, so the `mv_` prefix would be load-bearing if it were ever added |
| `mv_materialize` | `"false"` | **DOES-NOT-EXIST-YET (out of job scope)** — materialization is a separate backend/OBO consent (MV-D1), never a job parameter. **Accepted-but-inert (Prompt 9):** `TriggerRequest.mv_materialize` now exists and `mv_create.create_and_attach_for_run` accepts it, but the create path logs it and installs a **non-materialized** metric view — materialization is a separate DDL path (`CREATE MATERIALIZED VIEW` + its own `materialize_consented` consent, MV-D7). **Resolved (Prompt 11):** the run-config panel surfaces NO materialize control — not live, and deliberately not disabled-with-rationale either (a disabled control for an *unbuilt* feature advertises vapor, unlike first-run "Create and attach" which is disabled because the user can still unlock it). `mv_materialize` stays plumbed through `buildOptimizationTriggerRequest` and is cleared with every other `mv_*` field when the toggle is off (tested); the materialization prompt adds the control and nothing else. **Owner:** the materialization path, a post-Phase-3 prompt — note MV-D1 also requires an `EXPLAIN CREATE MATERIALIZED VIEW` precheck that exists nowhere yet |
| `enable_discover_curation` (§8.6) | — | **DOES-NOT-EXIST-YET** |

String-typed parameters are **MATCHES** as a convention: all 20 declared parameters in the
root bundle are strings (19 in the package bundle, which omits the Workbench-only
`llm_model`), including the numeric ones (`"3"`, `"0.90"`, and the new `"75"`).

### 2.3 Task values (POV §7.7 parameters, §7.7.1 contract)

| POV task value | Status | Evidence |
|---|---|---|
| `{{tasks.enrichment.values.profile_table}}` | **CONFLICTS** | Task values forbidden (`test_phase7_job_dag.py:397-406`); no `enrichment` task |
| `{{tasks.baseline.values.eval_run_id}}` | **CONFLICTS** | Same; `eval_run_id` lives in `genie_opt_iterations.eval_run_id` (`ddl.py:101`) |
| `{{tasks.preflight.values.mv_effective_mode}}` | **CONFLICTS** | Same; no `preflight` task |
| `{{tasks.metric_view_advisor.values.candidate_table}}` | **CONFLICTS** | Same |
| `{{tasks.metric_view_apply.values.created_metric_views}}` | **CONFLICTS** | Same |
| The whole §7.7.1 JSON contract (`candidate_count`, `high_confidence_count`, `requested_mode`, `effective_mode`, `downgrade_reason`, `consent_probe_id`, `baseline_eval_run_id`, `created_metric_views`, `ddl_artifact_path`, `space_patch_ids`, `tables_freed`, `advisor_status`) | **CONFLICTS as a mechanism; viable as a payload** | The *fields* are fine. Persist them as a `genie_opt_artifacts` row with a new `artifact_kind` (e.g. `mv_advisor`) and read them downstream by `run_id`, exactly as `benchmark_qc` does (`test_phase7_job_dag.py:186-211`) |
| "Only numeric, string, and boolean values are usable inside If/else operands" | **MOOT** | No If/else tasks exist; the constraint disappears with the mechanism |

### 2.4 The consent gate and entitlement probe (POV §7.3, §7.3.1, §7.6)

| POV assumption | Status | Evidence |
|---|---|---|
| `POST /api/auto-optimize/runs` starts a run | **CONFLICTS (path)** | The route is `POST /api/auto-optimize/trigger` (`auto_optimize.py:1466`) |
| Request body carries `enable_metric_view_suggestions` etc. | **MATCHES (Prompt 9)** | `TriggerRequest` now has 14 fields (`auto_optimize.py` — the 8 loop fields plus `enable_metric_view_suggestions`, `mv_action_mode`, `mv_min_confidence`, `mv_approved_suggestion_ids`, `mv_consent`, `mv_materialize`), threaded through `trigger()` into `trigger_optimization(..., mv_attach_hook=...)` |
| `jobs.run_now(job_parameters={...})` | **MATCHES** | `job_launcher.py:91-126` |
| Pre-run entitlement probe under OBO | **MATCHES** | `mv_entitlement.probe` reads `grants.get_effective(principal=<user>)` under `require_obo_workspace_client`, exposed as `POST /mv/probe` (`auto_optimize.py:1425`). `/permissions/{space_id}` still probes the **SP** and is unchanged |
| Probe result JSON (`probe_id`, `checked_as`, `verdict`, `missing`, `remediation_sql`, `fallback_mode`) | **MATCHES** | `MvProbeResult` in `backend/models.py`, plus the typed `privileges` / `capabilities` rows MV-D8 requires and POV §7.3.1's sample now shows |
| Consent recorded with the run | **MATCHES** | Recorded in `genie_opt_mv_consents` keyed on `probe_id`, not as a `genie_opt_runs` column — so no `ADDITIVE_COLUMN_MIGRATIONS` entry is needed. Written from the backend by `wh_upsert_mv_consent` (`common/warehouse.py:473`), the Statement-Execution twin of `mv_state.upsert_mv_consent`, because the app has no SparkSession. `run_id` stays NULL until trigger time |
| "Writes execute under OBO" | **CONFLICTS** | The job runs as the SP (`databricks.yml:58-59`; `main.py:162-181` self-heals `run_as` to SP). No OBO token exists inside the job. |
| "The SP is never a write path for metric views" | **CONFLICTS** | Directly contradicted by the above |
| Preflight re-verification / downgrade-never-upgrade | **PARTIALLY MATCHES** | `mv_entitlement.verify(consent, fresh_probe)` implements the comparison and only ever returns `create_and_attach` or `suggest_only` with a `downgrade_reason`. Downgrades on a worse fresh verdict, an identity or target change, a **missing consent row** (reachable because persistence is best-effort), and a **different `observed_warehouse_id`** than the capabilities were read on (MV-D13). **Now called (Prompt 9):** `mv_create.verify_consent` loads the consent, runs a fresh OBO probe against the consented target, and calls `verify` before any create; a downgrade abandons the create and the run proceeds as `suggest_only`. Re-verification belongs in the backend at trigger time, not in a job task — the job has no OBO token |
| UI toggle + target picker + mode radio | **MATCHES (Prompt 11)** | `MvSuggestSection` (wired into `OptimizationConfig.tsx`) renders the "Suggest metric views" toggle, the suggest-only / create-and-attach mode radios, and — on the re-run gate — the approved-proposal checkboxes. The target is READ from each approved proposal's `proposed_object` (`deriveMvTarget`), not chosen in a free-form catalog/schema picker (MV-D23); first-run disables create-and-attach with the MV-D1 rationale, and the OBO probe gates it on the re-run. Follows the benchmark-repair consent idiom at `:141-164` |
| Copyable `GRANT` remediation | **PARTIALLY MATCHES** | Same idiom already exists for warehouse grants — read-only `<textarea>` at `OptimizationConfig.tsx:284-292` and `:300-310`. The `GET /runs/{run_id}/mv-ddl` route now returns a `grant_sql` template alongside the DDL |

*Prompt 9 backend surface (landed).* Four routes on `auto_optimize.py`: `GET /runs/{run_id}/mv-proposals`, `GET /runs/{run_id}/mv-ddl`, `POST /mv/proposals/{id}/decision`, `POST /mv/created/{id}/drop` (OBO, confirm-gated, DETACHED-only, non-owner 403). **Five** `wh_*` helpers in `common/warehouse.py` (not four — the drop route needs `wh_load_mv_created_object` to authorize the read): `wh_load_mv_candidates`, `wh_record_mv_candidate_decision`, `wh_upsert_mv_created_object`, `wh_update_mv_created_object_status`, `wh_load_mv_created_object`, pinned to the `mv_state` column contract by `test_wh_mv_state.py`. The OBO create-and-attach orchestration is `backend/services/mv_create.py`, reached from the engine through `trigger_optimization`'s `mv_attach_hook` (MV-D20/D22). *Prompt 11 adds a fifth route:* `GET /spaces/{space_id}/mv-proposals?approved_for_rerun=` — the space-scoped twin of the run-keyed proposals route (MV-D23), reusing `wh_load_mv_candidates`'s `target_space_id` / `approved_for_rerun` filters and returning the SAME `MvProposal` element type via a sibling `MvSpaceProposalsResponse`. *Prompt 13 adds a sixth route:* `GET /runs/{run_id}/mv-created` — the create-and-attach results read the output panel needs, returning the run's `MvCreatedObject` ledger with each object's `lift_report` (the frozen 14-key `LiftReport` mirrored as `MvLiftReport`, not reshaped) plus the run-level `downgrade_reason` from the consent row. Read-only and SP-tolerant (MV-D20). This grows the run-scoped Prompt-9 family from **four routes to five**. It also lands **two more `wh_*` readers** (now seven): `wh_load_mv_created_objects` (plural, by `run_id`, mirroring the singular loader) and `wh_load_mv_consent_by_run` (the consent table is `probe_id`-keyed but carries `run_id` from trigger time), both pinned by `test_wh_mv_state.py`. **TS mirror (Prompt 11 landed the run-config consumers):** `MvConsentPayload`, `MvProposal`, `MvProposalsResponse`, `MvSpaceProposalsResponse`, and `MvProbeRequest` are mirrored in `frontend/src/types/index.ts` and consumed by `MvSuggestSection`. **Prompt 13 landed the output-screen consumers:** `MvDdlArtifact`, `MvLiftReport`, `MvCreatedObject`, `MvCreatedObjectsResponse`, `MvProposalDecisionRequest`/`Response`, and `MvDropRequest`/`Response` are now mirrored too, consumed by `MvSuggestOnlyPanel` / `MvCreateAttachPanel` (via `MvRunOutputSection`) on the run-detail screen.

### 2.5 The patch and apply path (POV §7.8)

| POV assumption | Status | Evidence |
|---|---|---|
| Patch = `field_path` + `new_value` | **CONFLICTS** | Patches are dicts of `type`/`target`/`new_text`/`old_text` (`applier.py:2649-2659`); `field_path` appears nowhere in `src/` |
| `field_path: "data_sources.metric_views"`, `operation: "append"` | **CONFLICTS** | No path-addressed patches; `op` ∈ `add`/`update`/`remove`/`update_section`/`rewrite` on the *rendered command* |
| Attach MV by patching `data_sources.metric_views[]` | **MATCHES** | `mv_attach_data_source` (`config.py:2104-2115`) with a real `metric_views` branch in `_apply_action_to_config` (`applier.py:3921-3946`); the Lever-2 `uc_artifact` MV sections remain no-ops (`:3975-3993`) and are unrelated |
| Companion patch removing covered raw tables | **PARTIALLY MATCHES** | `remove_table` exists (`applier.py:3914-3919`) and is not in `_ALLOWED_PATCH_TYPES`; MV-D16 reuses it rather than adding an `mv_remove_raw_table` twin, so the raw-table half is available to a caller and not to the LLM |
| `CREATE VIEW … WITH METRICS LANGUAGE YAML` | **MATCHES (Prompt 9)** | `mv_yaml.create_ddl` builds the statement and `mv_yaml.generate` the YAML body it wraps; `mv_create.create_and_attach_for_run` now executes it under OBO via `sql_warehouse_execute(obo_ws, ...)` after replaying the persisted `yaml_text` and revalidating (MV-D22). The job still runs as the SP and never issues it |
| `EXPLAIN CREATE MATERIALIZED VIEW` precheck | **DOES-NOT-EXIST-YET** | — |
| "Inherits existing versioning, diff, rollback" | **MATCHES** | `genie_opt_patches` carries the attach (`mv_attach.py`, keyed `run_id:0:2:idx`); rollback is still whole-snapshot (`applier.py:4617-4627`), which MV-D16 makes sufficient by placing the attach where nothing else is in its snapshot |
| `on_regression: DETACH_ONLY_NEVER_DROP` | **MATCHES** | `mv_attach._detach` reverts through `applier.rollback` and writes status `DETACHED` with the lift report; no code path drops the UC object |
| Idempotency key `sha256(space_id \| expr \| sources)` | **PARTIALLY MATCHES (precedent)** | `genie_opt_artifacts.content_hash` exists for exactly this (`ddl.py:184`); the launcher already builds a SHA-256 idempotency token (`job_launcher.py:33-37`) |
| `tables_freed` / 30-table cap | **DOES-NOT-EXIST-YET** | No table-count ceiling logic |
| Free text in MV proposals must clear the leakage firewall | **CONFLICTS with current coverage** | `_PATCH_TEXT_FIELDS` covers only example-SQL types (`leakage.py:286-289`); package AGENTS.md requires new text-carrying patch types to be routed through it. `mv_attach_data_source` needs no entry — it carries an identifier and no free text — so the obligation still belongs to whatever ships MV `comment`/`synonyms` edits. Prompt 6c's curated harvest also needs no `_PATCH_TEXT_FIELDS` entry, but for a different reason: it carries no free text into evidence at all — every curated statement is canonicalized through `mv_fingerprint` (which erases literals) before it reaches a bucket's `canonical_expr`, so a quoted literal a human wrote into a snippet or example cannot survive into persisted evidence (pinned by `test_no_curated_corpus_statement_leaks_a_quoted_literal`) |

### 2.6 Evaluation (POV §7.8 step 7, Key Finding 2, Recommendation 2)

| POV assumption | Status | Evidence |
|---|---|---|
| Create eval run for benchmarks | **MATCHES** | `eval_runner.py:583-585` |
| Get eval run / poll | **MATCHES** | `eval_runner.py:638` |
| List evaluation results | **MATCHES** | `eval_runner.py:662-667` |
| Get result details | **MATCHES** | `eval_runner.py:669-671` |
| List eval runs in space | **MATCHES** | `OfficialBenchmarkRunner.list_eval_runs` (`eval_runner.py:539-551`) |
| "Wrap the Beta endpoints behind a thin adapter" | **MATCHES — already done** | `EvalRunner` Protocol + `OfficialBenchmarkRunner`, single seam (`eval_runner.py:305-330`, `:468-518`); `run_subset` / `lift_report` / `list_eval_runs` are additive on that seam |
| Statuses `RUNNING`/`DONE`/`NOT_STARTED`/`EVALUATION_FAILED`/`EVALUATION_CANCELLED`/`EVALUATION_TIMEOUT` | **MATCHES** | `eval_runner.py:33-34`, `:59-63` |
| Reasons `EMPTY_RESULT`/`RESULT_MISSING_ROWS`/`RESULT_EXTRA_ROWS`/`RESULT_MISSING_COLUMNS` | **MATCHES** | `genie_eval_taxonomy.py:8-20` |
| `num_questions`/`num_correct`/`num_needs_review`/`num_done` | **PARTIALLY MATCHES** | `num_needs_review` is a persisted column (`ddl.py:100`); accuracy is `num_correct/num_questions`; `num_done` is not persisted |
| Store `eval_run_id` not a copied score | **MATCHES** | `genie_opt_iterations.eval_run_id` (`ddl.py:101`) — though `overall_accuracy` is stored too |
| `mv_baseline` as a separate eval run isolating MV lift | **MATCHES, without the extra full eval** | MV-D16: iteration-0 *is* the pre-attach baseline, and the lift run is a `run_subset` over the affected question ids (`mv_attach.py`, label `mv_lift`) — so the isolation costs a subset, not a suite |
| "roughly 15 Delta tables plus Lakebase" | **CONFLICTS** | Ten tables in `_ALL_DDL` (+2 outside it): MV-D7 added three stateful MV stores and Prompt 15.3 added `genie_opt_mv_suppressions` |
| "MLflow… still the home for run provenance and versioning" | **CONFLICTS** | Decommissioned in Phase 5 (D3/D7). `models.py:1-16`: *"no MLflow LoggedModel snapshot, no UC Model Registry version, no per-mutation MLflow run."* GSO uses MLflow for **tracing only**. |

### 2.7 Scoring, signals, and outputs (POV §2, §3, §7.5)

| POV assumption | Status | Evidence |
|---|---|---|
| Blended `100*(0.35L+0.30Y+0.20S+0.15D)` score with HIGH/MEDIUM/LOW tiers | **EXISTS (Prompt 4)** | `optimization/mv_scoring.py` — component scorers, blend, tiers, dedup gate, POV Part 4 payload, persistence through `mv_state.upsert_mv_candidate`. Weights and every normalization constant in `common/config.py:2423-2506` (MV-D11), never inlined |
| sqlglot AST fingerprinting | **EXISTS (Prompt 3)** | `optimization/mv_fingerprint.py` — canonicalization, measure/dimension/filter/join-key extraction, shape classification, corpus scan; `sqlglot==30.0.3` pinned. Prior art: `benchmarking.py`, `unified_loop.py`, `wide_schema*.py` (**not** `applier.py`, which only names sqlglot in a comment at `:1534`) |
| Syntactic signal (**Y**) | **EXISTS (Prompt 4), curated occurrence-credit added (Prompt 6c / MV-D17)** | `mv_scoring.syntactic_score` consumes a `RecurrenceSignal`. Its equivalence flag is **corpus-internal** (MV-D11) — governed-MV equivalence is the dedup gate's, not this signal's. Prompt 6c credits each curated source as `k` generated occurrences *inside* the log: `Y = normalized_recurrence(r + k * curated_provenance_count)`, `k = MV_CURATED_OCCURRENCE_EQUIVALENT` (default 20), neutral by construction at zero (`k * 0 == 0` returns the identical float) so generated-only candidates and the pinned POV examples are unchanged. Whether one curated source outranks a heavily-recurring generated one is `k`'s authored value — at `k = 20` a single curated source (`r = 1 → 0.714`) beats a lightly-recurring generated measure but not sixty derivations (`0.949`); it is **not** an unconditional property of the function. `curated_provenance_count` is a new field on `FingerprintRecurrence` and `RecurrenceSignal`, counted in `_Bucket` from `mv_fingerprint.CURATED_PROVENANCE_KIND` — new surface, not a refresh. `provenance_count` remains populated but unread by scoring; damping Y by distinct-source **breadth** is a separate, deferred MV-D17 fix |
| `system.query.history` demand signal (**D**) | **MATCHES (Prompt 6a/6b)** | `optimization/mv_signals.py:371-474` is the advisor producer. `demand_signal` requires existing `candidate_fingerprints`, reads space-scoped traffic via `query_source.genie_space_id`, reuses `corpus_scan`, and matches only those candidate fingerprints before computing recurrence frequency, distinct users, duration cost, and age/recency (`:379-459`). No candidates returns `EMPTY` before the history read (`:394-396`); read failures remain `UNAVAILABLE`, while no history or no matching measure is `EMPTY` (`:404-443`). `mv_scoring.demand_score` normalizes the resulting `DemandSignal` (geometric mean, then 30-day half-life decay, per MV-D11). |
| Lineage signal (**L**) | **MATCHES (Prompt 6a/6b)** | `optimization/mv_signals.py:249-322` produces column-grain overlap. For new/add-measure candidates it reads `system.access.column_lineage`, scoped by `entity_metadata.genie_space_id` and the candidate source tables (`:344-365`); governed-MV replacement uses caller-supplied source columns. `COMPUTED` / `EMPTY` / `UNAVAILABLE` remains explicit rather than silently substituting zero. |
| Embedding / semantic signal (**S**) | **PARTIALLY MATCHES** | Two paths, no Vector Search index. Firewall: `leakage.py:46-57` (endpoint/threshold), `:139-144` (vectors, `question_embeddings = None` so disabled by default), `_cosine_similarity` at `:174`, `get_embedding` at `:189`, `precompute_benchmark_embeddings` at `:220`. Advisor: `mv_scoring.FoundationModelEmbeddingClient`, injectable and defaulting to `databricks-gte-large-en`, which borrows `get_embedding` and no other firewall symbol. *(Correction: this row previously anchored the firewall path at `leakage.py:132-137`, which is the `BenchmarkCorpus` shingle block. That anchor was **wrong when written**, not rotted by a commit — `leakage.py` is untouched on this branch. A quote can be stale by position with its content unchanged, which no keyword search can find; that is the case for byte-matching every fenced reference in VERIFY rather than grepping for stale wording.)* |
| Existing MV detection for dedup | **MATCHES** | `common/metric_view_catalog.py:63-73`, `:481-490`; flattened for the gate by `mv_scoring.metric_view_fields`, which keeps the `DESCRIBE ... AS JSON` parsing in `metric_view_catalog` |
| Harvest already-optimized space artifacts | **MATCHES (consumed, Prompt 6c)** | `mv_advisor.curated_corpus_entries` feeds three curated sources through `corpus_scan`'s existing extractors, tagged `CURATED_PROVENANCE_KIND`: trusted-asset SQL via `mv_scoring.example_question_sql_statements` (the single reader `trusted_asset_definitions` also uses), `instructions.sql_snippets.{measures,filters,expressions}` (substituted for the bodyless `sql_functions` — MV-D17), and this run's `genie_opt_patches.patch_json` (SQL-bearing types only). Governed `data_sources.metric_views` are **not** a corpus channel — `_advise` excludes governed measures from the seed set post-scan against the estate index (MV-D17 / blocker 4). `join_specs` is skipped (bare predicate fragments, no synthetic wrapping; deferred as `mv_yaml` join topology) |
| Copy-ready DDL panel with copy button | **MATCHES (Prompt 13)** | `MvProposalCard.tsx:98-99` renders the DDL and (when present) GRANT via `SqlCodeBlock` (`prism-react-renderer` under the hood), fed by the `MvDdlArtifact` read; graduated into the suggest-only output panel |
| Patch shown as a diff | **MATCHES (Prompt 13)** | `MvSpaceConfigDiff.tsx:30-44` — `react-diff-viewer-continued` over the current `data_sources.metric_views[]` identifiers vs. the same list with `proposed_object` appended. The proposed side is **synthesized client-side** (the Prompt 12 precedent — no patch endpoint), fixing the mockup's silent §7.5 omission |
| `ddl_artifact_path` on a Volume | **CONFLICTS** | No Volumes convention for run artifacts; use `genie_opt_artifacts` |
| `candidate_table: main.genie_workbench.mv_candidates` | **CONFLICTS (location)** | GSO tables live in `{GSO_CATALOG}.{GSO_SCHEMA}` as `genie_opt_*`, not `genie_workbench` |
| Lineage/graph visualization | **MATCHES (Watch only — not Prompt 12's renderer)** | `react-force-graph-2d` already used in `watch/pages/ResourceGraphView.tsx`, and that is where it stays. The semantic-model view is a deterministic layered SVG per the amended Prompt 12 body (playbook) — do not read this row as an instruction to use force-graph there |
| `GET /api/auto-optimize/spaces/{space_id}/semantic-graph` | **LANDED + CURRENTLY EXTENDED** — route `backend/routers/auto_optimize.py:3370-3440`, assembler `:2890-3305`, rendered through `SemanticBlueprint` by `frontend/src/components/model/SemanticModelTab.tsx:3-24,496-569` | Space-scoped nodes/edges are assembled from the live, OBO-tolerant, normalized `serialized_space` plus space-scoped proposals. Governed chips and MV structure come from one batched `DESCRIBE ... AS JSON` read; attached metric views round-tripped under `tables` are first normalized by `backend/services/genie_client.py:57-122,258-282`. The current single Blueprint adds metric-view semantic-model structure, truthful roles, per-measure source lineage, proposal overlays, semantic zoom, and Join Advisor advice without making an ad-hoc Agent-config edit. |

### 2.8 Discover curation (POV §8)

| POV assumption | Status |
|---|---|
| `discover_curator` task | **DOES-NOT-EXIST-YET** |
| Governed-tag application (`ALTER … SET TAGS`) | **DOES-NOT-EXIST-YET** — zero `governed_tag` / `MANAGE DISCOVERY` hits |
| `system.information_schema.table_tags` reads | **DOES-NOT-EXIST-YET** in GSO (`common/uc_metadata.py` has tag helpers but not for domains) |
| Domain/subdomain/Page proposals | **DOES-NOT-EXIST-YET** |
| PII / literal firewall on drafts | **PARTIALLY MATCHES** | `optimization/leakage.py` is a benchmark-leak firewall, not a PII scanner; the plumbing pattern transfers, the detector does not |

> The domain/tag curator half of POV Part 8 is deferred (Prompt 20). The
> near-term track is the **Ontology Pages** track (Prompts 17.x), which is
> **Page-only** (MV-D27): it drafts Discover Pages and writes NO Agent
> instructions and NO live space config. Its build map is the reuse inventory
> below.

---

## 2.9 Ontology surfaces — reuse inventory (Prompt 17.0)

This is the 17.x build map: what already exists to reuse, so the sub-prompts
extend one read/gen/persist path instead of forking a parallel one. Anchors
verified against HEAD; `path:line` is the current location + a signature quote.
**RE-SCOPED by MV-D36 + MV-D37:** the Ontology surface is now a standalone,
admin-gated, estate-wide page (not a per-Agent tab) proposing a Domain →
Sub-Domain → Page taxonomy on a governed-tag substrate. The evidence/gen/persist
anchors below still apply (per-space assembly becomes estate-wide); the new
estate-wide + governed-tag surfaces are grouped separately, and the frontend
target moves from a SpaceDetail tab to a top-level `App.tsx` view. Output stays
propose-only/copy-ready by default with ONE optional consented `SET TAG` write.

**Evidence assembly + deterministic detectors (17a).**

- Semantic graph (assembly + governance ladder + coverage): the read path the
  detectors reuse, not a new one.
  - `backend/routers/auto_optimize.py:3370` — `@router.get("/spaces/{space_id}/semantic-graph", response_model=MvSemanticGraph)` → `:3371` `async def get_space_semantic_graph(space_id: SpaceId)`
  - `backend/routers/auto_optimize.py:2890` — `def _build_semantic_graph(` (the assembler)
  - `backend/routers/auto_optimize.py:3126` — `def _measure_node(` — governance rungs at `:3167` `governed` / `:3209` `curated` / `:3234` `ungoverned`
  - `backend/routers/auto_optimize.py:3315` — `def _apply_coverage(`
- Fingerprint shapes + CONFLICT states (`[Disambiguation]` from CONFLICT
  candidates + same-concept-different-expr; `[Routing]`/`[Guardrail]` from
  measure shapes):
  - `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/mv_fingerprint.py:194` — `class ShapeMatch:` · `:273` `class FingerprintRecurrence:` · `:1074` `def shapes_in_statement(sql: str)`
  - `.../optimization/mv_scoring.py:118` — `VERDICT_CONFLICT = "CONFLICT"` · `:1028` `def dedup_gate(` · `:1093` `def _conflicting_definitions(` · `:1124` `def _defines_same_quantity(` · `:401` `class MetricViewCandidate:` · `:854` `class DedupOutcome:`

**LLM synthesis + firewall + read-only contradiction gate (17b).**

- `backend/services/llm_utils.py:24` — `def call_serving_endpoint(` · `:179` `def parse_json_from_llm_response(content: str) -> dict:`
- `backend/services/model_catalog.py:90` — `def validate_chat_model(` (guards the per-run model override)
- Firewall (extend for Page bodies — benchmark text never lands verbatim):
  `.../optimization/leakage.py:682` — `class LeakageOracle:` (methods `:719` `contains_sql` · `:734` `contains_question` · `:756` `is_scored_benchmark_qa` · `:794` `evaluate_example_sql`)
- Contradiction gate — **READ-only** against current instructions (Page-only:
  no write): `.../optimization/optimizer_utils.py:296` — `def _detect_instruction_contradictions(` over the block read at `.../optimization/applier.py:211` — `def _get_general_instructions(config: dict) -> str:` (reads `ti[0]["content"]` at `:217`)

**Persistence (17a, MV-D26 — the `genie_opt_*` precedent to extend).**

- Table constants `packages/.../common/config.py:1859` `TABLE_MV_CANDIDATES` · `:1869` `TABLE_MV_CONSENTS` · `:1877` `TABLE_MV_CREATED_OBJECTS` · `:1883` `TABLE_MV_SUPPRESSIONS`
- DDL `.../optimization/ddl.py:200 / :238 / :262 / :286` — `CREATE TABLE IF NOT EXISTS … genie_opt_mv_*`
- Warehouse `wh_*` twins (the backend's read path — corrected line numbers):
  `.../common/warehouse.py:689` — `def wh_upsert_mv_consent(` · `:815` — `def wh_load_mv_consent(`
- In-job accessors `.../optimization/mv_state.py:193` `upsert_mv_candidate` · `:325` `load_mv_candidates` (Spark-based, run in-job — the backend goes through the `wh_*` twins, per MV-D21)

**Estate-wide enumeration + governed-tag substrate (MV-D36 + MV-D37 — the new
read surfaces the standalone page adds).**

- GenieWatch SP + system-table reader (the usage/lineage/cost ranking signals):
  `backend/watch/services/system_tables.py` — reads via `get_service_principal_client()` (`backend/services/auth.py`), the SP-only pattern; TTL-cached. Reuse this reader, don't fork it.
- Grant preflight source of truth: `scripts/grant_permissions.py` — the SP grant list (USE CATALOG system + SELECT on `system.access.audit` / `.table_lineage` / `system.billing.usage` / `system.query.history`); the permission banner reads its status against this list, and MV-D37 adds a `SELECT system.tags.governed_tags` row + the optional-write entitlements (`MANAGE DISCOVERY`, `ASSIGN`, `APPLY TAG`).
- Workspace Agent enumeration: `backend/services/genie_client.py` — `list_spaces` (workspace-scoped; the estate's Agents).
- Account metric-view enumeration: `system.information_schema.tables WHERE table_type='METRIC_VIEW'` (OBO, auto-filtered, no explicit grant; `view_definition ILIKE '%WITH METRICS%'` fallback) + `DESCRIBE TABLE EXTENDED … AS JSON` on the shortlist for measures/synonyms.
- Governed-tag graph (MV-D37): `system.tags.governed_tags` + `SHOW GOVERNED TAGS` (allowed values, SP); assignments `system.information_schema.{catalog,schema,table,column}_tags` (OBO), joined on `tag_name = tag_key` for `is_governed`. The `manage_uc_tags` MCP + `SET/UNSET TAG` / `CREATE GOVERNED TAG` DDL back the optional consented write.
- Semantic dedupe anchor (MV-D40): **Lakebase Search** (`lakebase_vector` ANN + `lakebase_text` BM25) on the app's existing Autoscaling Lakebase (`backend/services/lakebase.py`) for embedding + keyword similarity over tag keys / measures / Pages in the reuse-vs-create pass; embeddings from the existing `databricks-gte-large-en` FMAPI endpoint (`mv_scoring` / `leakage.get_embedding`). No separate managed Vector Search service; degrades to in-process cosine when Lakebase Search is not enabled. Introduced in Phase 3 (enabling Lakebase Search is beta + irreversible).
- Settings/company: `backend/routers/analysis.py` `/api/settings` — the persisted company-name field feeding LLM naming.

**External enrichment / Context Pack (MV-D38 — the opt-in T1–T3 prior).**

- Web/industry/competitor/filings/regulatory (MV-D46/D47): an **opt-in registry of Unity AI Gateway MCP context sources** synthesizes the Context Pack via `llm_utils.call_serving_endpoint`. Primary web search is the managed **`system.ai.web_search`** MCP (an `EXECUTE`-granted UC securable; the Gateway proxies managed creds, built-in write-block policy, `ai_gateway.usage`/`audit` tracked), with a **fallback ladder** of the **You.com** MCP / a Model-Serving-native web tool / estate-only. Company docs come from **Confluence / Google Drive / Microsoft 365** MCPs (T1); the MS-Learn / Databricks-docs MCP supplies the T2 industry-model template; internal **Genie / Databricks SQL** MCPs (T0) add structural signal + validation. Each registry entry carries `{class, provenance_tier, influence}` so external/overlay sources reach naming/description only, never structure (MV-D38). Capability probe = `EXECUTE` on the service (HIPAA/BAA hard-off). It is the opt-in 5th permission tier — a **Context Sources panel**, default OFF — the toggle is disabled with a plain reason and the engine degrades to estate-only naming when no source is available.
- Enrichment contract to reuse (do NOT fork): the Page "Recent context" disclaimer + Sources + `certify-no` rule (MV-D28) and the `leakage.LeakageOracle` firewall (extend to reject unsourced numbers + PII in tag names).
- Persistence: `genie_ont_context_pack` (versioned, cached per company; pinned by the run that consumed it) — same Delta + Lakebase-mirror pattern as the other `genie_ont_*` tables.
- Structural firewall: external context is passed ONLY to the naming/description/synonym/gap functions — it must never reach the membership (`SET TAG`) or measure-definition writers. Full mechanics: `ontology-engine-architecture.md` §6.

**Frontend wiring (17c — a TOP-LEVEL page, not a SpaceDetail tab).**

- Target: a new top-level view in `frontend/src/App.tsx` beside SpaceList / AdminDashboard / CreateSpace / HowItWorks, admin-gated (reuse the AdminDashboard gating + GenieWatch lazy-load precedent).
- The SpaceDetail tab strip (`frontend/src/lib/navigation.ts:2`, `frontend/src/pages/SpaceDetail.tsx:177-181`) is now only a wiring PRECEDENT — Ontology no longer adds a `SpaceTab`.

**Deliberately NOT reused (MV-D27 + MV-D37) — inventoried so no sub-prompt wires
them by reflex.**

- OBO space-update / instruction write path — the track makes NO instruction or
  space-config write: `backend/services/create_agent_tools.py:3408` — `def _update_space(...)` (PATCH `/api/2.0/genie/spaces/{space_id}` `:3432-3435`); `require_obo_workspace_client` (`backend/services/auth.py:117`) is reused ONLY for the optional `SET TAG` membership write (MV-D37), never for instructions.
- `backend/genie_creator.py:65` — `def _enforce_constraints(config: dict) -> dict:` — bounds a write we do not perform; relevant only as the reason the GSL schema is READ context for the contradiction gate.
- **NOTE (MV-D37):** the domain/sub-domain **membership** write (`SET TAG`) is IN scope as an OPTIONAL, consented, dry-run-first apply (reusing the MV consent rails); the Discover **card** and **Pages** remain no-API/copy-ready. Page-dedupe is best-effort — Pages have no read API.

---

## 3. Decisions needed

Ordered by how much downstream work each unblocks. **1–4 are blocking.**

**1. Amend the POV, or amend the repo?** The POV's §7.2 DAG and §7.7 bundle are written
against an architecture `main` deleted, and three tests enforce the deletion. Options:
(a) rewrite POV §7.2/§7.7 against the four-task DAG (recommended); (b) reintroduce
condition tasks and task values, deleting `test_no_condition_tasks`,
`test_deploy_and_legacy_tasks_removed`, and `test_no_dbutils_notebook_run_or_task_values_in_new_notebooks`,
and reversing decision D9. **Until this is decided, no implementation can start.**

**2. `.cursor/rules/metric-view-advisor.mdc` must be amended too.** It currently mandates
three things the repo forbids: string-comparing If/else condition tasks, a `lever_loop`
task, and `run_if: ALL_DONE` on metric-view edges. As written, an agent following the rule
would break the build. The rule needs the same treatment as decision 1, in the same commit.

> Compounding this: the rule file lives at the **workspace** root
> (`genie-workbench-latest/.cursor/rules/`), one directory *above* the git repo, and
> `git ls-files` matches zero `.cursor` paths — **it is not version-controlled with the
> code it governs.** So the rule cannot be corrected in the same commit as the code, and
> reviewers will not see it change. Decide whether to move it to
> `databricks-genie-workbench/.cursor/rules/` and commit it.

**3. Where does the advisor run?** Given no condition tasks, the realistic options are:
(a) **a fifth linear task** `metric_view_advisor` between `optimize` and
`publish_and_audit`, self-skipping on a parameter check inside the notebook — preserves
the linear-DAG invariant, costs one task; (b) **in-process inside `optimize`**, gated by a
parameter — zero DAG change, but couples advisor failure to the optimization run unless
carefully guarded; (c) **a separate job** triggered independently — cleanest isolation,
abandons the POV's "single toggle, one tool" premise. Recommendation: **(a)**, with the
gate as an early `if` in the notebook.

> **Recorded OPEN as `MV-D23` (decided at playbook Prompt 13.5).** All three options
> above presume the advisor runs *inside* an optimization run. Whether it must — whether
> a candidate can be proposed and created for a space that has never been optimized — is
> now a recorded open decision rather than an unexamined assumption. Four `run_id`
> couplings built through Prompt 9 make today's answer "only inside a run": the `_advise`
> corpus gate, `genie_opt_mv_candidates.run_id` `NOT NULL`, the `mv_candidate_ddl` body's
> `run_id`-partitioned `genie_opt_artifacts` home (the sole thing `MV-D22` can replay),
> and the created-objects `(run_id, suggestion_id)` key. Three cannot be relaxed
> additively — `ADDITIVE_COLUMN_MIGRATIONS` only appends columns — so the decision
> carries a schema cost a later prompt must not discover mid-flight. The four anchors and
> the three options are in the playbook `MV-D23` entry.

**4. Which identity performs the UC write?** This is the sharpest conflict. The job runs
as the SP; the POV requires OBO and explicitly forbids SP writes. Options: (a) **drop
`create_and_attach` from v1** and ship `suggest_only` only — no write, no conflict, and
the POV itself says suggest-only is what most first runs will use; (b) execute the DDL
**in the app process under OBO** before `run_now`, then pass the created identifier as a
job parameter — preserves the security model, but moves a UC write into a request handler
and breaks the "optimize on top of the new foundation" sequencing; (c) accept **SP writes**
and revise POV §5. Recommendation: **(a) for v1**, revisit with (b).

**5. Task-value contract → artifact contract.** Confirm the §7.7.1 payload is persisted as
a `genie_opt_artifacts` row with a new `artifact_kind` (proposed: `mv_advisor`) rather than
a task value, and that downstream reads use `load_latest_artifact_record` with a
missing-record gate, matching the `benchmark_qc` precedent.

**6. One JSON parameter or seven scalars?** Given the four-place lockstep, recommend a
single `mv_config` JSON string parsed and validated in the notebook. Decide before the
first parameter is added — retrofitting is worse than starting right.

**7. Candidate storage location.** `genie_opt_mv_candidates` as a seventh table in
`_ALL_DDL`, or an `artifact_kind` row in `genie_opt_artifacts`? The POV's
`main.genie_workbench.mv_candidates` is wrong either way — GSO tables live in
`{GSO_CATALOG}.{GSO_SCHEMA}` under the `genie_opt_` prefix.

> **Resolved by `MV-D7`** (playbook decisions register): both. Three tables join `_ALL_DDL`
> for the stateful entities — `genie_opt_mv_candidates`, `genie_opt_mv_consents`,
> `genie_opt_mv_created_objects` — while the rendered DDL *text* stays a
> `genie_opt_artifacts` row, cross-referenced by setting its `content_hash` to the
> candidate's dedup fingerprint.

**8. Patch-type naming and firewall registration.** If MV attachment becomes a patch, it
needs a `PATCH_TYPES` key, an `_apply_action_to_config` branch for the `metric_views`
section, an entry in `_ALLOWED_PATCH_TYPES`, and — because it carries `comment`/`synonyms`
free text — an entry in `leakage._PATCH_TEXT_FIELDS`.

> **Resolved by `MV-D2` and `MV-D16`, shipped in Prompt 7.** The key is
> **`mv_attach_data_source`** (`config.py:2098-2115`), classified `HIGH_RISK`, with a real
> `metric_views` branch in `_apply_action_to_config`. Two corrections to the proposal above.
> There is **no `detach_metric_view` twin**: detach is a whole-snapshot revert through
> `applier.rollback` (see item 9), and removal of a raw table reuses the existing
> `remove_table`. And the type is deliberately **NOT** in `_ALLOWED_PATCH_TYPES` — that
> frozenset is the LLM-proposal surface rather than the lever surface, so listing attach
> there would let the loop's LLM invent a UC identifier and attach it with no consent row;
> the attach phase calls `apply_patch_set` directly. No `_PATCH_TEXT_FIELDS` entry is needed
> either: the attach patch carries an identifier and no free text.

**9. Rollback semantics.** POV `DETACH_ONLY_NEVER_DROP` presumes per-patch revert. Today
rollback restores a whole config snapshot. Decide whether to build a targeted detach path
or accept that a rollback also reverts unrelated same-iteration patches.

> **Resolved by `MV-D16`:** whole-snapshot revert, and the question of unrelated patches does
> not arise — the attach phase runs between iteration-0 and the first lever patch, so its
> `pre_snapshot` contains no other change to lose. `integration/revert.py` is not the
> primitive: it is a backend surface whose active-run guard rejects mid-run by design.

**10. Where does `mv_baseline` fit?** As a second in-process `_native_eval` inside
`optimize`, it costs one extra full eval run per run against the ~20 questions/min
workspace ceiling. Confirm the isolated-lift measurement is worth that, or make it a
separate opt-in.

> **Resolved by `MV-D16`: there is no second full eval.** Iteration-0's own baseline is the
> pre-attach measurement — it already runs, and MV-D16 keeps the attach after it precisely so
> that it stays pre-attach. The lift eval is a `run_subset` over the **affected question ids**
> recorded on the proposal, so the added cost is that subset rather than a suite, and it is
> only paid on a run that was given `mv_attach_views`.

**11. Correct the POV's factual claims.** Ten Delta tables not ~15 (MV-D7 added
three `genie_opt_mv_*` tables and Prompt 15.3 added the suppression ledger); MLflow is tracing-only, not the provenance home;
the route is `POST /api/auto-optimize/trigger`; no Volumes artifact path; the
eval-run status field is `eval_run_status` not `status`.

**12. Testing and CI.** No CI runs any suite, and there is no live-workspace integration
harness. Decide whether the MV advisor ships with a CI workflow, and how the OBO
entitlement probe and the DDL path get tested at all given they cannot run offline.

> **Partially resolved (Prompt 14, 2026-08-24).** `.github/workflows/test.yml` adds the
> first non-docs workflow — the backend suite, the GSO suite (`pythonpath=["src"]`), and
> frontend `vitest` on pull requests, all through `./scripts/test.sh` semantics
> (`uv run --frozen --extra dev`). It does **not** wire `uv lock --check` (structural
> failure on this repo's dynamic GSO version). The OBO probe and the `CREATE … WITH
> METRICS` DDL path stay offline-untestable — the open half of this item. Detail in §3A.

---

## 3A. Prompt 14 — test-hardening audit (2026-08-24)

Self-dated addendum. Audits the coverage this branch added (Prompts 1–13.5 plus 12b),
closes gaps, and records the write-to-read exposure sweep. The sweep's matrix lives at
`docs/design/mv-advisor-exposure-matrix.md`, pinned by `test_exposure_matrix.py`.

**Exposure sweep — first run, clean on the two historical near-misses.** 49 MV columns
classified: `genie_opt_mv_candidates` (23), `genie_opt_mv_consents` (12),
`genie_opt_mv_created_objects` (13), plus `genie_opt_runs.run_kind` (MV-D23). Scope is
**MV columns wherever they live**, not only the three `genie_opt_mv_*` tables the body
names, so the sentinel discriminator on `genie_opt_runs` cannot drift unclassified. The
two reads a prior "wire to existing endpoints" instruction assumed but the writing side
never exposed both classify **SERVED** on the matrix's first run — the space-scoped
proposals read (Prompt 11, `GET spaces/{space_id}/mv-proposals`) and the
created-objects/lift read (Prompt 13 step 0, `GET runs/{run_id}/mv-created`) — so the
matrix reopened neither.

**GAP (1) — `genie_opt_mv_created_objects.provenance` — RESOLVED (Prompt 14.1, 2026-08-24).**
The MV-D24 create-path discriminator. Written by route 5 (`register`, value `USER_CREATED`);
it gates route 9 (`drop` refuses `USER_CREATED`) and the attach-phase identity relaxation
server-side. Raised at Prompt 14 because route 10 (`mv-created`) did not return it, so a
reloaded UI could not tell `USER_CREATED` from `OBO_CREATED` to hide the Drop affordance the
mockups (frame 8b) omit for bring-your-own views. Closed at Prompt 14.1, additively: the
field is now on `MvCreatedObject` (`backend/models.py`) and its TS mirror
(`frontend/src/types/index.ts`), mapped by `_mv_created_object_from_row`
(`backend/routers/auto_optimize.py`) with NULL → `OBO_CREATED`, and `MvCreateAttachPanel`
hides Drop plus shows the `USER_CREATED` badge on that provenance. The server-side drop
guard is untouched — this is truthfulness, not enforcement. The exposure-matrix row moves
GAP → SERVED (route 10) in the same commit.

**Finding — the exclusion-predicate pin is comment-satisfiable (narrow, not broken).**
`test_mv_advice_run_exclusion_pin.py::test_every_run_listing_site_routes_through_the_pinned_predicate`
asserts `"MV_ADVICE_RUN_EXCLUSION" in inspect.getsource(fn)`. `inspect.getsource` includes
comments, so an **in-place** regression that inlines the raw predicate while leaving the
explanatory comment (`# … pinned MV_ADVICE_RUN_EXCLUSION predicate …`) in
`load_gso_runs_for_space` does not trip it — verified by mutation. The pin still fires for
the threat it was written against, a **brand-new** listing site, which carries no such
comment (removing the comment token as well makes the assertion fail as expected). Logged,
not tightened in Prompt 14; a stricter form would strip comments before the membership
check or assert the predicate is not inlined.

**Audit-the-audit (mutate → observe red → revert).** Each branch-added guard was confirmed
to fail when it should, not merely to pass:

| Guard | Mutation | Result |
|---|---|---|
| `test_rules_parity` | drift one baseline copy | red (byte-diff) |
| `gap_report_counts.py --check` (`test_gap_report_counts.py`) | wrong line-count claim | red |
| MV-D21 column pin (`test_wh_mv_state.py`) | drop `yaml_text` from the writer's `value_cols` | red (written set 19 → 18) |
| Debug-prompt contract (`test_debug_prompt_contract.py`) | inject retired column `best_repeatability` into a `sql` block | red |
| Exclusion pin (`test_mv_advice_run_exclusion_pin.py`) | inline predicate, drop the comment token | red (see finding above for the comment-satisfiable caveat) |

**Coverage summary.** Backend 636, GSO 1452 (measured 2026-08-24, `./scripts/test.sh`).
`+8` GSO this prompt: the exposure-matrix pin (`test_exposure_matrix.py`) and the
advice-run dry-run harness (`test_mv_dry_run_harness.py`). The harness extends the in-job
Delta-by-`run_id` handoff with the 13.5 standalone path end to end — `suggest` → sentinel
advice run (born terminal, excluded by `MV_ADVICE_RUN_EXCLUSION`) → candidate row carrying
`yaml_text` → `register` (`USER_CREATED`) → attach-phase acceptance. Baseline lockstep is
updated in both copies (`.cursor/rules/mv-advisor.mdc` and the playbook fenced block);
`test_rules_parity` holds.

**Intentionally untested, with reason (MV-D9).**
- Live OBO entitlement probe and the UC `CREATE … WITH METRICS` DDL path — no OBO, no
  warehouse offline; exercised only by deploy-time E2E, per the repo's no-local-server
  rule. This is item 12's remaining open half.
- Frontend "stories for all states" — there is no Storybook on this branch (Prompt 10
  finding). The established substitute is the mockup emitter's static HTML render, which
  `mockups.test.tsx` already asserts frame by frame; component tests cover the live panels.
- The `inspect.getsource` comment-inclusion edge on the exclusion pin (above) —
  characterized, deliberately not converted to a stricter assertion in this prompt.

---

## 4. Decision record — D1 through D6 (and D7–D9)

The GSO v2 rewrite is governed by numbered decisions cited throughout the code as
`(D1)`, `(D3/D7)`, `(D8)`, `(D9)`. **The playbook that defines them,
`GSO_OPTIMIZER_V2_TODO.md`, is referenced by the code but is not checked into this
repository** (`eval_runner.py:3`, `common/config.py:119`). The reconstructions below are
derived **entirely from in-repo citations**, and are marked accordingly.

> **Two conflicting D1–D3 namespaces exist.** Besides the v2 playbook's, `applier.py:358`
> opens a block labelled *"D1–D3: GSO quality-instruction policies (baseline-eval-fix
> plan)"* — a different plan with its own D1–D3 (`mv_preference`, `column_ordering`, and a
> third policy in `_GSO_QUALITY_V1_POLICIES`). When citing a decision, always name the
> plan. This report means the **v2 playbook** unless stated otherwise.

### D1 — The native Genie Benchmark Eval API is the sole eval runner

**Reconstruction: high confidence.** Stated near-verbatim in code.

```16:19:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/eval_runner.py
Decision **D1**: the official API is the SOLE eval runner in v2 — scoring is the
server-side ``assessment`` (GOOD/BAD/NEEDS_REVIEW), accuracy is
``num_correct / num_questions``, and lever routing reads ``assessment_reasons``.
We never double-run the retired in-process scorer path.
```

Corollaries: **fail-closed** — *"a non-DONE / partial / empty run NEVER reads as a"*
success (`eval_runner.py:810`); the knobs at `common/config.py:116-133` are the D1
implementation surface.

**Impact on the MV advisor.** POV Recommendation 2 is already satisfied. Any MV lift
measurement must go through `EvalRunner`, must treat non-`DONE` as invalid rather than
zero, and must not introduce a second scorer.

### D2 — The nine scored LLM judges are retired

**Reconstruction: high confidence.**

```90:98:packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py
# Phase 3 (D2): the 9 scored LLM judges are RETIRED. The official Databricks
# Genie Benchmark API verdict is the sole quality signal, so acceptance gating is
# on API accuracy alone — there are NO per-judge thresholds. Accuracy is carried
# under the legacy ``result_correctness`` key (on the official path this equals
# ``num_correct / num_questions``); ``all_thresholds_met`` reads it. The unified
# loop compares full-corpus accuracy before accepting a candidate.
DEFAULT_THRESHOLDS = {
    "result_correctness": 85.0,
}
```

Corroborated at `benchmarking.py:1790` (*"the 9 scored judges are retired, so gating is on
API accuracy"*) and `eval_runner.py:367`, `:463` (asset-type annotation on BAD /
NEEDS_REVIEW rows, "Phase 3, D2").

**Impact.** The POV's Caveat that *"public write-ups describing a bank of automated MLflow
judges reflect an earlier architecture"* is correct and this is the decision that did it.
An MV proposal cannot be scored by a bespoke judge — only by the platform verdict.

### D3 — Tracking and versioning are Delta-only; MLflow is decommissioned

**Reconstruction: high confidence.**

```1:9:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/models.py
"""Champion selection for Genie Agent optimization runs (Delta-only).

GSO v2 (Phase 5, D3/D7): tracking and versioning are Delta-only. There is no
MLflow LoggedModel snapshot, no UC Model Registry version, and no per-mutation
MLflow run. The champion iteration is selected from ``genie_opt_iterations``
(highest accepted ``overall_accuracy``) and marked in Delta via
``mark_champion_iteration``. Rollback stays Delta-based — see
``applier.rollback`` (in-memory ``pre_snapshot`` re-PATCH) and
``integration.discard`` (``genie_opt_runs.config_snapshot`` re-PATCH).
```

Artefacts: `genie_opt_iterations.config_json` and `.is_champion` (`ddl.py:97`, `:99`); the
whitelisted config projection (`state.py:607`, `:627`); the scrubbed `experiment_name`
column (`trigger.py:201`, `warehouse.py:296`); retired templates at `config.py:1884`.

**Impact.** **Directly contradicts POV §1c**, which lists MLflow as *"Still the home for
run provenance and versioning."* It is not. Per
`packages/genie-space-optimizer/AGENTS.md`: *"GSO uses MLflow only for tracing."* MV
provenance must go to Delta.

### D4 — *Not reconstructable*

**Zero citations anywhere in the repository.** A whole-repo search for `\bD4\b` (excluding
`.git` and lockfiles) returns nothing, and the defining playbook is not checked in.

Circumstantial inference only: the phases cited alongside decisions run Phase 1 (D1) →
Phase 3 (D2) → Phase 4/5 (D3) → … → Phase 7/9, so D4 plausibly covers **Phase 2**, whose
in-repo footprint is the example-SQL leakage guard — but Phase 2 work is cited as **D8**,
not D4 (`test_scored_benchmark_qa_exclusion.py:1`: *"GSO Optimizer v2 — Phase 2 example-SQL
leakage guard (§3.6 / D8)"*). So the phase-to-decision mapping is not one-to-one and this
inference should not be relied on. **Do not build against an assumed D4.**

### D5 — *Not reconstructable*

**Zero citations anywhere in the repository.** Same search, same result.

### D6 — *Not reconstructable*

**Zero citations anywhere in the repository.** Same search, same result.

> **Action required.** D4, D5, and D6 cannot be honoured because nobody in this repo can
> read them. Either check `GSO_OPTIMIZER_V2_TODO.md` into `docs/design/`, or promote the
> full D1–D9 list into `packages/genie-space-optimizer/AGENTS.md` where the code can cite
> a durable location. Until then, any instruction to "follow D1–D6" is only half
> actionable.

### D7 — Cross-environment deploy is out of scope

**Reconstruction: high confidence.** Cited at `scripts/deploy_lib/gso_job.py:27`, `:85`;
`databricks.yml:73`; `models.py:3`; `config.py:1884`;
`test_phase7_job_dag.py:114`; `backend/tests/test_auto_optimize_router.py:1136`.

Consequences: the `deploy` task is gone, the `deploy_target` parameter is gone, and there
is no UC Model Registry path. `models.py:14-16` notes a future intent to use *"the official
DAB `genie_space` resource."*

**Impact.** The POV's DAG ends `finalize → deploy`. There is no `deploy`. Also relevant to
POV §8: publishing Discover artifacts would be a new deploy-shaped concern in a codebase
that deliberately removed one.

### D8 — Benchmark leakage firewall and the 30–40 question window

**Reconstruction: high confidence.** Two linked rules:

*Leakage.* Benchmark questions and their SQL must never reach the live space's Example SQL
section. Deterministic hard-block on **every** write path:

```291:296:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py
# Every patch/proposal type that persists a question+SQL pair into the live
# space's Example SQL Queries section. The deterministic scored-benchmark
# hard-block must cover all of them on every write path (v2 §3.6 / D8).
_EXAMPLE_SQL_PATCH_TYPES: frozenset[str] = frozenset(
    {"add_example_sql", "update_example_sql"}
)
```

Also at `leakage.py:454`, `:756`, `:927`, `:971`; `applier.py:4045`, `:4063`;
`publish.py:32`, `:601`; `test_scored_benchmark_qa_exclusion.py:1`. Note *"no train/held-out
split per D8"* — the whole scored set is protected.

*Window.* A 30–40 question working set, recommended (not enforced) over the post-merge
live set: `config.py:317`, `:322`; `preflight.py:2656`, `:2668`, `:2872`;
`genie_client.py:1425`; surfaced in the UI at
`frontend/src/components/auto-optimize/BenchmarkChangesPanel.tsx:32`.

**Impact.** POV §8.4's Page-drafting `examples` field draws on *benchmark questions*. D8
plus the POV's own Caveat (*"a benchmark that leaks into instructions stops being a test"*)
means Page bodies and MV comments must be routed through the leakage firewall, and
`_PATCH_TEXT_FIELDS` must be extended for any new text-carrying patch type.

### D9 — Linear DAG, no condition tasks, no task values

**Reconstruction: high confidence.** The most consequential decision for this feature.

```110:113:databricks.yml
      # Every task carries its job-parameter subset as base_parameters so there
      # is NO inter-task task-value plumbing (D9). run_id/catalog/schema
      # bootstrap each notebook; all other handoff state is read from Delta by
      # run_id. This mirrors the package bundle; `llm_model` is added to every
```

```20:21:packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_intake_and_snapshot.py
# MAGIC State handoff is via Delta only (D9): every task reads job parameters +
# MAGIC durable state by `run_id`. There is no `dbutils.jobs.taskValues` plumbing.
```

Three rules, each enforced by a passing test:

| Rule | Test |
|---|---|
| No condition tasks; every task is a `notebook_task` | `test_phase7_job_dag.py:70-76` |
| No `dbutils.notebook.run`, no `taskValues.set` / `.get` | `test_phase7_job_dag.py:397-406` |
| Strictly linear dependency chain | `test_phase7_job_dag.py:79-89` |

Plus the round-trip contract at `auto_optimize.py:132`, `:179` ("arch §13 / D9") and the
installer mirror at `gso_job.py:30`.

**Impact.** D9 is what invalidates POV §7.2 and §7.7 wholesale — `mv_gate`,
`mv_write_gate`, `discover_gate`, `run_if: ALL_DONE`, and the entire §7.7.1 task-value
contract. It is also what `.cursor/rules/metric-view-advisor.mdc` contradicts. Any MV
design must express gating as **in-notebook conditionals on job parameters** and handoff as
**`genie_opt_artifacts` rows keyed by `run_id`**.

---

## Appendix — quick reference

**Corrections to apply to the POV**

| POV says | Reality |
|---|---|
| `preflight → baseline → enrichment → lever_loop → finalize → deploy` | `intake_and_snapshot → benchmark_qc_and_repair → optimize → publish_and_audit` |
| `python_wheel_task` + `entry_point` | `notebook_task` + `notebook_path` |
| `{{tasks.X.values.Y}}` | `genie_opt_artifacts` row read by `run_id` |
| `condition_task` gates | In-notebook `if` on a job parameter |
| Patch = `field_path` + `new_value` | Patch = `type` + `target` + `new_text` |
| "roughly 15 Delta tables" | Ten in `_ALL_DDL` (twelve counting `genie_opt_scan_snapshots` and `genie_benchmarks_{domain}`); the fourth advisor table is the Prompt 15.3 suppression ledger |
| MLflow = provenance and versioning home | Delta-only (D3); MLflow is tracing-only |
| `POST /api/auto-optimize/runs` | `POST /api/auto-optimize/trigger` |
| `/Volumes/.../runs/<id>/metric_views.sql` | No Volumes convention; use `genie_opt_artifacts` |
| `main.genie_workbench.mv_candidates` | `{GSO_CATALOG}.{GSO_SCHEMA}.genie_opt_*` |
| Eval-run field `status` | `eval_run_status` |
| Writes execute under OBO inside the job | The job runs as the SP |

**Things the POV needs that already exist**

`sqlglot` (pinned, in use) · Eval-API adapter (`eval_runner.py`) · `eval_run_id`
persistence · MV detection and YAML extraction (`metric_view_catalog.py`) ·
`system.query.history` mining with fallback (`wide_schema_history.py`) · artifact-based
handoff with content hashing (`genie_opt_artifacts`) · consent-checkbox-with-warning UI
pattern (`OptimizationConfig.tsx:141-164`) · copyable grant-SQL textarea pattern
(`OptimizationConfig.tsx:284-292`) · diff viewer, SQL code block, force-directed graph
(Watch resource view only — the semantic-model view is layered SVG per the amended
Prompt 12 body), charts (all in `frontend/package.json`) · OBO-only client accessor
(`require_obo_workspace_client`).

---

## Live E2E findings (MV-D9)

Findings from running the Prompt 15 live suite against a real workspace. These
are recorded here (not fixed inline) so the run and the repo state stay in sync.

### 2026-08-24 — Tier 1 (Scenario D), suggest run has no `mv-ddl` artifact — RESOLVED (Prompt 15.1, 2026-08-24)

**Observed.** `POST /api/auto-optimize/spaces/{space_id}/mv/suggest` returns a
born-terminal sentinel advice run and persists proposals (each `MvProposal`
carries `proposed_object`, the rendered body), but it does **not** write a
run-scoped `mv_candidate_ddl` artifact. `GET /api/auto-optimize/runs/{run_id}/mv-ddl`
reads only that artifact kind, so it returns `404 {"detail":"No metric view DDL
artifact for this run."}` for a suggest-only run. The full optimization/create
path is the only writer of that artifact.

**Impact.** The E2E BYO leg (`test_scenario_d_byo_register_refuse_and_provenance`)
sources its manually-created view's DDL from the `mv-ddl` endpoint, so its
precondition is incompatible with the suggest path and the leg's real assertions
(register → `USER_CREATED`, drop refused 409, route-10 provenance) were not
reached. The other two Scenario-D legs (curated → `COMPLETE` with proposals
carrying `evidence`; bare → `SKIPPED` with a `skip_reason`, no 500) passed.

**Resolution options (for review, not decided here).** Either (a) point the BYO
setup at `proposal.proposed_object` (already returned by suggest) or drive the
BYO leg from a full run that emits the artifact, or (b) have the suggest route
persist a `mv_candidate_ddl` artifact for its sentinel run so the run-scoped
endpoint is uniform across suggest and optimize. Verbatim failure and full
config are in `scripts/e2e/mv_advisor_e2e.md` → Run record → 2026-08-24 Tier 1.

**Resolution (Prompt 15.1, 2026-08-24) — option (b), read-side.** `get_mv_ddl`
(`auto_optimize.py`) now falls back to the candidate rows when no
`mv_candidate_ddl` artifact exists: a new `_load_candidate_ddl_fallback` reads
`wh_load_mv_candidates(run_id=…)`, picks the row named by an optional
`suggestion_id` query param (else the best-confidence row — `wh_load_mv_candidates`
orders confidence DESC), and renders the same `MvDdlArtifact` shape via
`mv_yaml.create_ddl` off the row's `yaml_text` + `evidence.join_strategy`. So an
advice run serves copy-ready DDL instead of 404ing, and the IQ Scan advisory
panel fetches DDL per proposal (`getMvDdl(run_id, suggestion_id)`,
`MvIqScanAdvisorySection.tsx`). Two orderings on one route — artifact latest-wins,
fallback best-wins — and `validation: None` on the fallback (a preview; the
artifact carries the real validation) are documented in the route docstring so
the difference is not read as a bug. The exposure-matrix `yaml_text` row was
corrected (`DELIBERATELY INTERNAL` → `SERVED | route 7`) — the old rationale was
false twice over (route 7 served the raw column for artifact-backed runs even
before this, and advice runs 404'd). Pinned by
`test_get_mv_ddl_falls_back_to_candidate_yaml_text` (backend 637 → 638) and the
IQ Scan DDL-panel frontend test. The Scenario D rerun VERIFIED the 404 is gone —
the BYO leg now clears `assert ddl_resp.status_code == 200` and receives
`yaml_text` — but then surfaced a distinct, deeper defect one step later (at the
CREATE), recorded as its own finding below. So this gap is resolved; the rerun is
**2/3** (curated-COMPLETE and empty-SKIPPED still pass), not three-for-three, for
a reason orthogonal to this fix.

### 2026-08-24 — Tier 1 rerun (Scenario D), rendered measure expr masks numeric literals — RESOLVED (Prompt 15.2, MV-D29, 2026-08-24)

**Observed.** With the 404 resolved, the BYO leg now fetches the advisor's
rendered DDL (HTTP 200) and executes `CREATE VIEW … WITH METRICS LANGUAGE YAML`
against the warehouse. The CREATE fails:

```
[INVALID_IDENTIFIER] The unquoted identifier n-l_discount is invalid and must be
back quoted as: `n-l_discount`. … == SQL of METRIC VIEW …
[measures.`measure_l_discount_l_extendedprice`.expr] …
```

The stored `yaml_text` (read directly from `genie_opt_mv_candidates`) contains:

```
measures:
  - name: measure_l_discount_l_extendedprice
    expr: sum(l_extendedprice * (?n - l_discount))
```

The curated measure was the canonical TPC-H revenue measure
`SUM(l_extendedprice * (1 - l_discount))`. The numeric literal `1` was masked to
a placeholder token `?n` somewhere in the candidate/measure synthesis (a
literal-normalization or parameterization step — the same class of transform
used for fingerprint dedup / leakage-guarding), and that masked form is what got
persisted as the measure `expr`. When the metric-view engine parses the body,
`?n - l_discount` is read as an expression referencing an invalid identifier
`n-l_discount`, so the DDL is rejected.

**Impact.** This is more consequential than a test blocker. Prompt 15.1's whole
promise is *copy-ready* DDL, and for any measure containing a numeric literal
(discounted revenue, tax-inclusive totals, percentage-of, etc. — a large and
common class) the served DDL is **not executable as-is**. The read route is
faithful — it serves exactly what was rendered — so the defect is upstream in
the measure-expr rendering, not in the Prompt 15.1 fallback. Scenario A's
"structurally-valid DDL artifact" assertion and any create-and-attach on such a
measure are exposed to the same masking.

**Not fixed here (a finding per the ground rules).** The literal-masking is
outside the Prompt 15.1 / Scenario-B scope and belongs in the measure-synthesis
/ mv_yaml expr path, with its own review — the parameterized form must not leak
into the persisted `expr`; the human-readable literal (`1`) must survive to the
rendered body. Recorded so the "copy-ready DDL" claim is qualified until it is
fixed. Root-cause pointer: search the candidate/measure expr construction for
where a normalized/parameterized expression (`?`-placeholders) is used in place
of the literal source expression. Verbatim failure and the dumped `yaml_text`
are in `scripts/e2e/mv_advisor_e2e.md` → Run record → 2026-08-24 Tier 1 rerun.

**Diagnosis correction (do not treat as a rendering leak).** The `?n` is not
masking that "leaked" — it is `mv_fingerprint`'s *deliberate, firewall-motivated*
literal erasure, whose module docstring (`:24-42`) states the contract: "a
literal that reaches shipped metric-view metadata is a firewall violation", and
"a generator must recover concrete predicate values from profiling, never by
reading them back out of a canonical form — there is nothing left there to
read." The defect is a **consumer violating that contract**:
`candidate_from_measure` set `measure_expr=measure.canonical_expr` (the erased
identity form), and `FingerprintRecurrence` carried *no* concrete expression, so
the generator had nothing else to reach for. A design gap, not a leak.
Weakening erasure would break the firewall property and reshape what merges (an
MV-D10 change the register forbids re-deriving).

**Resolution (Prompt 15.2, decides MV-D29).**
1. `MeasureRef` / `FingerprintRecurrence` gain `representative_expr`
   (`mv_fingerprint.render_expr`): the same normalization as `canonicalize_expr`
   **minus `_erase_literals`**, captured from the first occurrence of a
   fingerprint. Identity/scoring/dedup keep reading `canonical_expr` only —
   pinned by `test_representative_expr_is_never_an_identity` (two measures
   differing only in a literal share one `fingerprint` while their
   `representative_expr` differs). `canonicalize_expr` output is byte-identical.
2. `candidate_from_measure` renders `measure_expr=representative_expr`, so the
   POV's `SUM(l_extendedprice * (1 - l_discount))` reaches the body as
   `SUM(source.l_extendedprice * (1 - source.l_discount))` — executable.
3. The firewall becomes an actual gate, not erasure-by-construction: before a
   candidate is scored/persisted, `LeakageOracle.contains_sql(representative)`
   runs and a match DROPS the candidate (`candidates_dropped_for_leakage`). The
   existing oracle, no second scanner.
4. `mv_yaml.validate` / `validate_registered` reject `?n` / `?s` in any emitted
   `expr` — the cheap MV-D8 static guard that would have caught this at Prompt
   5.5. This is why the class of defect can no longer pass offline.
5. Fixtures: the POV golden case renders to executable DDL
   (`test_the_rendered_body_carries_the_literal_not_a_placeholder`), the gate
   drops a leaking representative
   (`test_a_leaking_representative_drops_the_candidate`), and `mv_yaml` rejects
   placeholder-bearing bodies. +9 GSO; no persisted column added, so the
   exposure matrix is unchanged (the render source flows in-memory into the
   already-SERVED `yaml_text`). Exit: Scenario D BYO leg's `CREATE VIEW`
   executes a literal-bearing measure — recorded in the runbook's rerun entry.

### 2026-08-25 — Tier 2 (Scenario A), in-job `suggest_only` run has no servable `mv-ddl` — RESOLVED (Prompt 15.5, MV-D30 as-implemented, 2026-08-25)

**Observed.** First clean Tier 2 A/B run against the **redeployed** Prompt 15.3
tree (`e65cc5a6`; the earlier attempt was VOID on a stale-import tree).
`test_scenario_a_suggest_only` reaches `SUCCESS` and persists ≥1 candidate, then:

```
    ddl_resp = api_primary.get(f"/api/auto-optimize/runs/{run_id}/mv-ddl")
>       assert ddl_resp.status_code == 200, ddl_resp.text
E       AssertionError: {"detail":"No metric view DDL artifact for this run."}
E       assert 404 == 200

tests/e2e/test_mv_advisor_e2e.py:227: AssertionError
```

**Impact.** This is the **in-job twin of the Prompt 15.1 finding** (above),
reborn at the view grain. 15.1 resolved the same 404 for the *standalone suggest*
path by falling back to the candidate's `yaml_text`; Scenario D stays green
because `mv_suggest._persist` writes `yaml_text` unconditionally on `rendered.ok`.
Scenario A drives the **in-job** engine instead, where both `write_ddl_artifact`
and the candidate `yaml_text` are gated on `rendered.ok` (`mv_advisor.py:1346-1348`,
the bundle persist/artifact pair). So a candidate persists but neither the
run-scoped `mv_candidate_ddl` artifact nor the 15.1 fallback resolves, and the
run-scoped endpoint 404s. The IQ-Scan / run-output "copy-ready DDL" contract is
broken for any in-job `suggest_only` run whose proposal is a bundle.

**Prime suspect (verify in the fix prompt, do not assume).** MV-D30's view-grained
**bundle** body either does not render `ok` on the in-job path, or its rendered
body is not propagated into the in-job persist — so `rendered.ok` is False and the
gated writes are skipped. The offline unit suite is green because it covers the
standalone `_persist` writer, not the in-job bundle render, which is exactly the
gap a live Tier catches. Concrete next step: dump the Scenario-A candidate row's
`yaml_text` and the bundle `rendered.ok` on the in-job path; decide whether the
fix is to make the bundle render `ok` (renderer), to propagate the bundle body to
the in-job persist (engine wiring), or to extend the read-side fallback to a
bundle row that legitimately carries no single-object DDL. A **real 15.3
regression signal**, not a fixture issue. Verbatim failure and full config:
`scripts/e2e/mv_advisor_e2e.md` → Run record → 2026-08-25 Tier 2.

**Resolution (Prompt 15.5).** Root cause named and reproduced offline before any
fix: the recurring-shape expander (`mv_yaml._shape_measures`) rendered ratio /
conditional-count / pct-of-total components from `ShapeMatch.components`, which
are the canonical, literal-erased forms — so a `*_rate_numerator`-style measure
emitted a `?n`/`?s` canonicalizer placeholder into the metric-view body and
MV-D29 validation correctly rejected the whole view (`rendered.ok=False`). The
in-job persist then wrote a bundle candidate whose only servable body is the
`rendered.ok`-gated artifact, so `/mv-ddl` 404'd. Two-part fix, both in this
commit: (1) **root repair** — `ShapeMatch` carries a `render_components` twin
(built via `render_expr`, literal-preserving) that the expander renders from,
keeping `components` as the identity form; because a render form can now carry a
literal, the shapes are gated through the same `LeakageOracle` the primary
measure clears, at the assembly site in `advise_from_corpus`. (2) **structural
guard (net, not repair), persist-variant (a)** — a PROPOSE bundle whose body does
not render is DROPPED, not persisted, and the drop rides the run outcome loudly
(`AdvisorOutcome.candidates_render_failed` + `render_failures`, in the stage
`detail_json`, ids/codes only). The guard lives in the shared
`advise_from_corpus` body, so both persistence writers (in-job artifact, backend
`yaml_text`) inherit it. Recorded as an MV-D30 "as implemented (Prompt 15.5)"
addendum; the Suggest-Surface Contract carries the servable-body clause in both
rule copies. Offline pins: `test_a_recurring_ratio_shape_renders_a_servable_body`
(the regression), `test_a_bundle_that_fails_to_render_never_persists_and_rides_the_outcome`
and the two writer tests via `assert_every_surfaced_proposal_is_servable` (the
§4b invariant), `test_a_shape_whose_render_form_matches_the_benchmark_is_dropped`
(the shape firewall). Live re-confirmation is the Tier 2 rerun gating Tier 3.

### 2026-08-25 — Tier 2 (Scenario B), auto-downgraded run records no `downgrade_reason` — RESOLVED (Prompt 15.5, 2026-08-25)

**Observed.** Same run. The probe half is green
(`test_scenario_b_probe_insufficient` — the denied read-only schema yields a
non-SUFFICIENT verdict). The run half creates no MV (correct) but records no
reason:

```
        created = api_primary.get(f"/api/auto-optimize/runs/{run_id}/mv-created").json()
        assert created["created"] == [], "a downgraded run must create NO metric view"
>       assert created["downgrade_reason"], "downgrade left no downgrade_reason on the run"
E       AssertionError: downgrade left no downgrade_reason on the run
E       assert None

tests/e2e/test_mv_advisor_e2e.py:332: AssertionError
```

**Impact.** The downgrade transparency contract (POV §7.7.1 `downgrade_reason`;
UI smoke item 10) is unmet: a create_and_attach run that re-verified to a
non-SUFFICIENT verdict downgraded to `suggest_only` and created nothing, but the
surface that should explain *why* returns `None`.

**Triage undetermined — first live Tier-2 observation of B's run-half** (the prior
Tier 2 was VOID). Two candidate loci to tell apart: (a) the downgrade path wrote
no `downgrade_reason` — `mv_create.verify_consent` / `mv_entitlement.verify` (the
gap-report row above notes `verify` "only ever returns … with a
`downgrade_reason`", so a genuine downgrade should carry one); or (b) it was
written to the run/created row but the `/runs/{run_id}/mv-created` response
assembly does not surface it. Because MV-D30 left the create/downgrade path
untouched except the proposal grain, this is **more likely pre-existing or
environmental than a 15.3 regression** — but that is a hypothesis for the
reviewer's triage, not a conclusion. Not fixed here. Verbatim and config:
`scripts/e2e/mv_advisor_e2e.md` → Run record → 2026-08-25 Tier 2.

**Resolution (Prompt 15.5) — hypothesis verified, not assumed.** Queried the live
`genie_opt_mv_consents` rows for the downgraded run: `run_id` **NULL** and
`downgrade_reason` **NULL**. So locus (a) was correct and (b) was not —
`mv_entitlement.verify` does compute a `downgrade_reason`, but nothing persisted
it: `create_and_attach_for_run` returned an `MvAttachHandoff` carrying the
reason, `trigger.py` read only `attach_views`/`consent_id`/`action_mode` and
discarded it, and the one function that would stamp the consent
(`mv_state.mark_mv_consent_reverified`) was dead code — no warehouse twin and no
production caller. Since `/mv-created` reads the reason via
`wh_load_mv_consent_by_run` (keyed on `run_id`, itself NULL), the surface had
nothing to find. Fix: added the warehouse twin `wh_mark_mv_consent_reverified`
(UPDATE-only MERGE, MV-D21 parity with the Spark accessor) and call it from
`create_and_attach_for_run` as the SP on both the downgrade path (stamping
`run_id` + re-verified `verdict` + `downgrade_reason`) and the success path
(`run_id` + `verdict`, no reason), plus the create-time-failure downgrade. The
consent→run loop is now closed, so route 10 surfaces the reason. Offline pins:
`test_reverify_closes_the_consent_to_run_loop` + the UPDATE-only/omit-unset guards
in `test_wh_mv_consent.py`, and `test_downgrade_stamps_the_consent_with_run_and_reason`
/ `test_success_stamps_the_consent_run_without_a_downgrade_reason` in
`test_mv_create.py`. Live re-confirmation is the Tier 2 rerun's run-half.

### 2026-08-25 — Third look (finding: acceptance CUJ), the IQ card offers no way to accept a suggestion — RESOLVED (Prompt 15.8, MV-D34, 2026-08-25)

**Observed.** Third human smoke run. On the IQ-Scan surface — where the user
first meets a suggestion — the proposal card exposed only `[Review in run
setup]` and `[I created this myself]` (`MvIqScanAdvisorySection.tsx:500-505`).
No action on the panel called the decision endpoint that has existed since
Prompt 9, so run setup forever found zero approved proposals and "Create and
attach" could never enable from this path.

**Impact.** The primary CUJ (see a suggestion → accept it → get a metric view)
was structurally dead on the surface the user actually visits. The two-run
consent model (MV-D1) was designed for proposals discovered by a run the user
is *not* watching; on the IQ surface the user is present, the OBO token is
live, and every piece of the create machinery already runs interactively for
BYO registration — so the missing path was a UI gap, not a safety one.

**Resolution (Prompt 15.8, MV-D34).** A single shared accept flow
(`MvAcceptFlow.tsx`) now leads every proposal card on BOTH surfaces (IQ +
run-output) with `[Create this metric view]`. It reuses the existing seams with
zero forks: `POST /spaces/{space_id}/mv/create` (`auto_optimize.py`) →
`mv_create.create_at_approval`, which reuses `mv_entitlement.probe`,
`record_consent`/`verify_consent`, the `mv_create` DDL replay + `_confirm_metric_view`,
`wh_create_advice_run` (sentinel run id) and `wh_upsert_mv_created_object`
(`OBO_CREATED`) — the same BYO-register rails MV-D24 attach-on-next-run already
picks up. An INSUFFICIENT fresh probe (or a create-time degrade) falls back to
`[Approve for later]` + the remediation GRANT, creating nothing. Offline pins:
`test_mv_create_at_approval.py` (happy path → `OBO_CREATED` ledger under an
advice run; degrade-with-remediation; revalidation/rung-below/collision refusals;
route created/degraded/OBO-required) and the shared-component structural test
`MvSharedAcceptFlow.test.tsx`. Live re-confirmation is the scenario_d
create-at-approval sub-leg (create → `Type: METRIC_VIEW` → ledger provenance →
manual teardown).

### 2026-08-25 — Third look (finding: quality display), the score shown as "NN% confidence / LOW" is a category error — RESOLVED (Prompt 15.8, MV-D35, 2026-08-25)

**Observed.** Third smoke run, reviewer's challenge: "if a metric view is
syntactically accurate, executable, and orthogonal to existing metric views,
why is our confidence low? Would you accept a low-confidence suggestion?" The
card led with "NN% confidence / LOW" for proposals that were in fact validated,
executable, and non-overlapping.

**Impact.** The display was a category error twice band-aided (MV-D32(1)'s
caption, 15.7b's badge) rather than replaced. Quality is binary and already
gated — nothing surfaces without a rendered, validated, placeholder-free,
executable body (MV-D8, MV-D29, 15.5's servable-body invariant), and dedup
guarantees non-overlap; the LYDS score measures *demand evidence* (a ranking
signal), not doubt about correctness. Showing it as confidence told users their
strongest suggestions were weak.

**Resolution (Prompt 15.8, MV-D35).** The card now leads with a facts row of the
gates it PROVES ran — validated / executable / no-overlap — each key present
only when its gate provably ran for that row (`_mv_checks_from_row` in
`auto_optimize.py`, computed at hydration from `proposed_object` + `conflicts`,
never persisted, so a check that did not run is never rendered). The numeric
score orders the list and picks one Recommended (or an orthogonality callout
when all surfaced proposals are disjoint) and is NEVER rendered as a percent or
the word "confidence" (`mvFormat.ts`; the evidence caption replaces "Confidence
basis:"). Both surfaces render through the one shared card + display module.
Offline pins: `mvFormat.test.ts` (facts row, orthogonality, resolved-evidence
caption, no "%"/"confidence"), `MvSharedAcceptFlow.test.tsx` (re-grep of both
rendered surfaces). Live re-confirmation is the scenario_d rerun's IQ + run-
output surfaces.

### 2026-08-25 — Third look (four presentation defects) — RESOLVED (Prompt 15.8, 2026-08-25)

**Observed / Resolution.** Four numbered defects from the third look, each
fixed and adversarially re-greppable: **(1) count truth** — the run-output
header counts created suggestions from the accept flow's callbacks, not a static
proposal length (`MvSuggestOnlyPanel.tsx`); **(2) uniform collapse** — cards
default collapsed unless explicitly Recommended, identical on both surfaces
(`MvProposalCard.tsx` `defaultExpanded ?? Boolean(recommended)`); **(3) ACL-
derived grantees** — the consent modal's GRANT preview names a real audience
principal (CAN RUN/VIEW/MANAGE, deduped) via `_space_audience_grantees`
(`auto_optimize.py`) → `MvProbeResult.audience_grantees`, falling back to the raw
statement when the ACL is unreadable; **(4) ETA phrasing** — the scan-progress
line drops the "usually takes about that long" projection when only a single
sample exists (`MvIqScanAdvisorySection.tsx` `ScanProgress`), an honest-estimate
fix. Offline pins across `test_mv_create_at_approval.py`, `mvFormat.test.ts`,
`MvIqScanAdvisorySection.test.tsx`, `MvOutputPanels.test.tsx`.

### 2026-08-25 — Third look finding 1: semantic canvas mockup-vs-reality fidelity gap — OPEN (tracked to Prompt 12f, same redeploy)

**Observed.** Third look found a large fidelity gap between the approved v7
mockup and the deployed canvas: a small, cramped diagram floating in a mostly-
empty panel vs the mockup's spacious, legible, legend-equipped frame.

**Root cause (recorded).** The v7 mockup was never committed — the v3 note
(`semantic-graph-v2-note.md` / v3 note §3–§4) names it "the visual contract" but
it existed only in the interactive session that produced it (brainstorm v4→v7).
Prompt 12e implemented the mechanics against an uncommitted contract; structural
tests cannot see density; the reviewer met the gap in production.

**Not in Prompt 15.8's scope.** Tracked to **Prompt 12f** (runs with 15.8 in the
same redeploy): step 0 commits the v7 frame as an emitter mockup (frame 9-family,
both themes) as the fidelity-gate reference, then — after reviewer confirmation
that the committed frame is the approved contract — step 1 reconciles
`SemanticGraph` fidelity to it (initial render fills the viewport via
`computeFit`, density/spacing/typography/legend/controls/chips/badges). The 12e
mechanics (dedup canvas, proof arrows, boundaries, drag) are not rebuilt. The
new fidelity-gate rule (both rules copies) exists to prevent a recurrence.

**Resolution, round 2 (2026-08-25, after reviewer confirmed the committed 9e
frame and reported the remaining gap as "polish and professionalism").** Step 1's
first pass fixed the fit *math* but the reviewer's comparison still failed, so a
second reconciliation pass ran with the reviewer's instruction to compare for
myself. Six causes were found by reading the exports rather than the code:

1. **Dead reserved width (the dominant cause).** `layoutCards` sized content to
   `COL_X[3] + COL_W[3]`, reserving all four columns whether or not they held
   cards. A space whose measures all belong to metric views leaves column 3
   empty, so ~29% of the content box was empty space on the right — and
   `computeFit` dutifully shrank the *whole* canvas by that much. Empty columns
   are now compacted away and width follows the real rightmost edge; the column
   captions follow the compacted positions.
2. **Height-priority fit on a tall model.** `computeFit` took
   `min(byWidth, byHeight)`, so a 30-table stack (~2400 units) framed in a 520px
   canvas resolved to ~20% scale — the tiny-blob defect arriving by a second
   road, every label illegible so that the whole graph could "fit". The fit now
   prefers the width and refuses to shrink past a legibility floor (0.8, itself
   yielding to the width fit), top-aligning the overflow so drag pans it.
3. **Fixed canvas height.** A 380px box gave a wide-and-short model (the common
   shape) ~250px of dead vertical space. The canvas now takes the content's
   aspect ratio, bounded 220–520px. NOTE: `aspect-ratio` alone let the browser
   derive *width* from a binding `min-height` and the canvas blew out past its
   panel (measured 1316px inside a 798px parent) — the width is now explicit.
4. **Unbacked edge labels printing across card borders.** Selecting a card made
   every edge in its neighborhood verbose, so several full `ON` predicates and a
   second stacked relationship line drew straight over the cards. The reveal is
   now narrowed to the edge actually pointed at (hovered, or an endpoint IS the
   selection), collapsed to one line, and every edge label carries an opaque
   plate. Same for the boundary caption and the `replaces` label.
5. **Erasure-grade dimming.** Out-of-focus cards at 0.3 opacity were unreadable
   and translucent enough for edges to print through their labels; now 0.45.
6. **The inset was a flat key/value list.** The v7 frame's curator inset is where
   a curator decides something, so `NodeDetail` was rebuilt as a titled panel:
   governance roll-up chips, source tables, the metric view's measures (with the
   canvas's own `+N unnamed` treatment, which the inset previously contradicted
   by printing `canonical_expr` internals raw), and the declared joins with their
   full `ON` predicates — the detail the canvas can only afford a glyph for.

Also: frame `9f` now renders the **full** `SemanticModelView` (panel header,
ladder, canvas, inset) rather than the bare `SemanticGraph`. Exporting only the
canvas made the comparison against 9e — which depicts the whole panel — unfair in
reality's favour, and hid the surface where most of the gap lived. A new
`initialSelectedId` prop seeds the selection so a static export can show the
selected state at all (boundary, focus dimming, inset), which is the state the
contract frame depicts.

**Named deviations after round 2 (all subsequently CLOSED — see round 3).** Round
2 ended with five named deviations: loose-measure placement, the unmodeled
region, the YAML-derived inset fields, the Reset control, and edge bundling at 30
tables. The reviewer's instruction was to close **all of them** and treat the
committed 9e frame as binding, so none of the five stands as an accepted
deviation. Round 3 below records how each closed.

**Resolution, round 3 (2026-08-25, reviewer: "treat the committed 9e frame as
binding and match it fully").**

1. **Loose measures moved below the canvas. (SUPERSEDED — see round 4.)** The
   "concepts" card is filtered out of `layoutCards` and rendered as
   `LooseMeasuresPanel`, a full-width HTML panel under the legend. This was WRONG:
   the v3 note §3 places the Space-config box "on the right … plus a Space-config
   box for the loose measures" — i.e. a peer of the MV boxes on the canvas, not a
   panel below it. The panel was a mockup-width convenience adopted as the
   functional design. Round 4 restores §3 (see below).
2. **Labeled UNMODELED region.** Unmodeled table cards are wrapped in padded
   dashed danger-tinted rects with one `UNMODELED · in no metric view` caption
   plate at the topmost rect, clamped below the column headers. The per-card
   7.5px "no metric view" footnote is gone — the region says it once, as the
   frame does.
3. **Reset always visible**, disabled until there is a drag to reset, with a
   title that says which state it is in.
4. **The YAML-derived inset fields are now carried by the payload.** This was a
   *backend* gap, not a display choice, so the reader was extended rather than
   the omission defended. `_metric_view_internals` now returns `filter`,
   `materialization` and `dimensions` (with each dimension's resolved binding)
   from the same parsed document 12e already read — no second DESCRIBE — and the
   node carries `mv_source`, `mv_filter`, `materialization`, `dimensions`.
   Field names follow the metric view YAML reference: a join's `name` is its
   ALIAS and `source` is the joined table, `fields` is the current spelling of
   `dimensions`, `materialization` is an object (`mode` / `schedule` /
   `materialized_views`) summarised to one line of posture, never printed raw.
   An unreadable YAML carries NO detail — unreadable stays unproven.
   `NodeDetail` renders the join tree rooted at `mv_source` (a root guessed from
   join direction is wrong whenever the config declares a fact→fact join, which
   is exactly the frame's shape), dimensions grouped by binding, and a
   `Definition` section with filter / served / reuse.
5. **Overlap warnings are real.** A loose measure whose NAME an attached metric
   view already exposes under a different expression — the collision
   canonical-expression dedup cannot merge — is flagged server-side (`overlaps`)
   and raised in both the Space-config panel and the inset. The earlier
   "overlaps … (≈ ADR)" chip claimed expression-level equivalence the server did
   not compute; it now says what is true.
6. **Edge bundling.** Above `BUNDLE_MIN` (6) edges on one card side, the fan is a
   bundle: members drop their at-rest labels (hover still reveals the predicate)
   and route as §5 orthogonal elbows through a shared trunk, so 29 strands read
   as one line plus short per-row stubs instead of a ribbon; the group gets one
   `N declared joins` chip under its hub card, where there is room for it.
   Bundling counts JOINS only — a `uses` edge on the same side is neither
   labelled nor a declared join, and counting it made the chip lie ("30" for 29).

Two polish fixes came out of the round-3 comparison itself:

- The per-chip rung letter is drawn only when a card's measures actually differ
  in rung. Inside a metric view they never do, and 24 copies of the same "G" was
  noise; the rung stays in each chip's accessible label.
- Coverage badges on table cards are inset rather than centred on the border,
  where they half-hung into the gutter and read unfinished.

**Verification.** 386 frontend tests green and 2193 backend tests green; `tsc`
and lint clean; frames re-emitted in both themes and compared against 9e. The
join tree, dimensions-by-binding, filter/served/reuse rows and the bundled
30-table trunk were each confirmed in the exported screenshots. A static export
still cannot exercise the measured-viewport fit, so the runtime fill is confirmed
on the redeploy.

**Resolution, round 4 (2026-08-25, reviewer: "loose measures back on the right
(v3 §3 / image #2) and correct the 9e contract + gap report to match. Fix 2 and 3
as well. Also, ensure UI is matching the reference functionally").** Deploying
round 3 surfaced three real breaks from the reference and the v3 note, each now
closed and the 9e contract corrected to match rather than the code bent to a
stale frame:

1. **Loose measures back ON the canvas, on the right (v3 §3).** (Partially
   SUPERSEDED — round 5 moved this to its OWN column `CONCEPTS_COL=3`, since
   stacking it under the MV boxes read as "below"; see round 5.) The round-3
   `LooseMeasuresPanel` is deleted. `buildCards` now places the concepts card at
   `(CONCEPTS_COL=2, CONCEPTS_ROW=1_000_000)` — the metric-view column, with a
   sentinel row so it deterministically stacks BELOW the real MV boxes — and it
   renders through the same `CardView` as every other card ("Space config" title,
   "loose measures · not in any MV" subtitle, governance pill, measure chips,
   `+N unnamed`, and an on-canvas amber overlap caret on any colliding name). The
   **9e contract frame was corrected to match**: its `SpaceConfigBox` HTML panel
   is replaced by an on-canvas `SpaceConfigNode` SVG group in the measures column
   below the MV boxes. Item 1 of round 3 is thereby reversed, not merely amended.
2. **Selection lights the edges (v3 §4 / reference `BlueprintEdge`).** A join
   edge at rest stays neutral grey; when an endpoint is selected the edge switches
   to the accent hue with an accent arrowhead (`url(#mv-arrow-on)`) over a soft
   accent glow underlay, so clicking a box makes its lines "light up" the way the
   reference does. Previously the active join only thickened — invisible against a
   busy canvas.
3. **Selecting a MEASURE wraps its owning MV's tables (v3 §4).** The select-time
   boundary now resolves a selected measure to its owning metric view via the
   `membership` edge and anchors the hull/rect on that MV's `uses`-tables — not
   only on a direct MV-card click. Clicking `total_revenue` inside Revenue MV now
   draws "Tables used by Revenue MV" around orders / order_items / customer.

**Reference parity (functional).** To match the reference's interaction model,
`SemanticGraph` now clears the selection on an empty-canvas click (a background
press that releases without a pan) and `SemanticModelView` toggles the selection
off when the same node is re-clicked. `onSelectNode` accepts `null` to carry the
deselect. Hover still surfaces the tooltip (`<title>`); pan/zoom/drag are
unchanged.

**Verification (round 4).** 388 frontend tests green (new pins: concepts card in
col 2, accent arrowhead only appears on selection, measure→owning-MV boundary,
Space-config box on canvas / collapse behaviour); `tsc` and lint clean; all 41
mockup HTML files re-emitted in both themes (9e now shows the on-canvas
Space-config box; 9f/9g/9i render the corrected real `SemanticGraph`). Runtime
fit and the click interactions are confirmed on the redeploy to fevm-serverless.

**Resolution, round 5 (2026-08-25, reviewer, nine points on the deployed
canvas).** After the round-4 deploy the reviewer filed nine issues; each is now
closed. Where a change had a design fork the reviewer chose the direction (own
Space-config column; prove fact/dim or stay neutral; make Impact meaningful and
gate it; definition edges on-select; calmer palette).

1. **Space config in its OWN column, not below.** Round 4 stacked it under the MV
   boxes in col 2, where a tall MV column pushed it out of sight and it read as
   "below". `CONCEPTS_COL` is now `3` — a dedicated **"Space config · measures"**
   column to the RIGHT of the metric views. Column headers are honest:
   `["Source tables", "Joined tables", "Metric views · measures", "Space config ·
   measures"]`.
2. **Click a measure → its expression + description.** Measure nodes now carry
   `expr` and (best-effort) `description` on the wire (`MvSemanticGraphNode` /
   `SemanticGraphNode`). The backend fills `expr` from the canonical expression
   (governed), the snippet SQL (curated), and leaves it `None` for an ungoverned
   proposal (name only). `NodeDetail` renders a **Definition** section (expression
   in a mono block + description) for a selected measure.
3. **Legend disambiguates the line kinds.** The legend now draws a SOLID grey
   line = *declared join (N:1)* and a DASHED accent line = *MV definition (on
   select)*, plus a governed·curated·ungoverned dot cluster and a dashed-danger
   *unmodeled* chip.
4. **Text overlap fixed.** The MV title takes the full card width on row 1; the
   "N measures · governed" pill drops to row 2, so a long MV name
   (`customer_analytics_metrics`) no longer collides with the pill.
5. **Arrows cleaned up.** The "hodge podge" was the dotted MV→table (`uses`)
   edges drawn for EVERY MV at rest. They now render **only for the selected MV**;
   at rest the canvas shows just the join skeleton. The data still carries every
   `uses` edge (the boundary + unmodeled region read it) — only the rendering is
   gated.
6. **Fact vs dim, honestly.** The caption was `col === 0 ? "FACT" : "DIM"` — a
   pure layout guess, which is why `dim_property` / `dim_user` showed **FACT**. The
   backend now sets a PROVEN `role` (`fact` = a metric view's declared `source`,
   `dim` = an MV-joined table) and the caption reads `FACT` / `DIM` only when
   proven, else a neutral **`TABLE`**. A table known only from `join_specs` stays
   neutral rather than being guessed.
7. **Impact now means something.** Impact (downstream blast radius) is only
   meaningful for a selected TABLE, so the toggle is **disabled + greyed** (with a
   tooltip) unless a table is selected, and the effective mode falls back to
   Lineage otherwise — it never appears to do nothing.
8. **Calmer palette.** MV boxes are a near-neutral tint at rest (accent only on
   selection); coverage badges are neutral grey (accent reserved for
   selection/lineage). Governance stays a small dot vocabulary in the legend.
9. **Clearer boundary.** The select-time boundary is a solid 2px accent border
   over a 0.10 accent wash (dashed only for the disjoint HULL case) with a FILLED
   accent label chip ("Tables used by …"), replacing the faint 0.06 dotted rect.

The authoritative post-round-5 export is `RealModelV7Frame`
(`Mv12fFidelityFrames.tsx`), which renders the production `SemanticGraph`. The
hand-drawn 9e frame is kept as the step-0 layout reference (headers relabelled;
its geometry intentionally not re-hand-drawn) and its header records what
round-5 supersedes.

**Verification (round 5).** 394 frontend tests green (new pins: role-based
FACT/DIM/TABLE caption, `uses` edges hidden at rest / shown on MV select, legend
line disambiguation, Impact disabled until a table is selected, measure
expression+description in the inset, concepts card in col 3) and 31 backend
`test_semantic_graph` tests green (new pins: proven `role`, measure
`expr`/`description` on the wire); `tsc` and lint clean; all 41 mockup HTML files
re-emitted in both themes with the round-5 fixture (`role` on tables, `expr` on
measures).

**Resolution, round 6 (2026-08-25, reviewer: "arrows still hodge podge and
illegible … too many arrows … columns and tables too close"; "Show proposal
overlay tends to hide some measures"; "I don't see any MV proposals when I click
on it").** Round 5 gated the dotted `uses` edges but left the SOLID join edges
loud at rest, so the join skeleton itself still read as spaghetti — and the
overlay was actively hiding the very measures it was meant to explain. The root
causes came straight from the reference the reviewer keeps citing
(`7-Eleven … BlueprintEdge`), read line-by-line this round.

1. **Arrows — focus + context (the real fix).** The reference is clean not because
   of routing but because of edge STATE: at rest every edge is faint
   (`opacity 0.4`), and on selection the clicked node's edges light up while
   **every other edge is dimmed to near-invisible** (`opacity 0.06`,
   `getEdgeState → 'dimmed'`). Ours did the opposite — joins were full-opacity at
   rest AND on select we only brightened neighbours, never dimming the rest, so a
   click *added* clutter. `EdgeView` now takes a `dimmed` prop; the render loop
   sets `dimmed = highlight != null && !active && hoverEdge !== index`. Result: a
   quiet faint skeleton at rest; a click leaves only the handful of edges touching
   the selection, everything else fades out.
2. **Labels on demand.** The at-rest per-edge glyph (`N:1`/`SCD2`/`ON …`) is gone;
   a join paints a label ONLY when hovered or when an endpoint IS the selection.
   The resting canvas has zero floating plates (the legend still names the glyph as
   vocabulary). This matches the reference, which shows nothing until you point.
3. **More room.** `COL_GUTTER` 40 → 68 and `VGAP` 26 → 34 so curves have a wider
   channel and stacked cards more air — the reviewer's own "too close" diagnosis.
   Both are visual-only (the collapse threshold keys off `ROW_GAP`, untouched).
4. **Overlay: keep + link (stop hiding measures).** The old `withOverlay` added a
   `membership` edge from the loose measure to the ghost MV, which made
   `buildCards` MOVE that measure OUT of the Space-config box and INTO a ghost card
   placed at `col 2, row mvRows+i` — below the real MVs, off the visible frame. So
   toggling the overlay looked like it hid measures and showed no proposals (they
   were below the fold). The overlay now KEEPS every loose measure where it is and
   draws a dashed **"would govern →"** link (new client-only edge `kind: "governs"`)
   from a visible ghost proposed-MV card to the measure it would govern. The raw
   -tables-freed `replaces` spaghetti is dropped from the canvas (it lives in the
   proposal's detail instead). `SemanticModelView` remounts `SemanticGraph` on
   overlay toggle (`key`) so the canvas re-fits and frames the new ghost cards.
5. **Legend gains the link.** When the overlay is on, the legend adds a dashed
   accent **"would govern (proposal)"** entry.

The authoritative export of the overlay-on state is the new REAL-component frame
`RealModelOverlayFrame` (**9j**, `Mv12fFidelityFrames.tsx`). The hand-drawn 9c
overlay frame (Prompt-12 scaffold) is left as historical record with a header note
pointing at 9j; the 9e contract frame's header records the round-6 supersession.

**Verification (round 6).** 395 frontend tests green (new/updated pins: join
labels appear on selection not at rest; non-neighbor edges dimmed to `0.06` once
something is selected; `withOverlay` adds a `governs` link and NO membership move,
leaving the loose measure in Space config; the overlaid graph renders the ghost MV
+ "would govern" link + keeps the measure chipped). `tsc` and lint clean. No
backend change this round (edges are client-synthesized). Mockups re-emitted in
both themes, now including 9j (overlay ON, keep + link).

**Resolution, round 7 (2026-08-25, reviewer: "Why are there 2 arrows between 2
tables? If it's bi-directional, we should limit to 1 right?" and "Why doesn't
lineage light up for space config measures?").** Two independent gaps, both real.

1. **Two arrows for one relationship → collapse to one.** `dim_property ↔ dim_host`
   drew two near-parallel join arrows because the config declares two `join_specs`
   for that pair (a reciprocal declaration plus an SCD2 `is_current` current-row
   variant), and `_build_semantic_graph` emits one edge per spec. A reader sees a
   single relationship, so the canvas should show one arrow. New exported pure
   helper `collapsePairJoins(items)` (SemanticGraph) keeps ONE `join` per unordered
   card pair, preferring the left→right direction so the surviving arrowhead points
   at the joined (dim) side; `renderedEdges` runs it after building items. Nothing is
   lost — the full predicate list still lives in the node-detail "Declared joins"
   panel. Non-join edges pass through untouched.
2. **Loose-measure lineage — the `derives` edge.** A Space-config (loose) measure
   belongs to no metric view, so it carried NO edge: `focusSet` found an empty
   neighbourhood and round-6 focus+context then dimmed the whole canvas — selecting
   it made things *quieter*, not lit. A loose measure's real lineage is the tables
   its EXPRESSION reads. New backend pass (after the measure ladder, before the
   coverage lens) parses each non-governed measure's `expr` with `_expr_table_refs`
   (fully-qualified `catalog.schema.table.column` → its table) and emits a
   `derives` edge measure → table (new `MvSemanticGraphEdge.kind` literal). A table
   the expression proves but the space never modeled is ADDED to the canvas as a
   neutral source table (`role=None`), so it lands in the unmodeled region — the
   honest read that the curated measure depends on an ungoverned table. The client
   renders `derives` ONLY when its measure is the selection (like `uses`): a dashed
   accent link, and `focusSet` (which walks every edge kind) lights the referenced
   table(s). Governed measures already wrap their MV on select, so they are skipped;
   ungoverned proposals carry no expr, so they contribute nothing.

**Verification (round 7).** Backend: 34 semantic-graph tests green — new pins:
`_expr_table_refs` extracts the 4-part table (add-eligible), treats a 3-part run as
match-only, ignores bare functions/2-part columns; a curated loose measure emits one
`derives` edge and ADDS the referenced table as a neutral source; a governed measure
emits none. Frontend: 93 model tests green — new pins: `focusSet` lights a loose
measure's `derives` source table (and only it); `collapsePairJoins` collapses a
reciprocal pair to one, keeps the left→right survivor, and leaves distinct pairs +
non-join edges alone. `tsc` and lint clean.
