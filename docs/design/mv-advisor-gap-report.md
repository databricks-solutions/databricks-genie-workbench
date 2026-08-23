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
| Read against | `feature/metric-view-advisor` @ `1d0d04b9` (branched from `main` @ `ae8c4367`, "GSO optimizer-v2: engine rewrite + Genie Agent rename (#315)") |
| Date of survey | 2026-08-23 |
| Method | Direct file reads. Every quote below was read from the working tree on the date above. Line numbers are from that state. |
| Scope | Read-only. No feature code was written or modified in producing this report. |

## Headline

The POV was written against a GSO job that **no longer exists**. `main` @ `ae8c4367`
replaced the multi-stage wheel DAG with a **linear four-task notebook DAG**, and the
replacement is *enforced by passing unit tests* that specifically assert the POV's task
names are gone and that condition tasks and task values are forbidden.

This is not a naming mismatch that can be papered over. Three structural mechanisms the
POV depends on — `condition_task` gates, `{{tasks.X.values.Y}}` handoff, and
`python_wheel_task` entry points — are each individually prohibited by a test that
passes today.

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

```397:406:packages/genie-space-optimizer/tests/unit/test_phase7_job_dag.py
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

```text
src/genie_space_optimizer/
  _telemetry.py, _version.py, _workspace_client.py
  backend/        job_launcher.py, utils.py          # shared with Workbench
  common/         config.py (2506 L), genie_client.py, metric_view_catalog.py,
                  asset_semantics.py, delta_helpers.py, warehouse.py, uc_metadata.py, ...
  integration/    trigger.py, apply.py, discard.py, revert.py, levers.py, types.py
  iq_scan/        scoring.py, context.py, rls_audit.py
  jobs/           the four notebooks + _helpers.py
  optimization/   applier.py (4721 L), benchmarking.py (4594 L), unified_loop.py (3628 L),
                  preflight.py (3461 L), state.py (1790 L), publish.py, ddl.py,
                  eval_runner.py, leakage.py, models.py, champion.py,
                  wide_schema*.py, genie_eval_taxonomy.py,
                  mv_fingerprint.py (1333 L), mv_scoring.py (945 L),
                  mv_state.py (639 L), ...
```

**Existing metric-view surface** (relevant — the advisor is not starting from zero):

| Module | Role |
|---|---|
| `common/metric_view_catalog.py` (690 L) | `detect_metric_views_via_catalog_with_outcomes(...)` / `detect_metric_views_via_catalog(...)` / `summarize_outcomes(...)` — DESCRIBE-based MV detection and YAML extraction |
| `common/asset_semantics.py` | `is_metric_view`, table/MV shelf split, measure maps |
| `optimization/preflight.py` | `_profile_metric_view`, reclassification into `_metric_view_yaml` |
| `optimization/benchmarking.py` | MV join precheck/repair, `MEASURE()` wrapping, `build_metric_view_measures` |
| `optimization/wide_schema_history.py` | `system.query.history` mining (`:248`), warehouse-history fallback (`:357`) |

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
`EVALUATION_FAILED` is returned on the first attempt (`unified_loop.py:133-146`,
`:1278-1298`).

`tests/unit/test_eval_timeouts.py` covers conversation/statement timeouts, **not** the
eval-run adapter. Seam contract tests (terminal statuses, taxonomy reasons, `run_subset`
serialization, `lift_report`) live in `tests/unit/test_eval_runner.py`.

**Where `eval_run_id` is stored:** `genie_opt_iterations.eval_run_id` /
`.eval_run_status`, written by `state.write_iteration` (`state.py:923-966`). This
directly satisfies POV Recommendation 2 ("store the `eval_run_id`, not a copied score") —
though the accuracy is *also* stored, in `overall_accuracy`.

**Baseline eval is in-process, not a task:**

```2854:2859:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py
    baseline_eval = _native_eval(
        w,
        space_id=space_id,
        benchmarks=benchmarks,
        iteration=0,
    )
```

reached from `jobs/run_optimize.py:337` → `run_unified_optimization_loop(...)`. Candidate
evals run in the same loop at `unified_loop.py:3287-3292`.

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

**Patch types.** The canonical registry is `PATCH_TYPES` in `common/config.py:2005` — 50
keys (the section comment says 35 and is stale). The Lever-2 metric-view subset exists
(`add_mv_measure`, `update_mv_measure`, `remove_mv_measure`, `add_mv_dimension`,
`remove_mv_dimension`, `update_mv_yaml`, all `scope: uc_artifact`) at `config.py:2067-2103`.

But the **live loop allowlist is 11 types and contains no MV type**:

```79:93:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py
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
| `_apply_action_to_config` | `(config: dict, action: dict) -> bool` | `applier.py:3409` |
| `_apply_action_to_uc` | `(w: WorkspaceClient, action: dict) -> bool` | `applier.py:3942` |
| `apply_patch_set` | `(w, space_id, patches, metadata_snapshot, *, apply_mode, deploy_target, force_apply, benchmark_corpus) -> dict` | `applier.py:4025` |

**Critical gap — nothing attaches a metric view to a space.** `add_table` / `remove_table`
mutate `data_sources.tables` only (`applier.py:3871-3886`). The MV sections are explicit
config-level no-ops:

```3914:3917:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py
    # ── TVF / MV operations (config-level no-ops for uc_artifact patches) ──
    if section in ("tvf_parameters", "tvf_definition", "tvfs", "mv_measures", "mv_dimensions", "mv_yaml"):
        if section == "tvfs":
            funcs = config.setdefault("instructions", {}).setdefault("sql_functions", [])
```

Only the `tvfs` branch does anything (it appends to `instructions.sql_functions`); the
three `mv_*` sections fall straight through to `return True` at `applier.py:3932`.

`_apply_action_to_uc` handles only `update_column_description`, `update_description`, and
`update_tvf_sql` — **no MV DDL**. Repo-wide grep for `WITH METRICS` returns **zero hits**.

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

```4550:4560:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py
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
bookkeeping is `state.mark_patches_rolled_back` (`state.py:1101-1108`).

**Consequence for the POV.** POV §7.8 step 6 says the MV attachment "is an ordinary patch,
[so] it inherits the existing versioning, diff, and rollback machinery." Half true: the
machinery would carry it, but the patch type, the applier branch, and the DDL execution
path all have to be built. And POV §7.8's `DETACH_ONLY_NEVER_DROP` semantics — reverting
*one* patch while leaving a UC object alone — has no analogue in a whole-snapshot rollback
model.

**Leakage firewall.** `optimization/leakage.py:414-420` `is_benchmark_leak(...)` is the
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

#### Delta — nine tables, not ~15

`optimization/ddl.py` is the sole definition point:

```280:290:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
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
}
```

> **Refreshed for `MV-D7`:** the last three entries are the metric view advisor
> tables added in Prompt 1; the counts in this section were refreshed with them.

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

Two more exist outside `_ALL_DDL`: `genie_opt_scan_snapshots` (`scan_snapshots.py:41-58`)
and the per-domain `genie_benchmarks_{domain}` (`benchmarks.py:194-209`).

Names are constants in `common/config.py:1830-1845`. FQN is
`{catalog}.{schema}.<table>`, resolved by string replacement:

```69:70:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py
    for name, ddl in _ALL_DDL.items():
        resolved = ddl.replace("{catalog}", catalog).replace("{schema}", schema)
```

`genie_opt_artifacts.artifact_kind` is the extension point an advisor would use — it is a
documented enum in the column comment:

```182:182:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
    artifact_kind       STRING        NOT NULL COMMENT 'run_manifest | wide_schema_inventory | wide_schema_evidence | wide_schema_selection_plan | wide_schema_profile_telemetry | wide_schema_prompt_telemetry | wide_schema_audit | space_metadata | benchmark_qc | space_quality_enrichment | publish_record',
```

#### Migration mechanism

Additive only, via a tuple of `(table, column, "TYPE COMMENT '...'")`:

```292:297:packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ddl.py
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
(`backend/routers/auto_optimize.py:51`), `/api/watch/*`.

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

```1401:1432:backend/routers/auto_optimize.py
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

```171:187:backend/routers/auto_optimize.py
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

**Full route inventory** (22 routes, all under `/api/auto-optimize`): `GET /health`,
`GET /permissions/{space_id}`, `POST /trigger`, `GET /runs/{run_id}`,
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

#### No user-level UC entitlement probe exists

`GET /api/auto-optimize/permissions/{space_id}` (`auto_optimize.py:1225-1398`) probes the
**service principal** — Genie `CAN_MANAGE` plus SP schema read via
`probe_sp_required_access(sp_ws, ...)` — and returns grant SQL naming the SP. It answers
"can the SP run this job", not "may this user create an object here."

`backend/services/uc_client.py` has no permission/privilege/grant surface at all —
`search_tables`, `list_catalogs`, `list_schemas`, `list_tables`, `get_table_columns` only.
`scripts/grant_permissions.py` is a deploy-time CLI that grants to the SP.

POV §7.3.1's entitlement probe is entirely new construction.

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

`frontend/src/components/auto-optimize/OptimizationConfig.tsx` (358 L). Two-column
layout: left = Optimization Scope (six lever checkboxes + benchmark-repair consent
checkbox); right = model picker + stopping criteria; below = query-usage warehouse
selection; footer = health issues, `PermissionAlert`, Start button.

State is eight local `useState` hooks (`:55-63`). The Start button gate:

```346:346:frontend/src/components/auto-optimize/OptimizationConfig.tsx
              disabled={loading || hasActiveRun || selectedLevers.size === 0 || !canStart || !knobsValid}
```

**There is an existing precedent for a consent checkbox with a warning** — the benchmark
repair toggle at `:141-164`, default off, revealing an amber warning when checked. The
POV's "Suggest metric views" toggle should follow this pattern.

Payload assembly is factored into a pure helper (deliberately, for the
`react-refresh/only-export-components` lint rule):

```28:50:frontend/src/components/auto-optimize/optimizationRequest.ts
export function buildOptimizationTriggerRequest(args: {
  spaceId: string
  applyMode: "genie_config" | "both"
  selectedLevers: Set<number>
  selectedModel: string | null
  targetAccuracy: number
  maxAttempts: number
  workloadWarehouseIds?: string[]
  benchmarkPolicy: "review_only" | "repair_allowed"
}): GSOTriggerRequest {
  return {
    space_id: args.spaceId,
    apply_mode: args.applyMode,
    levers: Array.from(args.selectedLevers)
      .filter((id) => id >= 1 && id <= 6)
      .sort((a, b) => a - b),
    llm_model: args.selectedModel,
    target_accuracy: args.targetAccuracy,
    max_attempts: args.maxAttempts,
    workload_warehouse_ids: args.workloadWarehouseIds ?? [],
    benchmark_policy: args.benchmarkPolicy,
  }
}
```

Covered by `OptimizationConfig.test.tsx` (189 L) and `contract.test.ts` (126 L).

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
| Backend | pytest 9.0.2 + pytest-asyncio 1.3.0 (`asyncio_mode=auto`) | `backend/tests/` | 23 files | `./scripts/test.sh` |
| GSO | pytest 9.0.2 (`pythonpath=["src"]`) | `packages/genie-space-optimizer/tests/unit/` | 83 files | `uv run pytest` from the package dir |
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
| `test_phase7_job_dag.py` (406 L) | The DABs job shape: four linear tasks, no condition tasks, no task values, parameter defaults, launcher parameter threading |
| `test_four_notebook_architecture.py` (86 L) | AST-walks the import closure of all four notebooks and fails on any import of 17 retired modules |
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

#### Known pre-existing backend failures: none

The expected baseline for `./scripts/test.sh` is **530 passed, 0 failed**. There is no
standing residue, so any backend failure during MV advisor work is either newly introduced
or an environment fault — never something to wave through as pre-existing.

Because no CI ever runs these suites, a broken local environment can look exactly like
feature breakage for a long time. Prompt 1's VERIFY hit both faults at once, and both were
local to the machine rather than to the repo:

| Symptom | Count | Root cause |
|---|---|---|
| `ImportError: cannot import name 'preview_revert_options' from 'genie_space_optimizer.integration'`, at collection, in `test_auto_optimize_router.py` and `test_current_version.py` | 2 collection errors | `_genie_space_optimizer.pth` in the interpreter's site-packages pointed at a **different checkout** of this repo, which lacked the symbol. Every backend test was silently running against foreign source. |
| Async tests failing with `PytestUnknownMarkWarning: Unknown pytest.mark.asyncio` — 7 in `test_scanner.py::TestScanSpaceGsoSelection`, 4 in `test_watch_traffic_gap_router.py`, 1 in `test_auto_optimize_router.py` | 12 failures | `pytest-asyncio` was absent from the interpreter, so `asyncio_mode = "auto"` was inert and every coroutine test was collected but never awaited. |

Both are fixed by pointing the editable install at this checkout and installing the pinned
dev dependencies (`pytest==9.0.2`, `pytest-asyncio==1.3.0` — root `pyproject.toml:20`):

```bash
uv pip install --python "$(pyenv which python)" -e packages/genie-space-optimizer \
  --force-reinstall --no-deps
uv pip install --python "$(pyenv which python)" pytest==9.0.2 pytest-asyncio==1.3.0
python -c "import genie_space_optimizer; print(genie_space_optimizer.__file__)"
```

That last line is now a rule, not a suggestion: the MV advisor rules file requires printing
it and confirming the path is inside this checkout before any backend pytest result is
trusted. A run against a foreign checkout is void.

Note the shared-interpreter hazard behind the first fault: the editable install lives in a
**global** pyenv `site-packages`, so exactly one checkout can own the `.pth` at a time.
Two clones of this repo on one machine will keep stealing it from each other until one of
them gets its own virtualenv.

---

## 2. Gap table — every POV Part 7 assumption

Legend: **MATCHES** — exists as described. **CONFLICTS** — the repo has something
incompatible, and adopting the POV means changing or deleting existing behaviour.
**DOES-NOT-EXIST-YET** — greenfield, no conflict.

### 2.1 Task keys and DAG structure (POV §7.2, §7.7)

| POV assumption | Status | Evidence |
|---|---|---|
| `preflight` task | **CONFLICTS** | Split into `intake_and_snapshot` + `benchmark_qc_and_repair`; `test_phase7_job_dag.py:57-67` asserts `preflight` absent |
| `baseline` task | **CONFLICTS** | In-process iteration 0 in `optimize` (`unified_loop.py:2854-2859`); `baseline_eval` asserted absent |
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

Every one is **DOES-NOT-EXIST-YET**; each requires the four-place lockstep of
[§1.2](#12-the-four-place-parameter-lockstep-requirement). `space_id` is the only
pre-existing name.

| POV parameter | Default | Status |
|---|---|---|
| `space_id` | `""` | **MATCHES** — `databricks.yml:77-78` |
| `enable_metric_view_suggestions` | `"false"` | **DOES-NOT-EXIST-YET** |
| `mv_action_mode` | `"suggest_only"` | **DOES-NOT-EXIST-YET** |
| `mv_target_catalog` | `""` | **DOES-NOT-EXIST-YET** |
| `mv_target_schema` | `""` | **DOES-NOT-EXIST-YET** — note `schema` already exists with a *different* meaning (the GSO state schema), so the `mv_` prefix is load-bearing |
| `mv_materialize` | `"false"` | **DOES-NOT-EXIST-YET** |
| `mv_consent` | `""` (JSON) | **DOES-NOT-EXIST-YET** |
| `mv_min_confidence` | `"75"` | **DOES-NOT-EXIST-YET** |
| `enable_discover_curation` (§8.6) | — | **DOES-NOT-EXIST-YET** |

String-typed parameters are **MATCHES** as a convention: all 15 existing parameters are
strings, including the numeric ones (`"3"`, `"0.90"`).

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
| `POST /api/auto-optimize/runs` starts a run | **CONFLICTS (path)** | The route is `POST /api/auto-optimize/trigger` (`auto_optimize.py:1401`) |
| Request body carries `enable_metric_view_suggestions` etc. | **DOES-NOT-EXIST-YET** | `TriggerRequest` has 8 fields (`auto_optimize.py:171-187`) |
| `jobs.run_now(job_parameters={...})` | **MATCHES** | `job_launcher.py:91-126` |
| Pre-run entitlement probe under OBO | **DOES-NOT-EXIST-YET** | No user-level UC privilege probe exists anywhere; `/permissions/{space_id}` probes the **SP** |
| Probe result JSON (`probe_id`, `checked_as`, `verdict`, `missing`, `remediation_sql`, `fallback_mode`) | **DOES-NOT-EXIST-YET** | — |
| Consent recorded with the run | **DOES-NOT-EXIST-YET** | `genie_opt_runs` has no consent column; would need `ADDITIVE_COLUMN_MIGRATIONS` |
| "Writes execute under OBO" | **CONFLICTS** | The job runs as the SP (`databricks.yml:58-59`; `main.py:162-181` self-heals `run_as` to SP). No OBO token exists inside the job. |
| "The SP is never a write path for metric views" | **CONFLICTS** | Directly contradicted by the above |
| Preflight re-verification / downgrade-never-upgrade | **DOES-NOT-EXIST-YET** | No preflight task to host it; the closest hook is `intake_and_snapshot` |
| UI toggle + target picker + mode radio | **DOES-NOT-EXIST-YET** | `OptimizationConfig.tsx` has no catalog/schema picker; the benchmark-repair checkbox at `:141-164` is the pattern to copy |
| Copyable `GRANT` remediation | **PARTIALLY MATCHES** | Same idiom already exists for warehouse grants — read-only `<textarea>` at `OptimizationConfig.tsx:284-292` and `:300-310` |

### 2.5 The patch and apply path (POV §7.8)

| POV assumption | Status | Evidence |
|---|---|---|
| Patch = `field_path` + `new_value` | **CONFLICTS** | Patches are dicts of `type`/`target`/`new_text`/`old_text` (`applier.py:2649-2659`); `field_path` appears nowhere in `src/` |
| `field_path: "data_sources.metric_views"`, `operation: "append"` | **CONFLICTS** | No path-addressed patches; `op` ∈ `add`/`update`/`remove`/`update_section`/`rewrite` on the *rendered command* |
| Attach MV by patching `data_sources.metric_views[]` | **DOES-NOT-EXIST-YET** | `_apply_action_to_config` handles `tables` only (`:3871-3886`); MV sections are no-ops (`:3914-3932`) |
| Companion patch removing covered raw tables | **PARTIALLY MATCHES** | `remove_table` exists (`applier.py:3880-3886`) but is not in `_ALLOWED_PATCH_TYPES` |
| `CREATE VIEW … WITH METRICS LANGUAGE YAML` | **DOES-NOT-EXIST-YET** | Zero `WITH METRICS` hits repo-wide |
| `EXPLAIN CREATE MATERIALIZED VIEW` precheck | **DOES-NOT-EXIST-YET** | — |
| "Inherits existing versioning, diff, rollback" | **PARTIALLY MATCHES** | `genie_opt_patches` would carry it; but rollback is whole-snapshot (`applier.py:4550-4560`), so per-patch detach has no analogue |
| `on_regression: DETACH_ONLY_NEVER_DROP` | **DOES-NOT-EXIST-YET** | And incompatible with snapshot rollback as written |
| Idempotency key `sha256(space_id \| expr \| sources)` | **PARTIALLY MATCHES (precedent)** | `genie_opt_artifacts.content_hash` exists for exactly this (`ddl.py:184`); the launcher already builds a SHA-256 idempotency token (`job_launcher.py:33-37`) |
| `tables_freed` / 30-table cap | **DOES-NOT-EXIST-YET** | No table-count ceiling logic |
| Free text in MV proposals must clear the leakage firewall | **CONFLICTS with current coverage** | `_PATCH_TEXT_FIELDS` covers only example-SQL types (`leakage.py:286-289`); package AGENTS.md requires new text-carrying patch types to be routed through it |

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
| `mv_baseline` as a separate eval run isolating MV lift | **DOES-NOT-EXIST-YET** | Mechanically feasible in-process (a second `_native_eval` call at iteration 0.5), but costs one full eval run against the ~20 q/min ceiling |
| "roughly 15 Delta tables plus Lakebase" | **CONFLICTS** | Nine tables in `_ALL_DDL` (+2 outside it) — MV-D7 added the three `genie_opt_mv_*` tables |
| "MLflow… still the home for run provenance and versioning" | **CONFLICTS** | Decommissioned in Phase 5 (D3/D7). `models.py:1-16`: *"no MLflow LoggedModel snapshot, no UC Model Registry version, no per-mutation MLflow run."* GSO uses MLflow for **tracing only**. |

### 2.7 Scoring, signals, and outputs (POV §2, §3, §7.5)

| POV assumption | Status | Evidence |
|---|---|---|
| Blended `100*(0.35L+0.30Y+0.20S+0.15D)` score with HIGH/MEDIUM/LOW tiers | **EXISTS (Prompt 4)** | `optimization/mv_scoring.py` — component scorers, blend, tiers, dedup gate, POV Part 4 payload, persistence through `mv_state.upsert_mv_candidate`. Weights and every normalization constant in `common/config.py:2423-2506` (MV-D11), never inlined |
| sqlglot AST fingerprinting | **EXISTS (Prompt 3)** | `optimization/mv_fingerprint.py` — canonicalization, measure/dimension/filter/join-key extraction, shape classification, corpus scan; `sqlglot==30.0.3` pinned. Prior art: `benchmarking.py`, `unified_loop.py`, `wide_schema*.py` (**not** `applier.py`, which only names sqlglot in a comment at `:1534`) |
| Syntactic signal (**Y**) | **EXISTS (Prompt 4)** | `mv_scoring.syntactic_score` consumes a `RecurrenceSignal`. Its equivalence flag is **corpus-internal** (MV-D11) — governed-MV equivalence is the dedup gate's, not a multiplier's |
| `system.query.history` demand signal (**D**) | **MATCHES** | `wide_schema_history.py:248`, with warehouse-history fallback at `:357` and a documented unavailable path. Normalized into a 0-1 score by `mv_scoring.demand_score` (geometric mean, then 30-day half-life decay, per MV-D11) |
| Lineage signal (**L**) from `system.access.table_lineage` | **DOES-NOT-EXIST-YET** | Still no lineage reads in GSO, and Prompt 4 did not add one: `mv_scoring.lineage_overlap_score` takes a precomputed `LineageOverlap` and the module queries nothing (pinned by `test_this_module_queries_nothing`). GenieWatch reads `system.access.table_lineage` but as **SP-only** (`backend/watch/services/system_tables.py`) |
| Embedding / semantic signal (**S**) | **PARTIALLY MATCHES** | Two paths, no Vector Search index. Firewall: `leakage.py:46-57` (endpoint/threshold), `:139-144` (vectors, `question_embeddings = None` so disabled by default), `_cosine_similarity` at `:174`, `get_embedding` at `:189`, `precompute_benchmark_embeddings` at `:220`. Advisor: `mv_scoring.FoundationModelEmbeddingClient`, injectable and defaulting to `databricks-gte-large-en`, which borrows `get_embedding` and no other firewall symbol. *(Correction: this row previously anchored the firewall path at `leakage.py:132-137`, which is the `BenchmarkCorpus` shingle block. That anchor was **wrong when written**, not rotted by a commit — `leakage.py` is untouched on this branch. A quote can be stale by position with its content unchanged, which no keyword search can find; that is the case for byte-matching every fenced reference in VERIFY rather than grepping for stale wording.)* |
| Existing MV detection for dedup | **MATCHES** | `common/metric_view_catalog.py:63-73`, `:481-490`; flattened for the gate by `mv_scoring.metric_view_fields`, which keeps the `DESCRIBE ... AS JSON` parsing in `metric_view_catalog` |
| Harvest already-optimized space artifacts | **MATCHES (data available)** | `genie_opt_patches.patch_json`, `genie_opt_iterations.config_json` / `observed_config_json` |
| Copy-ready DDL panel with copy button | **DOES-NOT-EXIST-YET (components exist)** | `SqlCodeBlock.tsx` + `prism-react-renderer` |
| Patch shown as a diff | **DOES-NOT-EXIST-YET (component exists)** | `SqlDiffView.tsx` + `react-diff-viewer-continued` |
| `ddl_artifact_path` on a Volume | **CONFLICTS** | No Volumes convention for run artifacts; use `genie_opt_artifacts` |
| `candidate_table: main.genie_workbench.mv_candidates` | **CONFLICTS (location)** | GSO tables live in `{GSO_CATALOG}.{GSO_SCHEMA}` as `genie_opt_*`, not `genie_workbench` |
| Lineage/graph visualization | **MATCHES** | `react-force-graph-2d` already used in `watch/pages/ResourceGraphView.tsx` |

### 2.8 Discover curation (POV §8)

| POV assumption | Status |
|---|---|
| `discover_curator` task | **DOES-NOT-EXIST-YET** |
| Governed-tag application (`ALTER … SET TAGS`) | **DOES-NOT-EXIST-YET** — zero `governed_tag` / `MANAGE DISCOVERY` hits |
| `system.information_schema.table_tags` reads | **DOES-NOT-EXIST-YET** in GSO (`common/uc_metadata.py` has tag helpers but not for domains) |
| Domain/subdomain/Page proposals | **DOES-NOT-EXIST-YET** |
| PII / literal firewall on drafts | **PARTIALLY MATCHES** | `optimization/leakage.py` is a benchmark-leak firewall, not a PII scanner; the plumbing pattern transfers, the detector does not |

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
needs a `PATCH_TYPES` key (proposed: `attach_metric_view` / `detach_metric_view`), an
`_apply_action_to_config` branch for the `metric_views` section, an entry in
`_ALLOWED_PATCH_TYPES`, and — because it carries `comment`/`synonyms` free text — an entry
in `leakage._PATCH_TEXT_FIELDS`.

**9. Rollback semantics.** POV `DETACH_ONLY_NEVER_DROP` presumes per-patch revert. Today
rollback restores a whole config snapshot. Decide whether to build a targeted detach path
or accept that a rollback also reverts unrelated same-iteration patches.

**10. Where does `mv_baseline` fit?** As a second in-process `_native_eval` inside
`optimize`, it costs one extra full eval run per run against the ~20 questions/min
workspace ceiling. Confirm the isolated-lift measurement is worth that, or make it a
separate opt-in.

**11. Correct the POV's factual claims.** Nine Delta tables not ~15 (MV-D7 added
three `genie_opt_mv_*` tables); MLflow is tracing-only, not the provenance home;
the route is `POST /api/auto-optimize/trigger`; no Volumes artifact path; the
eval-run status field is `eval_run_status` not `status`.

**12. Testing and CI.** No CI runs any suite, and there is no live-workspace integration
harness. Decide whether the MV advisor ships with a CI workflow, and how the OBO
entitlement probe and the DDL path get tested at all given they cannot run offline.

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

Also at `leakage.py:454`, `:764`, `:927`, `:971`; `applier.py:4045`, `:4063`;
`publish.py:32`, `:601`; `test_scored_benchmark_qa_exclusion.py:1`. Note *"no train/held-out
split per D8"* — the whole scored set is protected.

*Window.* A 30–40 question working set, recommended (not enforced) over the post-merge
live set: `config.py:317`, `:322`; `preflight.py:2656`, `:2668`, `:2872`;
`genie_client.py:1384`; surfaced in the UI at
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

Plus the round-trip contract at `auto_optimize.py:129`, `:176` ("arch §13 / D9") and the
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
| "roughly 15 Delta tables" | Nine in `_ALL_DDL` after MV-D7 (eleven counting `genie_opt_scan_snapshots` and `genie_benchmarks_{domain}`) |
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
(`OptimizationConfig.tsx:284-292`) · diff viewer, SQL code block, force-directed graph,
charts (all in `frontend/package.json`) · OBO-only client accessor
(`require_obo_workspace_client`).
