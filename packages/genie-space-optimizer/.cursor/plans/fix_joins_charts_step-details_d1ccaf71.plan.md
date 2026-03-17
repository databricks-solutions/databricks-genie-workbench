---
name: Fix joins charts step-details
overview: "Fix three issues in the Optimization Pipeline view: (1) Referenced Joins reads from wrong config path, (2) Score Progression chart has wrong lever labels and values, (3) Pipeline step details are thin for steps 1/4/5 with stage prefix mismatch for step 4."
todos:
  - id: fix-joins
    content: Move instr extraction before joins block, change joins_raw to read from instructions.join_specs with ds fallback
    status: completed
  - id: fix-chart-labels
    content: "Fix IterationChart.tsx leverLabel: when lever===0 and iteration>0, show 'Iteration N' instead of 'Lever 0'"
    status: in_progress
  - id: fix-chart-zero-values
    content: Filter out zero-accuracy non-baseline iterations in buildChartData()
    status: pending
  - id: fix-stage-timeline
    content: In StageTimeline.tsx, compute duration from timestamps when durationSeconds is null
    status: pending
  - id: fix-step4-prefixes
    content: Add 'AG_' to step 4 stage_prefixes in runs.py _STEP_DEFINITIONS
    status: pending
  - id: fix-step4-io
    content: Enrich step 4 _build_step_io with fallback data from run_data and patches_rows
    status: pending
  - id: add-step1-insights
    content: Add StepInsights rendering for step 1 (tables, functions, instructions, sample questions)
    status: pending
  - id: add-step4-insights
    content: Add StepInsights rendering for step 4 (patches, levers accepted/rolled back, accuracy delta)
    status: pending
  - id: add-step5-insights
    content: Add StepInsights rendering for step 5 (best accuracy, repeatability, convergence reason)
    status: pending
  - id: verify
    content: Run apx dev check, verify all three fixes render correctly
    status: pending
isProject: false
---

# Fix: Joins, Score Progression, and Pipeline Step Details

## Issue 1: Referenced Joins Empty

**Root cause:** `[spaces.py](src/genie_space_optimizer/backend/routes/spaces.py)` line 506 reads `join_specs` from `data_sources`, but according to the Genie schema they live under `instructions`.

```python
# Current (wrong):
joins_raw = ds.get("join_specs", [])

# Fix -- read from instructions (already extracted at line 551 as `instr`):
```

**Fix:** Move `instr` extraction (line 551) to **before** the joins block (line 506), then change `joins_raw` to read from `instr` first with `ds` as fallback:

```python
instr = ss.get("instructions", {}) if isinstance(ss, dict) else {}
joins_raw = (instr.get("join_specs", []) if isinstance(instr, dict) else []) or (
    ds.get("join_specs", []) if isinstance(ds, dict) else []
)
```

Remove the duplicate `instr = ...` at old line 551 since it's now moved up.

---

## Issue 2: Score Progression Chart

Two sub-problems in `[IterationChart.tsx](src/genie_space_optimizer/ui/components/IterationChart.tsx)` and `[harness.py](src/genie_space_optimizer/optimization/harness.py)`:

### 2a. X-axis: all labels say "Lever 0"

**Root cause:** `[harness.py](src/genie_space_optimizer/optimization/harness.py)` line 1775 hardcodes `lever=0` for every post-baseline full eval:

```python
write_iteration(spark, run_id, iteration_counter, full_result,
    catalog=catalog, schema=schema,
    lever=0, eval_scope="full", ...)  # <-- always 0
```

The function `_run_action_group` receives `ag_id` (e.g. `"AG1"`) but the action group's lever numbers are in `lever_keys` (from `ag.get("lever_directives", {}).keys()`). The lever numbers are not scalar -- action groups can span multiple levers.

**Fix (frontend, simpler and backward-compatible):** In `[IterationChart.tsx](src/genie_space_optimizer/ui/components/IterationChart.tsx)` `buildChartData()`, change the label logic so when `lever === 0` and `iteration > 0`, it falls back to `"Iteration N"` instead of `"Lever 0"`:

```typescript
leverLabel:
  it.iteration === 0
    ? "Baseline"
    : it.lever != null && it.lever > 0
      ? `Lever ${it.lever}`
      : `Iteration ${it.iteration}`,
```

### 2b. Y-axis: values near 0% except one at ~100%

This likely comes from non-`full` eval scope rows leaking in, or from data where `overall_accuracy` is stored as 0 for incomplete iterations. The chart already filters to `evalScope === "full"`, and the backend already normalizes to 0-100.

**Fix:** Add a defensive filter in `buildChartData()` to skip iterations where `overallAccuracy === 0` but `iteration > 0` (these are likely placeholder rows from interrupted runs):

```typescript
const full = iterations
  .filter((it) => it.evalScope === "full")
  .filter((it) => it.iteration === 0 || it.overallAccuracy > 0)
  .sort((a, b) => a.iteration - b.iteration);
```

### 2c. Stage Timeline not rendering

The `StageTimeline` filters out events with `durationSeconds == null || durationSeconds <= 0`. If stages aren't getting their `COMPLETE` row written (so `duration_seconds` stays `NULL`), no bars show.

**Fix:** In `[StageTimeline.tsx](src/genie_space_optimizer/ui/components/StageTimeline.tsx)`, relax the filter to also include events where duration is null but both timestamps exist (compute duration client-side):

```typescript
const chartData = stageEvents
  .map((e) => {
    let duration = e.durationSeconds;
    if ((duration == null || duration <= 0) && e.startedAt && e.completedAt) {
      duration = (new Date(e.completedAt).getTime() - new Date(e.startedAt).getTime()) / 1000;
    }
    return { ...e, durationSeconds: duration ?? 0, shortStage: ... };
  })
  .filter((e) => e.durationSeconds > 0);
```

---

## Issue 3: Pipeline Step Details Comprehensive Fix

### 3a. Add `StepInsights` for steps 1, 4, and 5

In `[$runId.tsx](src/genie_space_optimizer/ui/routes/runs/$runId.tsx)`, the `StepInsights` function (lines 593-735) returns `null` for steps 1, 4, and 5. Add rendering blocks for each.

**Step 1 (Configuration Analysis)** -- backend already provides `tableCount`, `tables`, `functionCount`, `instructionCount`, `sampleQuestionCount`, `sampleQuestionsPreview`:

- Show badges: Tables count, Functions count, Instructions count, Sample questions count
- Show list preview of table names and sample questions

**Step 4 (Configuration Generation)** -- backend provides `patchesApplied`, `leversAccepted`, `leversRolledBack`, `iterationCounter`:

- Show badges: Patches applied, Levers accepted, Levers rolled back, Iterations
- Note: the existing `LeverProgress` component already renders lever detail below step 4 (line 505-508), so this adds a summary header

**Step 5 (Optimized Evaluation)** -- backend provides `bestAccuracy`, `repeatability`, `convergenceReason`:

- Show badges: Best accuracy, Repeatability, Convergence reason

### 3b. Fix Step 4 stage prefix mismatch

In `[runs.py](src/genie_space_optimizer/backend/routes/runs.py)`, step 4's `stage_prefixes` is `["LEVER_"]` (line 268). The harness writes action-group stages as `AG_X_`* (e.g. `AG_1_FULL_EVAL`). These stages never match.

**Fix:** Add `"AG_"` to step 4's stage prefixes:

```python
{
    "stepNumber": 4,
    "name": "Configuration Generation",
    "stage_prefixes": ["LEVER_", "AG_"],
    ...
},
```

### 3c. Enrich Step 4 `_build_step_io` with data from `run_data`

In `[runs.py](src/genie_space_optimizer/backend/routes/runs.py)` `_build_step_io` step 4 block (lines 628-643), `patchesApplied` and `iterationCounter` come from `detail_json` of `LEVER_LOOP_STARTED` which may not have them. Fall back to `run_data`:

```python
if step_num == 4:
    patches_applied = detail.get("patches_applied")
    if patches_applied is None:
        patches_applied = detail.get("patches_count")
    iteration_counter = detail.get("iteration_counter")
    if iteration_counter is None:
        iteration_counter = run_data.get("best_iteration")

    levers_accepted = detail.get("levers_accepted", [])
    levers_rolled_back = detail.get("levers_rolled_back", [])

    return (
        { ... },
        {
            "patchesApplied": patches_applied,
            "leversAccepted": levers_accepted,
            "leversRolledBack": levers_rolled_back,
            "iterationCounter": iteration_counter,
            "baselineAccuracy": run_data.get("baseline_accuracy"),
            "bestAccuracy": _safe_float(run_data.get("best_accuracy")),
            "stageEvents": timeline,
        },
    )
```

