---
name: Fix Labeling Sessions
overview: Fix the MLflow labeling session pipeline so sessions are created with traces and visible in the experiment UI, and add print-level error reporting so failures are diagnosable.
todos:
  - id: delete-create-or-reuse
    content: Delete the `_create_or_reuse_schema` function (lines 38-75 of labeling.py)
    status: completed
  - id: rewrite-ensure-schemas
    content: Rewrite `ensure_labeling_schemas` to use `create_label_schema(..., overwrite=True)` directly with per-schema try/except and print() output
    status: completed
  - id: add-set-experiment
    content: Add `mlflow.set_experiment(experiment_name)` to `create_review_session` before session creation, plus print() at each failure point
    status: completed
  - id: add-prints-populate
    content: Add print() statements in `_populate_session_traces` at warning/error paths
    status: completed
  - id: fix-add-traces-per-run
    content: Refactor _populate_session_traces to call session.add_traces() separately for priority and backfill batches instead of re-concatenating
    status: completed
  - id: add-prints-harness
    content: Add print() with exception details at both harness.py call sites (baseline ~line 435 and lever loop ~line 3798)
    status: completed
isProject: false
---

# Fix MLflow Labeling Session Creation

## Problem

Labeling sessions are never created (or created in the wrong experiment) after lever loop runs, despite traces existing and being linked to evaluation runs. The root cause is a combination of: (1) no explicit `mlflow.set_experiment()` call before session/schema creation, (2) fragile schema creation using `overwrite=False` with error-message parsing, and (3) silent exception swallowing at every level.

## Files to Modify

- [labeling.py](src/genie_space_optimizer/optimization/labeling.py) -- main changes
- [harness.py](src/genie_space_optimizer/optimization/harness.py) -- add print-level error reporting at call sites

## Changes

### 1. Delete `_create_or_reuse_schema` (lines 38-75)

Remove the entire function. It will be replaced by direct `create_label_schema(..., overwrite=True)` calls inside `ensure_labeling_schemas`.

### 2. Rewrite `ensure_labeling_schemas` (lines 78-157)

Replace the four `_create_or_reuse_schema(...)` calls with direct `schemas.create_label_schema(..., overwrite=True)` calls. Each schema creation gets its own try/except that prints the error AND logs it, but does not prevent the other schemas from being created.

The new structure:

```python
def ensure_labeling_schemas() -> list[str]:
    try:
        import mlflow.genai.label_schemas as schemas
        from mlflow.genai.label_schemas import (
            InputCategorical,
            InputText,
            InputTextList,
        )
    except ImportError:
        print("[Labeling] mlflow.genai.label_schemas not available — skipping")
        return []

    available: list[str] = []
    _schema_defs = [
        {
            "name": SCHEMA_JUDGE_VERDICT,
            "type": "feedback",
            "title": "Is the judge's verdict correct for this question?",
            "input": InputCategorical(options=[
                "Correct - judge is right",
                "Wrong - Genie answer is actually fine",
                "Wrong - both answers are wrong",
                "Ambiguous - question is unclear",
            ]),
            "instruction": (
                "Review the benchmark question, expected SQL, Genie-generated SQL, "
                "and each judge's rationale. Was the overall verdict correct?"
            ),
            "enable_comment": True,
        },
        {
            "name": SCHEMA_CORRECTED_SQL,
            "type": "expectation",
            "title": "Provide the correct expected SQL (if benchmark is wrong)",
            "input": InputText(),
            "instruction": (
                "If the benchmark's expected SQL is incorrect or suboptimal, "
                "provide the corrected SQL. Leave blank if the benchmark is correct."
            ),
        },
        {
            "name": SCHEMA_PATCH_APPROVAL,
            "type": "feedback",
            "title": "Should the proposed optimization patch be applied?",
            "input": InputCategorical(options=["Approve", "Reject", "Modify"]),
            "instruction": (
                "Review the proposed metadata change (column description, instruction, "
                "join condition, etc.). Approve it, reject it, or suggest modifications."
            ),
            "enable_comment": True,
        },
        {
            "name": SCHEMA_IMPROVEMENTS,
            "type": "expectation",
            "title": "Suggest improvements for this Genie Space",
            "input": InputTextList(max_count=5, max_length_each=500),
            "instruction": (
                "Provide specific, actionable improvements: better column descriptions, "
                "missing join conditions, instruction changes, or table-level fixes."
            ),
        },
    ]

    for defn in _schema_defs:
        name = defn["name"]
        try:
            schemas.create_label_schema(**defn, overwrite=True)
            available.append(name)
        except Exception as exc:
            print(f"[Labeling] Failed to create schema '{name}': {exc}")
            logger.warning("Failed to create label schema '%s': %s", name, exc)

    print(f"[Labeling] Ensured {len(available)}/{len(_schema_defs)} schemas: {', '.join(available)}")
    return available
```

Key differences from current code:

- Uses `overwrite=True` -- no need to check existence first
- Eliminates the 38-line `_create_or_reuse_schema` helper
- Each schema fails independently (one failure doesn't block others)
- Prints visible output on failure AND success
- Same schema definitions (names, types, inputs, instructions) -- no functional change to what reviewers see

### 3. Add `mlflow.set_experiment()` to `create_review_session` (line ~189)

After the `ensure_labeling_schemas()` call and before `labeling.create_labeling_session()`, add:

```python
mlflow.set_experiment(experiment_name)
```

This ensures the session is created in the same experiment where traces live. The `experiment_name` parameter is already passed to this function but never used for this purpose.

Add it right before the session creation (around line 203). Also add print statements at key failure points:

```python
def create_review_session(
    run_id, domain, experiment_name, uc_schema,
    failure_trace_ids, regression_trace_ids,
    reviewers=None, eval_mlflow_run_ids=None,
) -> dict[str, Any]:
    try:
        import mlflow.genai.labeling as labeling
    except ImportError:
        print("[Labeling] mlflow.genai.labeling not available — skipping session creation")
        return {}

    schema_names = ensure_labeling_schemas()
    if not schema_names:
        print("[Labeling] No schemas available — cannot create labeling session")
        return {}

    mlflow.set_experiment(experiment_name)    # <-- NEW

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_name = f"genie_opt_review_{domain}_{run_id[:8]}_{ts}"

    print(
        f"[Labeling] Creating session '{session_name}' in experiment '{experiment_name}' "
        f"({len(failure_trace_ids)} failures, {len(regression_trace_ids)} regressions, "
        f"{len(eval_mlflow_run_ids or [])} eval runs)"
    )

    try:
        session = labeling.create_labeling_session(
            name=session_name,
            label_schemas=schema_names,
            assigned_users=reviewers or [],
        )
    except Exception as exc:
        print(f"[Labeling] Failed to create session: {exc}")
        logger.exception("Failed to create labeling session")
        return {}

    # ... rest unchanged, but add print to _populate_session_traces errors too
```

### 4. Refactor `_populate_session_traces` — per-batch `add_traces` and print reporting

Two changes to `_populate_session_traces`:

**4a. Call `session.add_traces()` per batch instead of re-concatenating**

Currently (lines 319-330), priority and backfill traces are re-concatenated into a `combined` DataFrame with `pd.concat([priority_traces, backfill], ignore_index=True)`, then passed as one `add_traces` call. This second concat can alter the DataFrame structure MLflow expects.

Instead, call `add_traces` separately for each batch:

```python
traces_added = 0

if len(priority_traces) > 0:
    session.add_traces(priority_traces.head(_MAX_SESSION_TRACES))
    traces_added += min(len(priority_traces), _MAX_SESSION_TRACES)

remaining = _MAX_SESSION_TRACES - traces_added
if remaining > 0 and len(other_traces) > 0:
    backfill = other_traces.head(remaining)
    session.add_traces(backfill)
    traces_added += len(backfill)
```

This preserves the priority ordering (failures first, then backfill) and the 100-trace cap, but avoids the second `pd.concat`. The first concat (collecting traces from multiple eval runs) is kept — it's necessary for deduplication and priority filtering across runs.

**4b. Add print statements at key points**

- When `search_traces(run_id=rid)` fails: print the error
- When falling back to experiment-wide search: print
- When no traces found: print
- When traces are added successfully: print count per batch

### 5. Add print-level error reporting at harness call sites

In `harness.py`, at both call sites (line ~435 and ~3798), change the `except Exception` blocks to also print the error:

**Baseline call site (~line 435):**

```python
except Exception as exc:
    print(f"[Labeling] Failed to create baseline labeling session: {exc}")
    logger.warning("Failed to create baseline labeling session", exc_info=True)
```

**Lever loop call site (~line 3798):**

```python
except Exception as exc:
    print(f"[Labeling] Failed to create labeling session: {exc}")
    logger.warning("Failed to create labeling session", exc_info=True)
```

## What Is NOT Changing

These items are explicitly preserved to avoid regressions:

- **Schema definitions** -- same names, types, titles, inputs, instructions, enable_comment values
- `**_populate_session_traces` priority logic** -- same priority order (failures first, regressions second, backfill), same `_MAX_SESSION_TRACES = 100` cap, same initial collection via `search_traces(run_id=...)` with concat+dedupe. Only the final `add_traces` call pattern changes (per-batch instead of re-concat).
- `**ingest_human_feedback`** -- no changes (reads from sessions)
- `**sync_corrections_to_dataset`** -- no changes
- `**_find_session_by_name`** -- no changes
- `**flag_for_human_review` / `get_flagged_questions**` -- no changes
- **Preflight feedback ingestion** (`preflight.py` lines 714-743) -- no changes; `ensure_labeling_schemas()` still called with same signature (no args)
- **Harness call site signatures** -- same parameters passed to `create_review_session`
- `**update_run_status` calls** -- same fields written (`labeling_session_name`, `labeling_session_run_id`, `labeling_session_url`)

## Callers of Modified Functions


| Function                  | Callers                                                               | Impact                                                                                                    |
| ------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `_create_or_reuse_schema` | Only `ensure_labeling_schemas` in labeling.py                         | Safe to delete -- no external callers                                                                     |
| `ensure_labeling_schemas` | `create_review_session` (labeling.py), `preflight.py` line 721        | Signature unchanged (no params). Return type unchanged (list of str). Behavior improved (overwrite=True). |
| `create_review_session`   | `harness.py` line 418 (baseline), `harness.py` line 3771 (lever loop) | Signature unchanged. Now calls `mlflow.set_experiment()` internally.                                      |


## Verification

After implementation, run the notebook test described in the prior conversation:

```python
import mlflow
import mlflow.genai.label_schemas as schemas
import mlflow.genai.labeling as labeling
from mlflow.genai.label_schemas import InputCategorical

mlflow.set_experiment("<your-experiment-path>")
s = schemas.create_label_schema(
    name="test_verify", type="feedback",
    title="Test", input=InputCategorical(options=["A", "B"]),
    overwrite=True,
)
session = labeling.create_labeling_session(
    name="test_verify_session", label_schemas=["test_verify"],
)
traces = mlflow.search_traces(max_results=10)
session.add_traces(traces)
print(f"Session URL: {session.url}")
```

