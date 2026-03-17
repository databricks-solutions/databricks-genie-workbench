---
name: Skip Slice Gate and Conditional Confirm
overview: Add an `ENABLE_SLICE_GATE` flag (default `False`) to skip the slice gate evaluation, with a 50% reduction threshold when enabled. Also skip the confirmation eval when accuracy improved over best.
todos:
  - id: config-flag
    content: Add ENABLE_SLICE_GATE = False and SLICE_GATE_MIN_REDUCTION = 0.5 to config.py next to SLICE_GATE_TOLERANCE
    status: completed
  - id: slice-gate-conditional
    content: "Make slice gate conditional in _run_gate_checks: skip when flag is False, skip when slice >= 50% of full set"
    status: completed
  - id: confirm-eval-conditional
    content: Skip confirmation eval in _run_gate_checks when accuracy_1 > best_accuracy
    status: completed
  - id: verify
    content: Run unit tests to verify no regressions
    status: completed
isProject: false
---

# Skip Slice Gate and Conditional Confirmation Eval

## Changes

### 1. Add `ENABLE_SLICE_GATE` flag in config

**File:** [src/genie_space_optimizer/common/config.py](src/genie_space_optimizer/common/config.py)

Add next to the existing `SLICE_GATE_TOLERANCE` (line 79):

```python
ENABLE_SLICE_GATE: bool = False
SLICE_GATE_MIN_REDUCTION = 0.5  # only run slice if it reduces benchmarks by at least 50%
```

### 2. Make slice gate conditional in `_run_gate_checks`

**File:** [src/genie_space_optimizer/optimization/harness.py](src/genie_space_optimizer/optimization/harness.py)

**Import:** Add `ENABLE_SLICE_GATE` and `SLICE_GATE_MIN_REDUCTION` to the existing import block (line 34-53).

**Slice gate block (lines 2777-2851):** Wrap the existing slice gate logic with two conditions:

- If `ENABLE_SLICE_GATE` is `False` -> skip entirely, print log line
- If `True` but `len(slice_benchmarks) > (1 - SLICE_GATE_MIN_REDUCTION) * len(benchmarks)` -> skip (slice is not meaningfully smaller), print log line
- Otherwise -> run existing slice gate logic unchanged

The replacement structure for lines 2777-2851:

```python
# -- Slice gate --
_run_slice = False
if ENABLE_SLICE_GATE:
    affected_qids: set[str] = affected_question_ids or set()
    slice_benchmarks = filter_benchmarks_by_scope(
        benchmarks, "slice", patched_objects,
        affected_question_ids=affected_qids,
    )
    _total = len(benchmarks)
    _sliced = len(slice_benchmarks) if slice_benchmarks else 0
    if slice_benchmarks and _sliced <= (1 - SLICE_GATE_MIN_REDUCTION) * _total:
        _run_slice = True
    else:
        print(
            _section(f"SLICE GATE [{ag_id}]: SKIPPED", "-") + "\n"
            + _kv("Reason", f"slice too broad ({_sliced}/{_total} benchmarks)") + "\n"
            + _bar("-")
        )
else:
    print(
        _section(f"SLICE GATE [{ag_id}]: DISABLED", "-") + "\n"
        + _bar("-")
    )

if _run_slice:
    # ... existing slice gate evaluation logic (lines 2788-2851) stays here unchanged ...
```

### 3. Make confirmation eval conditional on accuracy improvement

**File:** [src/genie_space_optimizer/optimization/harness.py](src/genie_space_optimizer/optimization/harness.py)

**Confirmation eval block (lines 2915-2944):** After `full_result_1` is obtained and `accuracy_1` is computed (line 2913), check if accuracy improved. If so, skip the 2nd eval and use `full_result_1` directly.

Replace lines 2915-2944 with:

```python
if accuracy_1 > best_accuracy:
    print(_kv("Confirmation eval", f"SKIPPED (accuracy improved {best_accuracy:.1f}% -> {accuracy_1:.1f}%)"))
    full_scores = scores_1
    full_accuracy = accuracy_1
    full_result = full_result_1
else:
    # existing confirmation eval code (lines 2916-2944)
    try:
        mlflow.end_run()
    except Exception:
        pass
    print(_kv("Confirmation eval", "running 2nd evaluation to average out variance"))
    _ensure_sql_context(spark, catalog, schema)
    full_result_2 = run_evaluation(...)  # same args as current
    scores_2 = full_result_2.get("scores", {})
    accuracy_2 = full_result_2.get("overall_accuracy", 0.0)

    all_judge_keys = set(scores_1) | set(scores_2)
    full_scores = {
        j: (scores_1.get(j, 0.0) + scores_2.get(j, 0.0)) / 2.0
        for j in all_judge_keys
    }
    full_accuracy = (accuracy_1 + accuracy_2) / 2.0
    full_result = full_result_1

    print(
        _kv("Eval run 1 accuracy", f"{accuracy_1:.1f}%") + "\n"
        + _kv("Eval run 2 accuracy", f"{accuracy_2:.1f}%") + "\n"
        + _kv("Averaged accuracy", f"{full_accuracy:.1f}%")
    )
```

The rest of the function (write_iteration, detect_regressions, etc.) already uses `full_scores`, `full_accuracy`, and `full_result` — no further changes needed downstream.

## Expected time savings

Per iteration (with 30 deduped benchmarks at ~8 min per eval):
- Slice gate disabled: saves ~8 min
- Confirmation eval skipped when improving: saves ~8 min on improving iterations
- Combined: ~16 min saved per improving iteration, ~8 min on non-improving

Over a 5-iteration run: **~50-60 minutes saved**, bringing ~2 hours down to ~1 hour.
