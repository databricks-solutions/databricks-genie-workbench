---
name: Fix Eval Dedup Slots TVF
overview: "Fix three issues: (1) evaluations still run 54 rows because run_evaluation prefers the un-pruned MLflow dataset over the deduped DataFrame; (2) instruction slot counting omits table/MV descriptions causing 110 vs 100 mismatch with the Genie API; (3) TVF removal confidence scoring is implemented but no remove_tvf patch is ever constructed for high/medium confidence tiers."
todos:
  - id: eval-dedup
    content: Fix run_evaluation to use deduped eval_data DataFrame instead of _eval_dataset_obj backed by 54-row Delta table
    status: completed
  - id: preflight-drop
    content: In preflight, always sync Delta table to match deduped benchmarks (not just on GENERATE path)
    status: completed
  - id: slot-count
    content: Update count_instruction_slots to include table/MV descriptions matching Genie API counting
    status: completed
  - id: slot-trim
    content: Update trimming logic in applier to account for table/MV descriptions and trim more aggressively
    status: completed
  - id: tvf-patch
    content: "Wire TVF removal: construct and apply synthetic remove_tvf patch when _handle_escalation returns auto_apply or apply_and_flag"
    status: completed
  - id: tvf-detail
    content: Ensure _handle_escalation stores tvf_id and previous_tvf_asset in result dict for patch construction
    status: completed
  - id: unit-tests
    content: Add unit tests for eval dedup, slot counting with descriptions, and TVF removal patch construction
    status: completed
isProject: false
---

# Fix Evaluation Dedup, Instruction Slot Budget, and TVF Removal

## Why these are still broken (investigation results)

### Issue 1: Evaluations still run 54 rows

The read-side dedup and write-side dedup we added previously work correctly at the **benchmark loading** level. The disconnect is in `run_evaluation` itself:

- In [`evaluation.py` ~line 2848-2859](src/genie_space_optimizer/optimization/evaluation.py), `run_evaluation` loads the MLflow `EvaluationDataset` object (`_eval_dataset_obj`) from the Delta table
- At [~line 2972](src/genie_space_optimizer/optimization/evaluation.py), it prefers `_eval_dataset_obj` over the deduped `eval_data` DataFrame:
  ```python
  evaluate_kwargs = {
      "predict_fn": predict_fn,
      "data": _eval_dataset_obj if _eval_dataset_obj is not None else eval_data,
      ...
  }
  ```
- The Delta table still has 54 rows because `_drop_benchmark_table` only runs when `regenerated=True` (GENERATE/RE-GENERATE paths in preflight). On the REUSE path, `merge_records` upserts but never deletes the 24 extra rows.

**Fix:** Always use the deduped `eval_data` DataFrame for evaluation. Optionally retain `_eval_dataset_obj` reference for MLflow dataset linking metadata only.

### Issue 2: Instruction slot budget mismatch

`count_instruction_slots` in [`genie_schema.py` lines 452-466](src/genie_space_optimizer/common/genie_schema.py) only counts:
- `text_instructions` (capped at 1)
- `example_question_sqls` (each = 1)
- `sql_functions` (each = 1)

But the Genie API error says: *"Total instructions **(including table/metric descriptions)** is 110; max per space is 100"*. The API counts table and metric view descriptions toward the 100-slot limit. With 10 tables + 2 metric views, the API sees ~10 extra slots our code doesn't account for.

The trimming logic in [`applier.py` lines 1507-1518](src/genie_space_optimizer/optimization/applier.py) trims to 100 by our count, but the API still sees 110.

**Fix:** Update `count_instruction_slots` to include table/MV description contributions, matching the Genie API's actual counting formula.

### Issue 3: TVF removal never applied

The full tiered confidence model is implemented:
- `_score_tvf_removal_confidence` in [`harness.py` lines 1935-2034](src/genie_space_optimizer/optimization/harness.py) -- works correctly
- `_handle_escalation` in [`harness.py` lines 2037-2164](src/genie_space_optimizer/optimization/harness.py) -- sets `tier_action` correctly
- Applier at [`applier.py` lines 971-975](src/genie_space_optimizer/optimization/applier.py) can apply `remove_tvf` patches (removes from `sql_functions`)

The gap is in the **lever loop dispatch** at [`harness.py` lines 3386-3416](src/genie_space_optimizer/optimization/harness.py):
- For `flagged_only` -> `continue` (skip AG) -- correct
- For `gt_repair` -> `continue` -- correct
- For `auto_apply` / `apply_and_flag` -> falls through to `generate_proposals_from_strategy` at line 3416
- But `generate_proposals_from_strategy` for lever 3 only produces `update_function_description`, never `remove_tvf`
- So the TVF is never actually removed despite the confidence model approving it

**Fix:** After `_handle_escalation` returns `auto_apply` or `apply_and_flag` for `remove_tvf`, construct a synthetic `remove_tvf` patch and inject it into the patch set before `apply_patch_set` is called.

---

## Implementation Plan

### Fix 1: Evaluation uses deduped benchmarks

**File:** `src/genie_space_optimizer/optimization/evaluation.py`

In `run_evaluation` (~line 2970):
- Change the `evaluate_kwargs["data"]` to always use `eval_data` (the deduped DataFrame)
- If `_eval_dataset_obj` is available, pass it via the `evaluation_dataset` parameter (for MLflow experiment linking) instead of as `data`
- If `mlflow.genai.evaluate` does not support both `data` and `evaluation_dataset` simultaneously, just use `eval_data` and drop the dataset linking (runtime is more important than UI linkage)

Also in `preflight.py` (~line 510), consider always calling `_drop_benchmark_table` + `create_evaluation_dataset` (not just on GENERATE path) to keep the Delta table in sync. This is a secondary safety net.

### Fix 2: Instruction slot counting includes table/MV descriptions

**File:** `src/genie_space_optimizer/common/genie_schema.py`

Update `count_instruction_slots` to also count:
- Each table in `data_sources.tables` that has a non-empty `description` = +1 slot
- Each metric view in `data_sources.metric_views` that has a non-empty `description` = +1 slot
- Each column with a non-empty `description` within any table/MV may also count -- need to validate against API behavior

The exact formula may need empirical validation. A conservative approach: count all tables + metric views that have descriptions, since the API message says "including table/metric descriptions."

Also update the trimming logic in `applier.py` to:
- Account for the higher slot count
- Trim more aggressively (potentially removing `sql_functions` entries for TVFs that are being blamed, not just `example_question_sqls`)
- Add a pre-flight budget check at the start of each iteration that refuses to add new instructions if the budget is already at or near capacity

### Fix 3: Wire TVF removal into patch application

**File:** `src/genie_space_optimizer/optimization/harness.py`

After the escalation dispatch block (~line 3386), add a new block for `remove_tvf` with `auto_apply` or `apply_and_flag`:

```python
if _escalation == "remove_tvf" and _esc_tier in ("auto_apply", "apply_and_flag"):
    tvf_id = _esc_result.get("detail", {}).get("tvf_id", "")
    if tvf_id:
        synthetic_patch = {
            "patch_type": "remove_tvf",
            "target": tvf_id,
            "rationale": f"TVF {tvf_id} auto-removed (confidence: {_esc_tier})",
            "previous_tvf_asset": _esc_result.get("detail", {}).get("previous_tvf_asset", {}),
        }
        # Inject into the patch pipeline
        # ... apply via apply_patch_set or direct config mutation
    # For apply_and_flag, also skip normal proposal generation
    # and proceed directly to evaluation
```

Also ensure `_handle_escalation` stores the `tvf_id` and `previous_tvf_asset` in the result dict so the harness can construct the patch.

### Fix 4: Unit tests

- Test that `run_evaluation` uses `eval_data` row count (not dataset table row count)
- Test `count_instruction_slots` with table/MV descriptions
- Test TVF removal patch construction and application for high/medium confidence
