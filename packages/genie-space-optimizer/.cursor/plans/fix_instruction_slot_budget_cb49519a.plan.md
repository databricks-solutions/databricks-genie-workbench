---
name: Fix instruction slot budget
overview: Fix the instruction slot budget calculation to include table and metric view descriptions, which the Genie API counts toward the 100-slot limit but the optimizer currently ignores. This causes PATCH failures when the combined count (instructions + descriptions) exceeds 100, and over-allocates example SQL / sql_function slots.
todos:
  - id: fix-count-fn
    content: Update count_instruction_slots() in genie_schema.py to count table/metric view descriptions
    status: pending
  - id: fix-strict-validate
    content: Update _strict_validate() error message to include description count breakdown
    status: pending
  - id: harden-trimming
    content: Harden apply_patch_set() trimming to handle description-heavy configs (fallback to sql_functions, warn on unrecoverable)
    status: pending
  - id: cap-mining
    content: Add slot-budget cap to _mine_benchmark_example_sqls so it stops mining once remaining slots are exhausted
    status: pending
  - id: add-unit-tests
    content: Add TestCountInstructionSlots tests in test_genie_schema.py and budget-cap test in test_optimizer.py
    status: pending
  - id: verify-reference-config
    content: Run existing tests to verify reference config and all fixtures still pass with the new counting logic
    status: pending
isProject: false
---

# Fix Instruction Slot Budget to Include Table/Metric Descriptions

## Problem

The Genie API counts table descriptions and metric view descriptions toward the 100-instruction-slot limit, but `count_instruction_slots()` only counts `text_instructions`, `example_question_sqls`, and `sql_functions`. This mismatch causes:

- Local validation passes (e.g., code sees 94/100 slots)
- API rejects with "Total instructions (including table/metric descriptions) is 106; max per space is 100"

The undercount cascades to **all** budget-aware code paths:

1. **Proposal validation** (`_validate_lever5_proposals` line 5537): `remaining_budget` is overstated, so too many `add_example_sql` and `add_instruction` proposals are accepted
2. **Benchmark mining** (`_mine_benchmark_example_sqls` line 5659): has no budget check at all -- mines unlimited proposals that are later trimmed or rejected
3. **Post-apply trimming** (`apply_patch_set` line 1507): trims too few `example_question_sqls` because it underestimates the slot count
4. **Structural validation** (`_strict_validate` line 628): misses the overrun entirely

## Change 1: Fix `count_instruction_slots()` in [genie_schema.py](src/genie_space_optimizer/common/genie_schema.py)

**Lines 452-466** -- Add counting of non-empty `description` fields from `data_sources.tables[]` and `data_sources.metric_views[]`.

```python
def count_instruction_slots(config: dict) -> int:
    instructions = config.get("instructions") or {}
    text_count = min(len(instructions.get("text_instructions") or []), 1)
    example_count = len(instructions.get("example_question_sqls") or [])
    function_count = len(instructions.get("sql_functions") or [])

    desc_count = 0
    ds = config.get("data_sources") or {}
    if isinstance(ds, dict):
        for source_key in ("tables", "metric_views"):
            for entry in ds.get(source_key) or []:
                if not isinstance(entry, dict):
                    continue
                d = entry.get("description")
                if d and (isinstance(d, str) or (isinstance(d, list) and any(d))):
                    desc_count += 1

    return text_count + example_count + function_count + desc_count
```

Update docstring to document that table/metric descriptions each consume 1 slot.

**Why this is safe:** All call sites pass dicts with `data_sources` when descriptions exist:

- `apply_patch_set` passes the full Genie config
- `_strict_validate` passes the full Genie config
- `_validate_lever5_proposals` passes `metadata_snapshot` which uses `data_sources.tables`
- `_mine_benchmark_example_sqls` passes `metadata_snapshot` (same)

Test fixtures in `test_optimizer.py` (`_SNAP`, `_METADATA`) have no `data_sources` key, so `desc_count` will be 0 -- existing tests unaffected.

**Downstream auto-fix:** Once `count_instruction_slots()` returns the correct (higher) count, `_validate_lever5_proposals` (line 5537-5538) automatically computes a smaller `remaining_budget`, so fewer `add_example_sql` proposals are accepted per iteration. No code changes needed in that function.

## Change 2: Improve error message in `_strict_validate()` in [genie_schema.py](src/genie_space_optimizer/common/genie_schema.py)

**Lines 627-637** -- Add `desc_count` to the error breakdown so the validation failure message shows where slots are consumed:

```python
slot_count = count_instruction_slots(config)
if slot_count > MAX_INSTRUCTION_SLOTS:
    _text_ct = min(len(ti_list), 1)
    _eq_ct = len(inst.get("example_question_sqls") or [])
    _fn_ct = len(inst.get("sql_functions") or [])
    _desc_ct = slot_count - _text_ct - _eq_ct - _fn_ct
    errors.append(
        f"Instruction slot budget exceeded: {slot_count}/{MAX_INSTRUCTION_SLOTS} "
        f"(example_sqls={_eq_ct}, sql_functions={_fn_ct}, "
        f"text_instructions={_text_ct}, table/mv_descriptions={_desc_ct})"
    )
```

## Change 3: Harden trimming in `apply_patch_set()` in [applier.py](src/genie_space_optimizer/optimization/applier.py)

**Lines 1507-1517** -- The current trimming only removes `example_question_sqls` and silently does nothing if there aren't enough to cover the excess (e.g., when descriptions consume most of the budget). Add a cascading fallback:

- Trim `example_question_sqls` first (lowest priority instruction type)
- If still over, trim `sql_functions` from the end
- If STILL over (descriptions alone exceed the limit), log a clear error -- this is an unrecoverable state that requires manual intervention

```python
slot_count = count_instruction_slots(config)
if slot_count > MAX_INSTRUCTION_SLOTS:
    excess = slot_count - MAX_INSTRUCTION_SLOTS
    logger.warning(
        "Post-apply config exceeds instruction slot budget (%d/%d) — trimming",
        slot_count, MAX_INSTRUCTION_SLOTS,
    )
    # 1. Trim example_question_sqls first (lowest priority)
    eqs = (config.get("instructions") or {}).get("example_question_sqls", [])
    trim_eqs = min(len(eqs), excess)
    if trim_eqs > 0:
        config["instructions"]["example_question_sqls"] = eqs[:-trim_eqs]
        excess -= trim_eqs

    # 2. Trim sql_functions if still over budget
    if excess > 0:
        fns = (config.get("instructions") or {}).get("sql_functions", [])
        trim_fns = min(len(fns), excess)
        if trim_fns > 0:
            config["instructions"]["sql_functions"] = fns[:-trim_fns]
            excess -= trim_fns

    # 3. Warn if descriptions alone exceed budget (cannot auto-fix)
    if excess > 0:
        logger.error(
            "Cannot trim enough instruction slots — %d excess slots from "
            "table/metric descriptions alone. Manual description cleanup required.",
            excess,
        )
```

## Change 4: Add slot-budget cap to `_mine_benchmark_example_sqls()` in [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

**Lines 5659-5777** -- Currently mines unlimited proposals with no budget awareness. The proposals are later filtered by `_validate_lever5_proposals` or trimmed by `apply_patch_set`, but when applied proactively via `_apply_proactive_example_sqls` (harness.py line 1243), they bypass `_validate_lever5_proposals` entirely and go straight to `apply_patch_set`.

Add an early exit once the slot budget is exhausted:

```python
def _mine_benchmark_example_sqls(
    benchmarks: list[dict],
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
) -> list[dict]:
    from genie_space_optimizer.common.genie_schema import count_instruction_slots, MAX_INSTRUCTION_SLOTS

    current_slots = count_instruction_slots(metadata_snapshot)
    remaining_budget = MAX_INSTRUCTION_SLOTS - current_slots

    # ... existing dedup setup ...

    proposals: list[dict] = []
    # ... existing loop ...
        # After passing all checks, before appending:
        if len(proposals) >= remaining_budget:
            logger.info(
                "Benchmark mining: stopping — slot budget exhausted (%d/%d used)",
                current_slots + len(proposals), MAX_INSTRUCTION_SLOTS,
            )
            break

        proposals.append({...})
```

This prevents generating doomed proposals that waste validation/execution time, and critically prevents the proactive application path (which bypasses `_validate_lever5_proposals`) from overshooting the budget.

## Change 5: Add unit tests

### In [test_genie_schema.py](tests/unit/test_genie_schema.py)

Add a new `TestCountInstructionSlots` class:

- **test_empty_config**: `{}` returns 0
- **test_instructions_only**: text + examples + functions counted correctly (no descriptions)
- **test_table_descriptions_counted**: 3 tables with descriptions = 3 extra slots
- **test_metric_view_descriptions_counted**: metric views with descriptions add to slot count
- **test_empty_descriptions_not_counted**: tables with `description: []` or `description: None` = 0 extra slots
- **test_mixed_all_types**: combined scenario with instructions + table descs + mv descs
- **test_strict_validation_rejects_with_descriptions**: 95 example_question_sqls + 6 table descriptions = 101 slots -- strict validation fails

### In [test_optimizer.py](tests/unit/test_optimizer.py)

Add to `TestMineBenchmarkExampleSqls`:

- **test_caps_at_slot_budget**: metadata_snapshot with 98 slots already used + 5 valid benchmarks -> only 2 proposals produced (the remaining budget)

## Regression Safety

- **Existing `_validate_lever5_proposals` tests pass unchanged**: Test fixtures use top-level `tables` (no `data_sources`), so `desc_count` = 0. Budget calculations stay the same.
- **Existing `_strict_validate` tests pass unchanged**: `_full_strict_config()` has 1 table description + 1 example SQL + 1 text instruction = 3 slots, well under 100.
- `**test_reference_config_passes`**: The reference config at `docs/genie_space_config.json` might now show a different slot count. Need to verify it stays under 100. If not, this test will alert us.
- **Existing `TestMineBenchmarkExampleSqls` tests pass unchanged**: `_METADATA` has no `data_sources`, so `count_instruction_slots` returns 0, leaving a full budget of 100 -- all existing tests mine < 100 proposals.
- **No behavioral change for spaces with few descriptions**: The fix only matters when total slots (including descriptions) approach or exceed 100.

