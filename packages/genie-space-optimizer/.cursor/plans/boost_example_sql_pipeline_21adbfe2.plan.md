---
name: Boost Example SQL Pipeline
overview: Increase example SQL throughput by fixing structural bottlenecks in the generation prompts and proposal pipeline, and strengthen validation to ensure every example SQL actually executes with non-null results before being applied.
todos:
  - id: detail-prompt-array
    content: Change STRATEGIST_DETAIL_PROMPT example_sql to example_sqls array (config.py)
    status: completed
  - id: holistic-prompt-boost
    content: "Update LEVER_5_HOLISTIC_PROMPT: 3 few-shot examples, broader scope, anti-sparse instruction (config.py)"
    status: completed
  - id: parse-array-response
    content: Update generate_proposals_from_strategy to handle example_sqls array + legacy compat (optimizer.py)
    status: completed
  - id: execute-validation
    content: Pass execute=True to validate_ground_truth_sql in _validate_lever5_proposals (optimizer.py)
    status: completed
  - id: dedup-existing
    content: Add dedup check against existing example_question_sqls in config (optimizer.py)
    status: completed
  - id: mine-benchmarks
    content: Add _mine_benchmark_example_sqls helper and integrate into lever-5 holistic path (optimizer.py + harness.py)
    status: completed
  - id: unit-tests
    content: Add unit tests for array parsing, dedup, and benchmark mining
    status: completed
isProject: false
---

# Boost Example SQL Generation and Validation

## Problem

Despite multiple optimization rounds, only 1-2 example SQLs are added per run. Six compounding bottlenecks limit throughput:

1. Strategist detail schema allows only **1** example SQL per AG (singular object, not array)
2. Holistic prompt's few-shot shows only **1** example (LLM mimics)
3. Prompt scopes example SQL to "routing issues" only
4. Validation rejects but **never executes** SQL (EXPLAIN-only, no `execute=True`)
5. No dedup against existing `example_question_sqls` in config
6. Benchmarks with `expected_sql` are never mined as ready-made example SQLs

## Changes

### 1. Strategist Detail Prompt: Singular to Array

**File:** [src/genie_space_optimizer/common/config.py](src/genie_space_optimizer/common/config.py)

In `STRATEGIST_DETAIL_PROMPT` (~line 1460), change the output schema from:

```
"5": {"instruction_guidance": "<text>", "example_sql": {"question": "...", "sql_sketch": "...", ...}}
```

to:

```
"5": {"instruction_guidance": "<text>", "example_sqls": [{"question": "...", "sql_sketch": "...", ...}]}
```

Also update the few-shot example (~line 1428) to show **2-3 example SQLs** in the array.

Update both occurrences of the schema in the prompt (the example block and the output_schema block).

### 2. Holistic Prompt: More Examples, Broader Scope

**File:** [src/genie_space_optimizer/common/config.py](src/genie_space_optimizer/common/config.py)

In `LEVER_5_HOLISTIC_PROMPT`:

- **Few-shot example** (~line 973): Expand `example_sql_proposals` from 1 to **3** example entries showing diverse patterns (routing, aggregation, temporal filter)
- **Scope** (~line 996): Change from "For ROUTING issues (wrong table/TVF/MV) or ambiguous patterns" to "For any recurring failure pattern (routing, aggregation, temporal, join, filter), propose example SQL. Genie pattern-matches against it directly. Aim for 3-5 proposals covering distinct failure patterns."
- **Anti-sparse instruction** (~line 1027): Change "If no example SQL needed" to "Always propose at least one example SQL per distinct failure cluster if a valid expected_sql exists."

### 3. Per-Cluster Strategist: Handle Array Response

**File:** [src/genie_space_optimizer/optimization/optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

In `generate_proposals_from_strategy` (~line 5477), the code currently reads:

```python
if isinstance(example_sql_dir, dict):
    eq = (example_sql_dir.get("question") or "").strip()
    ...
```

Change to handle both legacy single-object and new array format:

```python
example_sqls = l5_dir.get("example_sqls", [])
if not example_sqls and isinstance(l5_dir.get("example_sql"), dict):
    example_sqls = [l5_dir["example_sql"]]  # legacy compat

for ex_idx, example_sql_dir in enumerate(example_sqls):
    if not isinstance(example_sql_dir, dict):
        continue
    eq = (example_sql_dir.get("question") or "").strip()
    es = (example_sql_dir.get("sql_sketch") or "").strip()
    if eq and es:
        proposals.append({...})
```

Similarly update the "Example SQL from any lever" block (~line 5510) to handle arrays.

### 4. Validate with Execution (non-null results)

**File:** [src/genie_space_optimizer/optimization/optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

In `_validate_lever5_proposals` (~line 4964), change the `validate_ground_truth_sql` call to pass `execute=True` and forward `parameters`:

```python
is_valid, err = validate_ground_truth_sql(
    es, spark, catalog=catalog, gold_schema=gold_schema,
    execute=True,
    parameters=p.get("parameters"),
)
```

This ensures every example SQL:

- Passes EXPLAIN (syntax + column resolution)
- References existing tables
- **Actually executes and returns at least 1 row**

### 5. Dedup Against Existing Example SQLs in Config

**File:** [src/genie_space_optimizer/optimization/optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

In `_validate_lever5_proposals`, before appending a valid `add_example_sql` proposal, check it against existing `example_question_sqls` from the metadata:

```python
existing_eqs = metadata_snapshot.get("example_question_sqls", [])
existing_questions = {
    " ".join(e.get("question", [])).lower().strip()
    for e in existing_eqs if isinstance(e, dict)
}
...
if ptype == "add_example_sql":
    eq_norm = eq.lower().strip()
    if eq_norm in existing_questions:
        logger.info("Rejecting add_example_sql duplicate of existing: %.80s", eq)
        continue
```

Also add to `_filter_no_op_proposals` so it checks `add_example_sql` against existing.

### 6. Mine Benchmarks as Proactive Example SQLs

**File:** [src/genie_space_optimizer/optimization/optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

Add a new helper `_mine_benchmark_example_sqls(benchmarks, metadata_snapshot, spark, catalog, gold_schema)` that:

- Iterates benchmarks with non-empty `expected_sql`
- Skips benchmarks whose question already appears in existing `example_question_sqls`
- Validates each via `validate_ground_truth_sql(sql, spark, execute=True)`
- Returns `add_example_sql` proposals for benchmarks that pass

**File:** [src/genie_space_optimizer/optimization/harness.py](src/genie_space_optimizer/optimization/harness.py)

- Pass `benchmarks` to `generate_metadata_proposals` / `generate_proposals_from_strategy`
- Call `_mine_benchmark_example_sqls` at the start of the lever-5 holistic path to seed proposals with pre-validated example SQLs from benchmarks
- This is the highest-value change: benchmarks already have validated SQL that doesn't need LLM generation

### 7. Unit Tests

**File:** [tests/unit/test_optimizer.py](tests/unit/test_optimizer.py)

- Test that `_validate_lever5_proposals` rejects example SQL duplicates against existing config
- Test that `_mine_benchmark_example_sqls` produces correct proposals from benchmarks
- Test that the strategist detail response parser handles both array and single-object formats

