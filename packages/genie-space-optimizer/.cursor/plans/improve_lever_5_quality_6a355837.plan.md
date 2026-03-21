---
name: Improve Lever 5 Quality
overview: Improve Lever 5 (instructions/example SQL) proposal quality by strengthening the LLM prompt to prefer example SQL for routing, enforcing structured formatting for text instructions, adding proposal validation to reject generic/verbose output, and separating description (user-facing) from instructions (LLM-facing).
todos:
  - id: prompt-update
    content: "Update LEVER_5_INSTRUCTION_PROMPT in config.py: force example SQL for routing, structured formatting rules, anti-hallucination guard"
    status: completed
  - id: resolve-lever5
    content: "Update _resolve_lever5_llm_result() in optimizer.py: log warning when routing failure falls back to text_instruction"
    status: completed
  - id: proposal-validation
    content: "Add proposal validation in generate_metadata_proposals(): reject empty, generic, and over-length proposals"
    status: completed
  - id: max-chars-constant
    content: Add MAX_INSTRUCTION_TEXT_CHARS constant in config.py
    status: completed
  - id: description-cleanup
    content: Add _detect_instruction_content_in_description() in optimizer.py to separate description from instructions
    status: completed
  - id: example-sql-validation
    content: Extend _validate_example_sql_entry() in applier.py to verify SQL references real assets
    status: completed
  - id: unit-tests
    content: Add unit tests for new validation logic
    status: completed
isProject: false
---

# Improve Lever 5 Instruction and Example SQL Quality

## Context

Lever 5 generates two types of proposals:

- `**add_example_sql**` — a question + SQL pair that Genie pattern-matches against (high fidelity, preferred)
- `**add_instruction**` — free-text appended to the general instructions block (lower fidelity, last resort)

Current problems:

1. The LLM often generates verbose, generic `text_instruction` proposals instead of precise `example_sql` proposals, even when the failure is a routing issue
2. Text instructions are walls of text — no markdown structure, no conciseness
3. The description field (user-facing) gets polluted with LLM-facing routing rules
4. No validation rejects generic or asset-unaware proposals

The `_LEVER_TO_PATCH_TYPE` mapping in [config.py](src/genie_space_optimizer/common/config.py) (line 984) already maps `asset_routing_error → add_example_sql`, but `_resolve_lever5_llm_result()` in [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py) (line 1041) lets the LLM override this to `text_instruction`.

## Changes

### 1. Strengthen `LEVER_5_INSTRUCTION_PROMPT` in [config.py](src/genie_space_optimizer/common/config.py) (line 331)

Add three new sections to the prompt:

**a) Force example SQL for routing failures:**
After the priority hierarchy (line 356), add:

```
ROUTING FAILURES MUST USE EXAMPLE SQL:
When the failure involves asset routing (wrong table/TVF/metric view chosen),
you MUST return instruction_type "example_sql". Do NOT fall back to
text_instruction for routing issues — example SQL is orders of magnitude
more effective because Genie pattern-matches against it directly.
```

**b) Text instruction formatting rules:**
Replace the current "Keep under 500 chars" rule (line 368) with structured formatting guidance:

```
Rules for text instructions (when you must use them):
- Use structured markdown: ## headers, - bullets, short lines
- Be SPECIFIC: reference actual table names, column names, TVFs, or
  metric views from the Available Assets section
- NEVER generate generic domain guidance (e.g., "Revenue refers to
  gross revenue unless stated otherwise"). Every instruction must
  reference a specific asset from this Genie Space.
- Target ~50 lines max with clear section breaks
- Instruction budget remaining: {instruction_char_budget} chars
```

**c) Anti-hallucination guard:**
Add after the rules:

```
CRITICAL: If you have no blamed objects, no counterfactual fix, and
cannot identify a specific asset to reference, return:
{"instruction_type": "text_instruction", "instruction_text": "", "rationale": "No actionable fix identified"}
An empty instruction_text signals that no proposal should be generated.
```

### 2. Enforce example SQL preference in `_resolve_lever5_llm_result()` in [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py) (line 1041)

When the initial `patch_type` from `_LEVER_TO_PATCH_TYPE` is `add_example_sql` (routing failure) but the LLM returned `text_instruction`, log a warning and keep the LLM's response but mark it with a `downgraded_from_example_sql=True` flag so downstream validation can deprioritize it.

### 3. Add proposal validation in `generate_metadata_proposals()` in [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py) (line ~1644)

After proposals are constructed but before deduplication, add a validation pass:

- **Reject empty proposals:** If `proposed_value` (for `add_instruction`) or `example_sql` (for `add_example_sql`) is empty or whitespace, skip the proposal
- **Reject generic proposals:** If an `add_instruction` proposal doesn't contain any table name, TVF name, metric view name, or column name from the metadata snapshot, reject it with a log warning. Use the `table_names`, `tvf_names`, `mv_names` already available in the proposal context
- **Reject over-length text instructions:** If `proposed_value` for `add_instruction` exceeds `MAX_INSTRUCTION_TEXT_CHARS` (new constant, default 2000), truncate or reject

### 4. Add `MAX_INSTRUCTION_TEXT_CHARS` constant in [config.py](src/genie_space_optimizer/common/config.py)

```python
MAX_INSTRUCTION_TEXT_CHARS = 2000
```

This is the per-proposal limit. The existing `_enforce_instruction_limit()` in [applier.py](src/genie_space_optimizer/optimization/applier.py) (line 98) already caps total instruction text at 24,500 chars — this new constant prevents any single proposal from being a wall of text.

### 5. Add description-vs-instruction separation check in [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

Add a helper `_detect_instruction_content_in_description()` that checks if the Genie Space description contains instruction-like patterns (e.g., "ROUTING RULES", "MUST follow", "TVF", SQL keywords like "SELECT", "WHERE", "GROUP BY"). If detected, generate a proposal of type `update_description` that strips the instruction content, keeping only the user-facing summary paragraph.

Call this at the start of `generate_metadata_proposals()` when `target_lever == 5`. This is a one-time cleanup that separates concerns.

### 6. Validate example SQL references real assets in [applier.py](src/genie_space_optimizer/optimization/applier.py) (line 86, `_validate_example_sql_entry()`)

Extend `_validate_example_sql_entry()` to also check that the `sql` field references at least one table, TVF, or metric view that exists in the current config. This prevents hallucinated example SQL from being applied.

### 7. Update unit tests

- Test that `_resolve_lever5_llm_result()` logs a warning when routing failure downgrades from example_sql to text_instruction
- Test that the proposal validation rejects generic instructions (no asset references)
- Test that the proposal validation rejects empty proposals
- Test that the description cleanup detects instruction-like content

