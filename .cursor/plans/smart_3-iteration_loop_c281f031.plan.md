---
name: Smart 3-Iteration Loop
overview: "Implement 8 changes to make the lever loop intelligent within 3 iterations: fix the tried-patches dedup bug, add early-exit guards, hard-quarantine exhausted questions (with human-review flagging), add verbal self-reflection, classify rollback refinement modes, save skill exemplars, add strategist I/O logging, and reduce MAX_ITERATIONS to 3."
todos:
  - id: fix-dedup-bug
    content: "Step 1: Fix tried_patches key mismatch — add tried_root_causes set, update _filter_tried_clusters and rollback site"
    status: completed
  - id: consecutive-rollback
    content: "Step 2: Add CONSECUTIVE_ROLLBACK_LIMIT=2 config and breaker check in lever loop exit block"
    status: completed
  - id: fix-diminishing-returns
    content: "Step 3: Rewrite _diminishing_returns to consider all recent iterations, not just accepted ones"
    status: completed
  - id: hard-quarantine
    content: "Step 4: Hard-quarantine ADDITIVE_LEVERS_EXHAUSTED questions from clusters + call flag_for_human_review for labeling session"
    status: completed
  - id: verbal-reflection
    content: "Step 5: Add reflection_text to _build_reflection_entry, generate causal explanations at rollback/accept sites, render in format_reflection_buffer"
    status: completed
  - id: refinement-mode
    content: "Step 6: Add in_plan/out_of_plan refinement_mode classification to reflection entries + strategist prompt guidance"
    status: completed
  - id: skill-exemplars
    content: "Step 7: Save accepted patch patterns as skill_exemplars, render as Proven Patterns in strategist prompt"
    status: completed
  - id: strategist-io-logging
    content: "Step 8: Add detailed print blocks for strategist inputs (reflections, clusters, persistence) and outputs (full strategy JSON)"
    status: completed
  - id: reduce-max-iterations
    content: "Step 9: Change MAX_ITERATIONS from 5 to 3 in config.py"
    status: completed
  - id: update-tests
    content: Update existing tests for changed signatures (_build_question_persistence_summary return type, _build_reflection_entry new params, _diminishing_returns logic)
    status: completed
isProject: false
---

# Smart 3-Iteration Lever Loop

## Files Changed

- `[src/genie_space_optimizer/common/config.py](src/genie_space_optimizer/common/config.py)` — new constants, prompt update
- `[src/genie_space_optimizer/optimization/harness.py](src/genie_space_optimizer/optimization/harness.py)` — loop control, reflection, quarantine, logging
- `[src/genie_space_optimizer/optimization/optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)` — strategist I/O logging, quarantine filter, prompt assembly

---

## Step 1: Fix the tried-patches dedup bug

**Problem**: `tried_patches` stores `(patch_type, target_object)` (harness.py:3903-3907) but `_filter_tried_clusters` checks `(asi_failure_type, asi_blame_set)` (harness.py:1665-1668). These never match.

**Changes in `[harness.py](src/genie_space_optimizer/optimization/harness.py)`**:

- Add a second set `tried_root_causes: set[tuple[str, str]]` alongside `tried_patches` at line ~3444
- At the rollback site (line ~3902), also record `(root_cause, blame_set)` from the action group into `tried_root_causes`. Extract these from the `ag` dict's `source_cluster_ids` mapped back to the `clusters` list
- Update `_filter_tried_clusters` (line ~1655) to accept and check against `tried_root_causes` instead of `tried_patches`
- `tried_patches` remains for the DO NOT RETRY list in the strategist prompt (textual dedup at the LLM level)

---

## Step 2: Consecutive rollback breaker

**Problem**: No mechanism stops the loop when it's stuck in a rollback cycle. Testrun4 had 3 consecutive rollbacks.

**Changes in `[config.py](src/genie_space_optimizer/common/config.py)`**:

- Add `CONSECUTIVE_ROLLBACK_LIMIT = 2` near other loop constants (line ~84)

**Changes in `[harness.py](src/genie_space_optimizer/optimization/harness.py)`**:

- At the exit-checks block (line ~3448), after the `_diminishing_returns` check, add:

```python
_consecutive_rb = 0
for entry in reversed(reflection_buffer):
    if not entry.get("accepted"):
        _consecutive_rb += 1
    else:
        break
if _consecutive_rb >= CONSECUTIVE_ROLLBACK_LIMIT:
    logger.info("Consecutive rollback limit (%d) reached — stopping", CONSECUTIVE_ROLLBACK_LIMIT)
    break
```

---

## Step 3: Fix diminishing returns to count rollbacks

**Problem**: `_diminishing_returns` (harness.py:1629) only counts *accepted* iterations. With only 1 accepted iteration, `lookback=2` is never satisfied and the check never fires.

**Change in `[harness.py](src/genie_space_optimizer/optimization/harness.py)`**:

- Rewrite `_diminishing_returns` (line ~1629) to consider all recent iterations:

```python
def _diminishing_returns(reflection_buffer, epsilon=None, lookback=None):
    if epsilon is None:
        epsilon = DIMINISHING_RETURNS_EPSILON
    if lookback is None:
        lookback = DIMINISHING_RETURNS_LOOKBACK
    recent = reflection_buffer[-lookback:]
    if len(recent) < lookback:
        return False
    for r in recent:
        if r.get("accepted") and r.get("accuracy_delta", 0.0) >= epsilon:
            return False
    return True
```

---

## Step 4: Hard-quarantine exhausted questions + flag for human review

**Problem**: `_build_question_persistence_summary` classifies questions as `ADDITIVE_LEVERS_EXHAUSTED` and tells the LLM not to target them, but the LLM can ignore this. Also, quarantined questions must surface in the labeling session for human review.

**Changes in `[harness.py](src/genie_space_optimizer/optimization/harness.py)`**:

- Inside the lever loop, after building `_verdict_history` (line ~3497) and before calling the strategist (line ~3501):
  1. Call `_build_question_persistence_summary(_verdict_history, reflection_buffer)` to get the structured persistence data
  2. Collect `qid`s classified as `ADDITIVE_LEVERS_EXHAUSTED` or `PERSISTENT` with `max_consecutive >= 3`
  3. Remove those `qid`s from `clusters` (strip from each cluster's `question_ids` list; drop clusters that become empty)
  4. Add those `qid`s to `_correction_state["quarantined_qids"]` so `_analyze_and_distribute` excludes them in subsequent iterations
  5. Call `flag_for_human_review(spark, run_id, catalog, schema, domain, items)` for each quarantined question, with `reason` set to the classification string (e.g. `"ADDITIVE_LEVERS_EXHAUSTED: failed 4/5 evals, 3 consecutive"`) and `iterations_failed` / `patches_tried` populated from the structured data
- This ensures these questions appear in the `genie_opt_flagged_questions` table and are surfaced to human reviewers via `get_flagged_questions` in the labeling API

**Note**: `_build_question_persistence_summary` currently returns only a string. We need to modify it to also return the structured dict, changing the return to `(text, structured)`. Update the call in `optimizer.py` line 4656 to unpack the tuple: `persistence_text, persistence_structured = _build_question_persistence_summary(...)`.

---

## Step 5: Verbal self-reflection after every gate

**Problem**: The reflection buffer stores scores and patch types but no causal explanation. The strategist has to infer why something failed.

**Changes in `[harness.py](src/genie_space_optimizer/optimization/harness.py)`**:

- Add `reflection_text: str = ""` parameter to `_build_reflection_entry` (line ~1565) and include it in the returned dict
- At the rollback site (line ~3892): Build a reflection string from the gate result:
  - If `regressions` in gate_result: `"Rollback: patches caused regressions on {N} questions ({qids}). {target questions improved/did not improve}. The approach of {lever description} {worked for targets but had collateral damage / did not address the root cause}."`
  - If accuracy dropped: `"Rollback: overall accuracy dropped by {delta}%. {root_cause_summary} was not resolved by {patch_types}."`
- At the accept site (line ~3910): `"Accepted: {root_cause_summary} resolved. {lever description} improved accuracy by {delta}% affecting {N} questions."`

**Changes in `[optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)`**:

- In `format_reflection_buffer` (line ~871): Render `reflection_text` as a block under each iteration entry, e.g.: `"  Reflection: {text}"`

---

## Step 6: In-plan vs out-of-plan rollback classification

**Problem**: Not all rollbacks are equal. "Right lever, too broad" vs "wrong lever entirely" require different next actions.

**Changes in `[harness.py](src/genie_space_optimizer/optimization/harness.py)`**:

- Add `refinement_mode: str = ""` parameter to `_build_reflection_entry`; include in returned dict
- At the rollback site (line ~3892): Determine mode:
  - Get `affected_qids` from the AG and `new_failure_qids` from gate result
  - If any affected question improved (was failing, now passing or better score): `refinement_mode = "in_plan"` (right direction, collateral damage)
  - Otherwise: `refinement_mode = "out_of_plan"` (approach itself doesn't work)

**Changes in `[config.py](src/genie_space_optimizer/common/config.py)`**:

- In `ADAPTIVE_STRATEGIST_PROMPT` (line ~1719, Escalation section), add a paragraph:

```
## Refinement Mode Guidance
When the Reflection History shows a ROLLED_BACK entry:
- If "in_plan": The lever direction was correct but caused regressions. Refine the SAME lever with narrower scope or more specific targeting. Do NOT switch to a different root cause.
- If "out_of_plan": The approach fundamentally did not work. Switch to a different lever class or escalate. Do NOT retry the same lever type on the same target.
```

**Changes in `[optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)`**:

- In `format_reflection_buffer` (line ~905): Render `refinement_mode` alongside each rolled-back entry: `"  Refinement guidance: {in_plan|out_of_plan}"`

---

## Step 7: Skill exemplar reuse

**Problem**: Successful patches are not remembered as reusable patterns for the strategist.

**Changes in `[harness.py](src/genie_space_optimizer/optimization/harness.py)`**:

- Add `skill_exemplars: list[dict] = []` alongside `reflection_buffer` at line ~3443
- At the accept site (line ~3910): If `accuracy_delta >= 1.0`, append a compact exemplar:

```python
  skill_exemplars.append({
      "root_cause": ag.get("root_cause_summary", ""),
      "lever_pattern": sorted(ag.get("lever_directives", {}).keys()),
      "patch_types": [p.get("patch_type") for p in patches[:5]],
      "accuracy_gain": accuracy_delta,
  })
  

```

- Pass `skill_exemplars` to `_call_llm_for_adaptive_strategy`

**Changes in `[optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)`**:

- Add `skill_exemplars: list[dict] | None = None` parameter to `_call_llm_for_adaptive_strategy` (line ~4608)
- Build a `proven_patterns` text block from it and add to `format_kwargs`

**Changes in `[config.py](src/genie_space_optimizer/common/config.py)`**:

- Add `{{ proven_patterns }}` placeholder to `ADAPTIVE_STRATEGIST_PROMPT` in the context section (after `## Reflection History`):

```
  ## Proven Patterns
  {{ proven_patterns }}
  

```

---

## Step 8: Strategist I/O logging (user feedback #2)

**Problem**: Hard to understand what the strategist sees and decides without reading raw logs.

**Changes in `[optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)`**:

- After building `format_kwargs` (line ~4713) and before the LLM call, add a print block showing all key inputs:

```
  == STRATEGIST INPUT (Iteration N) ==================================
  | Success Summary:     {success_summary}
  | Priority Ranking:    {first 5 lines of ranking_text}
  | Reflection Buffer:   {reflection_text truncated to ~500 chars}
  | Persistence Summary: {persistence_text truncated to ~500 chars}
  | Proven Patterns:     {proven_patterns or "(none)"}
  | Clusters:            {N} hard, {N} soft
  | Human Suggestions:   {present/absent}
  =====================================================================
  

```

- After receiving the LLM response (line ~4784, where the response is already printed), expand the existing print to also show the full strategy JSON (pretty-printed, truncated to ~2000 chars):

```
  == STRATEGIST OUTPUT (Iteration N) =================================
  | AG:        {root_cause_summary}
  | Levers:    {lever list}
  | Questions: {affected_questions list}
  | Escalation: {escalation or "none"}
  | Rationale: {rationale}
  | Global Instruction Rewrite: {first 200 chars}...
  =====================================================================
  

```

---

## Step 9: Reduce MAX_ITERATIONS

**Changes in `[config.py](src/genie_space_optimizer/common/config.py)`**:

- Line 78: Change `MAX_ITERATIONS = 5` to `MAX_ITERATIONS = 3`

---

## Test Considerations

- Existing test `[tests/unit/test_persistence_escalation.py](tests/unit/test_persistence_escalation.py)` will need updates for the new `_build_question_persistence_summary` return type change (string -> tuple)
- `_build_reflection_entry` signature change (new params `reflection_text`, `refinement_mode`) needs any existing call sites updated — there are ~6 call sites in `harness.py`
- `_filter_tried_clusters` signature change (new `tried_root_causes` param) needs its call site at line ~3486 updated
- `_diminishing_returns` logic change should be verified against the existing test if any, or add a simple unit test

