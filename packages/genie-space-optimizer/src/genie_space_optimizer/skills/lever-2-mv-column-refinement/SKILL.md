---
skill_id: lever-2-mv-column-refinement
causal_or_non_causal: causal
pickable_by_stage_1: true
description: Refine metric-view column metadata — definitions, synonyms, important_filters.
when_to_pick: Failure stems from missing or weak metric-view metadata; the right MV column exists but Genie picks the wrong one or applies wrong filter semantics.
target_kind: metric_view
target_min_count: 0
---

<!--
This SKILL.md is metadata-only. The LLM template is
shared with LEVER_1_2_COLUMN_PROMPT (also documented as
"shared with lever-1-table-column-description") — both
lever=1 and lever=2 route through the same template at the lever-call
dispatch site in optimizer.py. Do not add an XML body (role / context /
instructions blocks) here; it will be dead code that nobody loads.

If you need a separate L2 template in the future:
  1. Create LEVER_2_MV_COLUMN_PROMPT in common/config.py via
     _SKILL_LOADER.load_prompt("lever-2-mv-column-refinement",
     expected_constant_name="LEVER_2_MV_COLUMN_PROMPT").
  2. Split prompt_map[2] in _call_llm_for_proposal to point at the new
     constant.
  3. Register the new constant in LEVER_PROMPTS.
  4. Replace this comment with the new body.
-->
