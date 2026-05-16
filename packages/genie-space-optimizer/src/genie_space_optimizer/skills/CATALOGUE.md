# Skill Catalogue (canonical, post-Plan-4)

This file lists every migrated skill, its `prompt_constant_name`,
and its key metadata. The matching `<skill_id>/SKILL.md` file is
the source of truth — this file is a discovery aid only.

**Metadata-only SKILLs.** A SKILL.md row with `shared: <CONSTANT_NAME>` in
the second column means that skill participates in Stage-1 discovery
(its frontmatter selects it) but reuses the LLM template owned by
another skill. The body of the SKILL.md is intentionally empty — editing
it has no effect. To split a metadata-only SKILL into its own template,
see the comment inside the SKILL.md file.

| skill_id | prompt_constant_name | causal_or_non_causal | pickable_by_stage_1 | raw_evidence_v1 |
|---|---|---|---|---|
| `lever-1-table-column-description` | `LEVER_1_2_COLUMN_PROMPT` | causal | true | pass-through (N=3) |
| `lever-2-mv-column-refinement` | shared: `LEVER_1_2_COLUMN_PROMPT` | causal | true | pass-through (N=3) |
| `lever-3-tvf-routing` | `PROPOSAL_GENERATION_PROMPT` | causal | true | pass-through (N=3) |
| `lever-4-join-discovery` | `LEVER_4_JOIN_DISCOVERY_PROMPT` | non_causal | true | pass-through (N=3) |
| `lever-4-join-spec` | `LEVER_4_JOIN_SPEC_PROMPT` | causal | false | pass-through (N=3) |
| `lever-5a-instructions` | `LEVER_5A_INSTRUCTION_PROMPT` | causal | true | pass-through (N=3) |
| `lever-5-instruction` | `LEVER_5_INSTRUCTION_PROMPT` | causal | false | pass-through (N=3) |
| `lever-5b-example-sql` | `LEVER_5B_EXAMPLE_SQL_PROMPT` | causal | true | excluded |
| `lever-6-sql-expression` | `LEVER_6_SQL_EXPRESSION_PROMPT` | causal | true | pass-through (N=3) |
| `lever-1-rca-bridge` | `LEVER_1_RCA_BRIDGE_PROMPT` | causal | false | pass-through (N=3) |
| `stage-1-discovery` | `STAGE_1_DISCOVERY_PROMPT` | causal | false | excluded |
| `adaptive-strategist` | `ADAPTIVE_STRATEGIST_PROMPT` | causal | false | excluded (legacy fallback only) |

## How to add a new skill

1. Create `<skill_id>/SKILL.md` with frontmatter + body.
2. In `common/config.py`, add
   `XXX_PROMPT = _SKILL_LOADER.load_prompt("<skill_id>",
   expected_constant_name="XXX_PROMPT")`.
3. If the skill should be picked by Stage-1 discovery, add it to
   `_THREE_STAGE_SKILL_NAMES` (`common/config.py`) AND register a
   `_stage_2_<short>` adapter in
   `optimization/three_stage_pipeline.py:_STAGE_2_DISPATCH_TABLE`.
4. If the skill should receive raw evidence, add it to
   `_PROJECTOR_TABLE` in `optimization/raw_evidence.py`. If it must
   be evidence-free, add it to `_EXCLUDED_SKILLS`.
5. Append a row to this catalogue.
