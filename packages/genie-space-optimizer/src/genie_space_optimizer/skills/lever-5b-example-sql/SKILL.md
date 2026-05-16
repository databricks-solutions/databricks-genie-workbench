---
skill_id: lever-5b-example-sql
prompt_constant_name: LEVER_5B_EXAMPLE_SQL_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: true
description: Synthesize ORIGINAL example_sql proposals matching a structural archetype. Subject to a strict benchmark-leakage firewall.
when_to_pick: Failure shows a structural SQL pattern Genie can't generalize from existing examples; a NEW demonstrating example_sql would teach the pattern.
target_kind: base_table
target_min_count: 0
---
You are synthesizing a single NEW example SQL to help a data assistant
handle a specific class of failures. You will produce an ORIGINAL
question/SQL pair that matches a structural archetype. You MUST NOT
reproduce any benchmark question or SQL; you have access only to an
abstracted failure signature (AFS).

# Failure Signature (AFS)
Cluster ID: {{ cluster_id }}
Failure Type: {{ failure_type }}
Affected Judge: {{ affected_judge }}
Affected Questions: {{ question_count }}
Blamed Objects: {{ blame_set }}
Counterfactual Fixes (from judges):
{{ counterfactual_fixes }}
Structural Diff Classification:
{{ structural_diff }}
Judge Verdict Pattern: {{ judge_verdict_pattern }}
Summary: {{ suggested_fix_summary }}

# Archetype
Name: {{ archetype_name }}
Shape Contract: {{ archetype_output_shape }}
Guidance:
{{ archetype_prompt_template }}

# Schema
You may ONLY reference identifiers from this allowlist. Any identifier
outside the allowlist is a hallucination and will cause your proposal to
be rejected.
{{ identifier_allowlist }}

# Constraints
- Produce exactly ONE example_sql proposal.
- The ``example_question`` must be a clean, customer-style business
  question (not a benchmark quote).
- The ``example_sql`` must match the archetype's shape contract.
- Use only schema-allowlisted identifiers.
- Your proposal MUST be ORIGINAL — do not echo any field from the AFS.

# Output format (strict JSON)
{
  "example_question": "...",
  "example_sql": "...",
  "usage_guidance": "one-sentence explanation of when this example applies",
  "rationale": "one-sentence reference to the failure mode you are fixing"
}
