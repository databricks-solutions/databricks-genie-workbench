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
<role>
You are an expert Databricks SQL author synthesizing a single NEW example
SQL that teaches a data assistant how to handle a class of failures. You
will produce an ORIGINAL question/SQL pair that matches a structural
archetype. You MUST NOT reproduce any benchmark question or SQL — you
have access only to an abstracted failure signature (AFS) that has been
scrubbed by the leak-safe contract above.
</role>

<context>
<failure_signature>
{{ afs_block }}
</failure_signature>

<archetype>
Name: {{ archetype_name }}
Shape Contract: {{ archetype_output_shape }}
Guidance:
{{ archetype_prompt_template }}
</archetype>

<schema>
Identifier allowlist — ONLY these identifiers may appear in your
example_sql. Any identifier outside the allowlist is a hallucination and
will cause your proposal to be rejected.

{{ identifier_allowlist }}
</schema>
</context>

<examples>
<example>
Input AFS: cluster H001, failure_type=missing_aggregation, blame_set=cat.demo.fact_sales, counterfactual fix "aggregate by region".
Output:
{"example_question": "What are total sales by region?", "example_sql": "SELECT region, SUM(amount) AS total_sales FROM cat.demo.fact_sales GROUP BY region ORDER BY total_sales DESC", "usage_guidance": "Use when the user asks for a metric broken down by a single dimension.", "rationale": "Exercises the missing_aggregation pattern via GROUP BY over a dimension column."}
</example>
<example>
Input AFS: cluster H002, failure_type=wrong_qualification, blame_set=cat.demo.orders + cat.demo.customers, counterfactual fix "join orders to customers on customer_id, filter by tier".
Output:
{"example_question": "What were premium-customer order counts last month?", "example_sql": "SELECT COUNT(*) AS premium_orders FROM cat.demo.orders o INNER JOIN cat.demo.customers c ON o.customer_id = c.customer_id WHERE c.tier = 'premium' AND DATE_TRUNC('month', o.order_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH)", "usage_guidance": "Use when the user asks for a segment-filtered metric in a relative month window.", "rationale": "Exercises wrong_qualification by demonstrating the explicit fact-to-dimension join + tier filter the cluster's counterfactual fix called for."}
</example>
</examples>

<instructions>
Produce exactly ONE example_sql proposal that matches the archetype's
shape contract.

Rules:
- ``example_question`` is a clean, customer-style business question (not
  a benchmark quote and not a paraphrase of any failed input).
- ``example_sql`` is a valid Databricks SQL query (no trailing semicolon)
  that uses ONLY identifiers from the <schema> allowlist above.
- Match the archetype's shape contract exactly — no extra clauses,
  no missing clauses.
- ``usage_guidance`` is one sentence: when should Genie reuse this
  example?
- ``rationale`` is one sentence: which failure mode does this example
  fix and why?
- Your proposal MUST be ORIGINAL — do not echo any string field from
  the AFS verbatim.
</instructions>

<output_schema>
Return a SINGLE JSON object. All four fields are required:

- "example_question": customer-style business question (string)
- "example_sql": valid Databricks SQL query, no trailing semicolon (string)
- "usage_guidance": one-sentence explanation of when this example applies (string)
- "rationale": one-sentence reference to the failure mode being fixed (string)
</output_schema>
