# Optimizer LLM Call Skill Catalogue

Canonical map of every live LLM call in `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/`. Authored as part of Plan 1 (Phase 0). Plans 2–4 cite this document by `skill_id`.

## Conventions

- **skill_id** — stable identifier (kebab-case, scoped by lever or stage).
- **Causal** — output must satisfy `UNIFIED_RCA_ENGINE_CONTRACT_PROMPT` invariants (see Definitions in plan-1 doc).
- **Leakage relevance** — does the firewall in `optimization/afs.py` apply? Only `lever-5b-example-sql` and the cluster-driven synthesis path (`synthesis.py:88`) are firewall-bound.
- **Path** — `live-primary` (every iteration), `live-preflight` (once per run), `fallback` (only when primary fails).

## Live primary path — strategy stage

| skill_id | Prompt constant | Defined at | Called at | Output schema (top-level keys) | Causal | Leakage |
|---|---|---|---|---|---|---|
| `strategy-adaptive` | `ADAPTIVE_STRATEGIST_PROMPT` | `config.py:3150` | `optimizer.py:9994` (rendered) → `harness.py:20284` (called) | `action_groups[]`, `global_instruction_rewrite{}`, `rationale` | yes | not L5b |

## Live primary path — per-AG per-lever generators

| skill_id | Prompt constant | Defined at | Called at | Output schema (top-level keys) | Causal | Leakage |
|---|---|---|---|---|---|---|
| `lever-1-tables-columns` | `LEVER_1_2_COLUMN_PROMPT` | `config.py:1636` | `optimizer.py:8148` (lever in {1,2}) → `_call_llm_for_proposal` | `changes[]`, `table_changes[]`, `rationale` | yes | not L5b |
| `lever-1-rca-bridge` | inline string | `optimizer.py:11658-11687` | `optimizer.py:11684` (`span_name="lever1_rca_proposal"`) | `description`, `synonyms[]` | yes | not L5b |
| `lever-2-metric-views` | `LEVER_1_2_COLUMN_PROMPT` | `config.py:1636` | `optimizer.py:8149` (lever in {1,2}) → `_call_llm_for_proposal` | same as L1 | yes | not L5b |
| `lever-4-join-spec-from-failures` | `LEVER_4_JOIN_SPEC_PROMPT` | `config.py:2160` | `optimizer.py:8150` (lever 4) → `_call_llm_for_proposal` | `join_specs[]`, `rationale` | yes | not L5b |
| `lever-4-join-discovery` | `LEVER_4_JOIN_DISCOVERY_PROMPT` | `config.py:2239` | `optimizer.py:8516` (`_call_llm_for_join_discovery`) | `join_specs[]`, `rationale` | **no** (structural / heuristic-driven, no cluster context) | not L5b |
| `lever-5-instruction` | `LEVER_5_INSTRUCTION_PROMPT` | `config.py:2382` | `optimizer.py:8151` (lever 5) → `_call_llm_for_proposal` | `instruction_text`, `example_sqls[]`, `rationale` | yes | **L5b applies** to `example_sqls[]` slice |
| `lever-5a-instructions` | `LEVER_5A_INSTRUCTION_PROMPT` | `config.py` (added in Plan 2) | `optimizer.py` (`_call_llm_for_lever_5a_instructions`, `span_name="lever_5a_instructions"`) | `instruction_text`, `rationale` (no SQL — enforced by `_validate_lever_5a_no_sql_output`) | yes | not L5b |
| `lever-5-holistic` (deprecated under `GSO_LEVER5_SPLIT_V1=1`) | `LEVER_5_HOLISTIC_PROMPT` | `config.py:2478` | `optimizer.py:8998` (`_call_llm_for_holistic_instructions`, `span_name="lever_5_holistic"`) | `instruction_text`, `example_sql_proposals[]`, `rationale` | yes | **L5b applies** to `example_sql_proposals[]` slice |
| `lever-5b-example-sql` | (rendered template — unchanged) | `synthesis.py:88` (`render_synthesis_prompt`) | `synthesis.py:860+` (`synthesize_example_sqls`, `span_name="lever_5b_example_sql"`) and `1039+` (`synthesize_example_sqls_for_rca`, `span_name="lever_5b_example_sql_for_rca"`) | `example_sql`, `example_question`, `parameters[]`, `usage_guidance` | yes | **L5b — strict firewall** (`afs.py:111` n-gram threshold 0.25 enforced) |
| `lever-6-sql-expression` | `LEVER_6_SQL_EXPRESSION_PROMPT` | `config.py:4709` (= header + `_LEVER_6_SQL_EXPRESSION_BODY`) | `optimizer.py:12042` (`span_name="lever6_llm"`) | `sql_expressions[]`, `rationale` | yes | not L5b |
| `lever-6-prose-rule-mining` | `PROSE_RULE_MINING_PROMPT` | `config.py:4890` | `optimizer.py:12578` (`span_name="prose_rule_mining"`) | `rules[]` (typed) | yes | not L5b |
| `proposal-generic-fallback` | `PROPOSAL_GENERATION_PROMPT` | `config.py:1599` | `optimizer.py:8153` (default for levers not in {1,2,4,5}) | `proposed_value`, `rationale` | yes | not L5b |

## Live preflight path — runs once per run

| skill_id | Prompt constant | Defined at | Called at | Output schema (top-level keys) | Causal | Leakage |
|---|---|---|---|---|---|---|
| `preflight-column-description-enrichment` | (inline string) | `optimizer.py:3608-3612` (`span_name="enrich_column_descriptions_batch_*"`) | same | per-column descriptions | no | not L5b |
| `preflight-table-description-enrichment` | (inline string) | `optimizer.py:3838-3840` (`span_name="enrich_table_descriptions_batch_*"`) | same | per-table descriptions | no | not L5b |
| `preflight-space-description` | (inline string) | `optimizer.py:3990-3992` (`span_name="generate_space_description"`) | same | space description text | no | not L5b |
| `preflight-instruction-expand` | `EXPAND_INSTRUCTION_PROMPT` | `config.py:2024` | `optimizer.py:4231` (called from `_expand_instructions_*`, `span_name` per attempt at `optimizer.py:4253`) | `{canonical_header: section_body}` | **no** (preflight gap-fill, no cluster context) | not L5b |
| `preflight-sample-questions` | (inline string) | `optimizer.py:4378-4380` (`span_name="generate_sample_questions"`) | same | `questions[]`, `rationale` | no | not L5b |
| `preflight-sql-expression-seeding` | `SQL_EXPRESSION_SEEDING_PROMPT` | `config.py:4969` | `optimizer.py:13383` (`span_name="sql_expression_seeding_llm"`) | `enrichments[]` (display_name, synonyms, instruction per candidate) | **no** (enriches schema-derived candidates, no cluster context) | not L5b |
| `preflight-rca-card-rationale-normalization` | inline (deterministic) | `rca_card_llm.py:82-94` (`_build_prompt`) | `rca_card_llm.py:67` (only when `GSO_RCA_CARD_LLM_NORMALIZATION` AND `llm_caller` provided) | rewritten `rationale` string only | no (rewrites rationale field of an already-built deterministic card) | not L5b |

## Fallback path — invoked only when primary fails

| skill_id | Prompt constant | Defined at | Called at | Output schema | Causal | Leakage |
|---|---|---|---|---|---|---|
| `strategy-monolithic-fallback` | `STRATEGIST_PROMPT` | `config.py:2607` | `optimizer.py:9496` (`span_name="monolithic_strategy_fallback"`) | same as `strategy-adaptive` | yes | not L5b |
| `strategy-triage-fallback` | `STRATEGIST_TRIAGE_PROMPT` | `config.py:2768` | `optimizer.py:10470` (`span_name="phase_1a_triage"`) → `harness.py:21116` (`_generate_holistic_strategy`) | AG skeletons | yes | not L5b |
| `strategy-detail-fallback` | `STRATEGIST_DETAIL_PROMPT` | `config.py:2906` | `optimizer.py:10625` (`span_name="phase_1b_detail_*"`) | per-AG `lever_directives{}` | yes | not L5b |

## Non-causal sites targeted by Plan 1 contract narrowing

The contract `_RCA_CONTRACT_HEADER` is currently injected at all three of these sites and will be made conditional on `GSO_RCA_CONTRACT_NARROW_V1`:

1. `EXPAND_INSTRUCTION_PROMPT` (`config.py:2032`) — `preflight-instruction-expand`.
2. `LEVER_4_JOIN_DISCOVERY_PROMPT` (`config.py:2245`) — `lever-4-join-discovery`.
3. `SQL_EXPRESSION_SEEDING_PROMPT` (`config.py:4975`) — `preflight-sql-expression-seeding`.

## Forward references — split in later plans

- Plan 2 splits `lever-5-holistic` into `lever-5a-instructions` (instruction text, sees raw evidence in Plan 4) and `lever-5b-example-sql` (AFS only, strict firewall).
- Plan 3 introduces `strategy-discovery` (replaces `strategy-adaptive` on the live path) and the `ActivationBundle` builder consumed by every lever skill.
- Plan 4 wires per-skill raw-evidence projections into every skill except `lever-5b-example-sql` and `lever-5b-example-sql-synthesis`.

## Leakage firewall summary

The firewall (`optimization/afs.py`) applies only to artifacts whose output is structurally isomorphic to a benchmark `(question, expected_sql)` row. In the catalogue above, that is:

- `lever-5-holistic` (deprecated path, runs only when split-mode flag is off) → `example_sql_proposals[]` slice
- `lever-5-instruction` → `example_sqls[]` slice (Plan 4 removes this; until then, the existing per-call validator stands)
- `lever-5b-example-sql` (entire output) — strict, n-gram threshold 0.25
- `lever-5a-instructions` produces NO SQL by output-schema design; the `_validate_lever_5a_no_sql_output` gate is a defense-in-depth check, not a primary firewall

Plan 2 makes the firewall surface area precisely "L5b only" once split-mode is default-on. L5a sees raw evidence (Plan 4) without leakage concern because its output schema forbids SQL.

Every other skill produces metadata (descriptions, synonyms, join specs, sql_expression fragments, instruction prose, action_group directives) whose shape cannot accidentally encode `expected_sql`. These are training, not leakage.

## Stage-1 + Stage-2 (Plan 3) catalogue overlay

When `GSO_THREE_STAGE_V1=1`, the legacy `_call_llm_for_adaptive_strategy` is bypassed for AG production. Instead:

1. **Stage-1 discovery** (one LLM call per iteration) picks `applicable_skills` from the table below.
2. **Stage-2 activation** dispatches to the executor for each picked skill_id.

| Stage-1 skill_id | Stage-2 executor | Existing per-lever entry (delegated to) | Output schema |
|---|---|---|---|
| `lever-1-table-column-description` | `three_stage_pipeline._stage_2_l1` | `optimizer._call_llm_for_proposal(... lever=1, patch_type="add_table_description"\|"add_column_description")` | `{proposed_value, rationale}` per target |
| `lever-2-mv-column-refinement` | `three_stage_pipeline._stage_2_l2` | `optimizer._call_llm_for_proposal(... lever=2)` | `{proposed_value, rationale}` per MV-column target |
| `lever-3-tvf-routing` | `three_stage_pipeline._stage_2_l3` | `optimizer._call_llm_for_proposal(... lever=3, patch_type="add_tvf_description")` | `{proposed_value, rationale}` per TVF target |
| `lever-4-join-discovery` | `three_stage_pipeline._stage_2_l4` | `optimizer._call_llm_for_join_discovery(metadata_snapshot, hints, w)` | `list[{join_spec, rationale}]` |
| `lever-5a-instructions` | `three_stage_pipeline._stage_2_l5a` | `optimizer._call_llm_for_lever_5a_instructions(...)` (Plan 2) | `{instruction_text, rationale}` |
| `lever-5b-example-sql` | `three_stage_pipeline._stage_2_l5b` | `optimizer._dispatch_lever_5b_for_cluster(cluster, metadata_snapshot, w, benchmark_corpus)` (Plan 2) per source cluster | `list[{example_question, example_sql, parameters, usage_guidance}]` |
| `lever-6-sql-expression` | `three_stage_pipeline._stage_2_l6` | `optimizer._generate_lever6_proposal(cluster, metadata_snapshot, ...)` | `{snippet_type, sql, alias, instruction, ...}` or `None` |

Skills NOT pickable by Stage-1 (deterministic / preflight / polish):
- `preflight-instruction-expand`, `preflight-sql-expression-seeding` — fire deterministically before discovery.
- `lever-5-holistic` (deprecated under `GSO_LEVER5_SPLIT_V1=1`).
- `rca-card-narrative-polish` — runs deterministically inside RCA card builder.

Shadow-mode comparison records (`tests/fixtures/three_stage_v1/`) record per-AG diffs between Stage-1 picks and the legacy strategist's `lever_directives.keys()`.

## Plan 4 raw-evidence wiring

Plan 4 adds an `ActivationBundle.raw_evidence` payload that exposes N≥3 diverse `(question, actual_sql, expected_sql, judge_rationale)` triples per cluster to every Stage-2 skill EXCEPT `lever-5b-example-sql`. Per-skill projector posture (gated by `GSO_RAW_EVIDENCE_V1`):

| Skill | raw_evidence_v1 posture |
|---|---|
| `lever-1-table-column-description` | pass-through (N=3) |
| `lever-2-mv-column-refinement` | pass-through (N=3) |
| `lever-3-tvf-routing` | pass-through (N=3) |
| `lever-4-join-discovery` | pass-through (N=3) |
| `lever-4-join-spec-from-failures` | pass-through (N=3) |
| `lever-5a-instructions` | pass-through (N=3) — L5a authors prose, never SQL; output firewall on Quick Fix path remains the safety net |
| `lever-5b-example-sql` | **excluded** — projector returns `()`. Output-side leakage firewall (`optimization/afs.py`) remains the safety net |
| `lever-5-holistic` (deprecated) | n/a |
| `lever-5-instruction` (legacy generic) | pass-through (N=3) |
| `lever-6-sql-expression` | pass-through (N=3) |
| `stage-1-discovery` | **excluded** — discovery routes; it does not synthesize patches and does not need raw triples |
| `proposal-generic-fallback` | pass-through (N=3) |

Default-off path is byte-stable with Plan 3. The slot `{{ raw_evidence_block }}` renders empty when `bundle.raw_evidence == ()`.

---

## Prompt registry invariants (2026-05-17)

Every prompt that fires an LLM call lives under one of three guardrails enforced by `tests/unit/optimization/test_prompt_registry_inventory.py`:

1. **Registry membership** — every SKILL.md-backed constant assigned via `_SKILL_LOADER.load_prompt(...)` must appear in `LEVER_PROMPTS` (in `common/config.py`). This is what registers the prompt to the MLflow Prompt Registry and surfaces it in the Linked Prompts tab. Adding a new constant without a matching `LEVER_PROMPTS` entry fails CI.
2. **No inline f-string LLM prompts in `optimization/`** — every prompt body must live in a `<skill_id>/SKILL.md` file. Inline f-string literals at `_traced_llm_call` callsites are forbidden; if you need conditional content, use template slots and `format_mlflow_template`.
3. **Trace linkage** — every callsite that invokes `_traced_llm_call` (or its routed variants in `_call_llm_for_proposal`) must call `_link_prompt_to_trace("<registry_key>")` immediately beforehand. Without this the Linked Prompts tab on the trace stays empty.

### Metadata-only SKILLs

A SKILL.md row in `skills/CATALOGUE.md` with `shared: <CONSTANT_NAME>` in the second column means the skill participates in Stage-1 discovery (its frontmatter selects it) but reuses the LLM template owned by another skill. The body of the SKILL.md is intentionally empty — editing it has no effect.

`lever-2-mv-column-refinement` is the only such metadata-only SKILL today; it shares `LEVER_1_2_COLUMN_PROMPT` with `lever-1-table-column-description`. To split a metadata-only SKILL into its own template, see the comment inside its SKILL.md file.

## Typed output contracts (2026-05-17)

Every active prompt has a Pydantic `LLMOutputContract` subclass in `optimization/prompt_io.py`. The contract:

- Pins the JSON shape the model's response must obey (`extra="forbid"` rejects unknown fields).
- Can be passed as `response_model=...` to `_traced_llm_call` to opt into server-side `response_format={"type":"json_schema","strict":true}` enforcement (Databricks Foundation Model API) AND client-side `validate_and_parse(...)` with auto-retry.
- The `build_response_format()` helper flattens Pydantic's `model_json_schema()` to the Databricks-supported subset (strips `pattern`, `anyOf`, `oneOf`, `allOf`, `prefixItems`, `$ref`, `maxLength`, etc.) so the call succeeds against the Foundation Model APIs.

Currently defined contracts (one per active prompt):

| Prompt registry key | Pydantic contract |
|---|---|
| `stage_1_discovery` | `Stage1DiscoveryOutput` |
| `lever_6_sql_expression` | `Lever6SqlExpressionOutput` |
| `lever_1_rca_bridge` | `Lever1RcaBridgeOutput` |
| `lever_1_2_column` | `Lever12ColumnOutput` |
| `adaptive_strategist` | `AdaptiveStrategistOutput` |
| `strategist_triage` | `StrategistTriageOutput` |
| `strategist_detail` | `StrategistDetailOutput` |
| `lever_4_join_discovery` | `Lever4JoinDiscoveryOutput` |
| `lever_5_instruction` | `Lever5InstructionOutput` |
| `lever_5a_instructions` | `Lever5aInstructionsOutput` |
| `lever_5b_example_sql` | `Lever5bExampleSqlOutput` |

Deferred-allowlist entries (low-volume preflight/enrichment + legacy fallbacks) are documented in `tests/unit/optimization/test_prompt_registry_inventory.py::TYPED_OUTPUT_DEFERRED_ALLOWLIST`.

### Adding a new prompt: contract checklist

1. Define the constant: `MY_NEW_PROMPT = _SKILL_LOADER.load_prompt("my-skill-id", expected_constant_name="MY_NEW_PROMPT")` in `common/config.py`.
2. Register: `LEVER_PROMPTS["my_new_prompt"] = MY_NEW_PROMPT`.
3. Wire trace linkage: call `_link_prompt_to_trace("my_new_prompt")` immediately before the LLM call.
4. Define the typed output contract: `class MyNewPromptOutput(LLMOutputContract): ...` in `prompt_io.py`. Class name MUST follow `snake_case → CamelCase + "Output"` so the coverage test finds it automatically.
5. (Optional) Wire `response_model=MyNewPromptOutput` at the callsite for server-side JSON-schema enforcement.

Plan reference: `docs/prompt_improvements/2026-05-17-prompt-registry-and-typed-io-hygiene.md`
