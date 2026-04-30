from __future__ import annotations


# RCA-driven prompts that MUST embed the full shared RCA contract.
# Keep this in sync with the "Scope Decisions" table in
# docs/2026-04-29-unified-rca-prompt-alignment-plan.md.
FULL_RCA_PROMPT_NAMES = (
    "ADAPTIVE_STRATEGIST_PROMPT",
    "STRATEGIST_PROMPT",
    "STRATEGIST_TRIAGE_PROMPT",
    "STRATEGIST_DETAIL_PROMPT",
    "LEVER_1_2_COLUMN_PROMPT",
    "LEVER_5_HOLISTIC_PROMPT",
    "LEVER_5_INSTRUCTION_PROMPT",
    "LEVER_6_SQL_EXPRESSION_PROMPT",
    "LEVER_4_JOIN_DISCOVERY_PROMPT",
    "LEVER_4_JOIN_SPEC_PROMPT",
    "PROPOSAL_GENERATION_PROMPT",
    "EXPAND_INSTRUCTION_PROMPT",
    "PROSE_RULE_MINING_PROMPT",
    "SQL_EXPRESSION_SEEDING_PROMPT",
)

# Schema-driven prompts that get the short leak-safe/narrowness contract,
# not the full RCA/action-group contract.
SCOPED_EXAMPLE_SYNTHESIS_PROMPT_NAMES = (
    "PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT",
)

OPTIMIZER_PROMPT_NAMES = FULL_RCA_PROMPT_NAMES + SCOPED_EXAMPLE_SYNTHESIS_PROMPT_NAMES

# Prompts that MUST NOT receive the contract (cosmetic / out of optimizer
# control plane). Keeping this explicit prevents accidental injection.
NON_OPTIMIZER_PROMPT_NAMES = (
    "PROACTIVE_INSTRUCTION_PROMPT",
    "DESCRIPTION_ENRICHMENT_PROMPT",
    "TABLE_DESCRIPTION_ENRICHMENT_PROMPT",
    "SPACE_DESCRIPTION_PROMPT",
    "SAMPLE_QUESTIONS_PROMPT",
    "GT_REPAIR_PROMPT",
)


def test_unified_rca_contract_constant_exists() -> None:
    from genie_space_optimizer.common import config

    contract = config.UNIFIED_RCA_ENGINE_CONTRACT_PROMPT

    # Literal-content checks live ONLY on the constant itself so that
    # rewording the contract does not break N consumer tests.
    assert "<unified_rca_engine_contract>" in contract
    assert "</unified_rca_engine_contract>" in contract
    assert "Unified RCA engine contract" in contract
    assert "ground_truth_correct" in contract
    assert "neither_correct" in contract
    assert "primary_cluster_id" in contract
    assert "source_cluster_ids" in contract
    assert "affected_questions" in contract
    assert "target_qids" not in contract
    assert "regression_debt_qids" in contract
    assert "patch type must match RCA" in contract
    assert "broad global instruction" in contract
    assert "judge feedback -> RCA -> lever -> patch" in contract


def test_leak_safe_example_synthesis_contract_constant_exists() -> None:
    from genie_space_optimizer.common import config

    contract = config.LEAK_SAFE_EXAMPLE_SYNTHESIS_CONTRACT_PROMPT

    assert "<leak_safe_example_synthesis_contract>" in contract
    assert "</leak_safe_example_synthesis_contract>" in contract
    assert "single JSON object" in contract
    assert "Do not copy held-out benchmark expected SQL" in contract
    assert "action_groups" not in contract
    assert "regression_debt_qids" not in contract


def test_full_rca_prompts_include_unified_rca_contract_tag() -> None:
    """Each RCA-driven prompt embeds the full contract by structural tag.

    We assert tag presence — not contract phrasing — so contract rewords
    don't ripple through every consumer test. Phrasing assertions live on
    ``test_unified_rca_contract_constant_exists`` alone.
    """
    from genie_space_optimizer.common import config

    for name in FULL_RCA_PROMPT_NAMES:
        prompt = getattr(config, name)
        assert "<unified_rca_engine_contract>" in prompt, name
        assert "</unified_rca_engine_contract>" in prompt, name


def test_preflight_prompt_uses_scoped_example_contract_only() -> None:
    from genie_space_optimizer.common import config

    prompt = config.PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT

    assert "<leak_safe_example_synthesis_contract>" in prompt
    assert "</leak_safe_example_synthesis_contract>" in prompt
    assert "<unified_rca_engine_contract>" not in prompt
    assert "regression_debt_qids" not in prompt


def test_non_optimizer_prompts_do_not_include_unified_rca_contract() -> None:
    """Cosmetic / out-of-scope prompts must NOT receive the contract."""
    from genie_space_optimizer.common import config

    for name in NON_OPTIMIZER_PROMPT_NAMES:
        prompt = getattr(config, name)
        assert "<unified_rca_engine_contract>" not in prompt, name
        assert "<leak_safe_example_synthesis_contract>" not in prompt, name


def test_rca_contract_header_disabled_when_env_flag_off(monkeypatch) -> None:
    """Setting GSO_INCLUDE_UNIFIED_RCA_CONTRACT=false yields an empty header."""
    import importlib

    monkeypatch.setenv("GSO_INCLUDE_UNIFIED_RCA_CONTRACT", "false")

    from genie_space_optimizer.common import config as live_config

    fresh = importlib.reload(live_config)
    try:
        assert fresh._RCA_CONTRACT_HEADER == ""
        assert fresh._EXAMPLE_SYNTHESIS_CONTRACT_HEADER == ""
        assert "Unified RCA engine contract" in fresh.UNIFIED_RCA_ENGINE_CONTRACT_PROMPT
        assert (
            "Leak-safe example synthesis contract"
            in fresh.LEAK_SAFE_EXAMPLE_SYNTHESIS_CONTRACT_PROMPT
        )
    finally:
        monkeypatch.delenv("GSO_INCLUDE_UNIFIED_RCA_CONTRACT", raising=False)
        importlib.reload(live_config)


def test_rca_contract_header_enabled_by_default() -> None:
    from genie_space_optimizer.common import config

    assert config._RCA_CONTRACT_HEADER.startswith("<unified_rca_engine_contract>")
    assert config._RCA_CONTRACT_HEADER.endswith("\n\n")
    assert config._EXAMPLE_SYNTHESIS_CONTRACT_HEADER.startswith(
        "<leak_safe_example_synthesis_contract>"
    )
    assert config._EXAMPLE_SYNTHESIS_CONTRACT_HEADER.endswith("\n\n")


def test_strategy_prompts_embed_contract_tag() -> None:
    """Each strategy prompt embeds the contract by structural tag."""
    from genie_space_optimizer.common.config import (
        ADAPTIVE_STRATEGIST_PROMPT,
        STRATEGIST_DETAIL_PROMPT,
        STRATEGIST_PROMPT,
        STRATEGIST_TRIAGE_PROMPT,
    )

    for prompt in (
        ADAPTIVE_STRATEGIST_PROMPT,
        STRATEGIST_PROMPT,
        STRATEGIST_TRIAGE_PROMPT,
        STRATEGIST_DETAIL_PROMPT,
    ):
        assert "<unified_rca_engine_contract>" in prompt
        assert "</unified_rca_engine_contract>" in prompt
        assert prompt.index("<unified_rca_engine_contract>") < prompt.index("<instructions>")


def test_lever_6_sql_expression_prompt_renders_with_double_brace_substitution() -> None:
    """Regression guard: prove ``{{ var }}`` placeholders survive intact."""
    from genie_space_optimizer.common.config import (
        LEVER_6_SQL_EXPRESSION_PROMPT,
        format_mlflow_template,
    )

    rendered = format_mlflow_template(
        LEVER_6_SQL_EXPRESSION_PROMPT,
        root_cause="missing time_window filter",
        cluster_context="(cluster_context)",
        schema_context="(schema_context)",
        existing_sql_snippets="(none)",
        strategist_hints="(none)",
    )
    assert "{{ root_cause }}" not in rendered
    assert "missing time_window filter" in rendered
    assert "{{ cluster_context }}" not in rendered
    assert '"snippet_type"' in rendered


def test_lever_prompts_embed_contract_tag() -> None:
    """Each lever / proposal prompt embeds the contract by structural tag."""
    from genie_space_optimizer.common.config import (
        EXPAND_INSTRUCTION_PROMPT,
        LEVER_1_2_COLUMN_PROMPT,
        LEVER_4_JOIN_DISCOVERY_PROMPT,
        LEVER_4_JOIN_SPEC_PROMPT,
        LEVER_5_HOLISTIC_PROMPT,
        LEVER_5_INSTRUCTION_PROMPT,
        LEVER_6_SQL_EXPRESSION_PROMPT,
        PROPOSAL_GENERATION_PROMPT,
    )

    for prompt in (
        LEVER_1_2_COLUMN_PROMPT,
        LEVER_4_JOIN_DISCOVERY_PROMPT,
        LEVER_4_JOIN_SPEC_PROMPT,
        LEVER_5_HOLISTIC_PROMPT,
        LEVER_5_INSTRUCTION_PROMPT,
        LEVER_6_SQL_EXPRESSION_PROMPT,
        PROPOSAL_GENERATION_PROMPT,
        EXPAND_INSTRUCTION_PROMPT,
    ):
        assert "<unified_rca_engine_contract>" in prompt
        assert "</unified_rca_engine_contract>" in prompt


def test_preflight_example_synthesis_prompt_renders_with_double_brace_substitution() -> None:
    """Regression guard for the preflight prompt's ``{{ var }}`` syntax."""
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
        format_mlflow_template,
    )

    rendered = format_mlflow_template(
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
        slice_tables="cat.sch.sales",
        slice_metric_views="(none)",
        slice_join_spec="(none)",
        slice_columns="cat.sch.sales.cy_sales",
        slice_data_profile="(none)",
        schema_example_identifier="cat.sch.sales",
        metric_view_contract="",
        archetype_name="top_n_by_metric",
        archetype_prompt_template="(template)",
        archetype_output_shape="{}",
        identifier_allowlist="cat.sch.sales",
        existing_questions_list="(none)",
        retry_feedback="",
    )
    assert "<leak_safe_example_synthesis_contract>" in rendered
    assert "<unified_rca_engine_contract>" not in rendered
    assert "regression_debt_qids" not in rendered
    assert "{{ slice_tables }}" not in rendered
    assert "cat.sch.sales" in rendered
    assert '"example_question"' in rendered


def test_build_context_data_surfaces_mandatory_regression_debt_qids() -> None:
    from genie_space_optimizer.optimization.optimizer import _build_context_data

    metadata_snapshot = {
        "_mandatory_regression_debt_qids": ["q014", "q007"],
        "_data_profile": {},
        "instructions": {
            "text_instructions": [{"content": "Use sales reports."}],
            "join_specs": [],
            "sql_snippets": [],
            "example_question_sqls": [],
        },
        "data_sources": {"tables": [], "metric_views": []},
    }

    context = _build_context_data(
        clusters=[],
        soft_signal_clusters=[],
        metadata_snapshot=metadata_snapshot,
        reflection_buffer=[],
        priority_ranking=[],
        blame_set=None,
        success_summary="10 of 14 benchmarks pass.",
        reflection_text="",
        persistence_text="",
        proven_patterns_text="",
        suggestions_text="",
    )

    assert context["mandatory_regression_debt_qids"] == ["q014", "q007"]


def test_build_context_data_returns_none_when_no_regression_debt() -> None:
    from genie_space_optimizer.optimization.optimizer import _build_context_data

    for empty_value in (None, [], ()):
        metadata_snapshot = {
            "_mandatory_regression_debt_qids": empty_value,
            "_data_profile": {},
            "instructions": {
                "text_instructions": [],
                "join_specs": [],
                "sql_snippets": [],
                "example_question_sqls": [],
            },
            "data_sources": {"tables": [], "metric_views": []},
        }

        context = _build_context_data(
            clusters=[],
            soft_signal_clusters=[],
            metadata_snapshot=metadata_snapshot,
            reflection_buffer=[],
            priority_ranking=[],
            blame_set=None,
            success_summary="(no progress yet)",
            reflection_text="",
            persistence_text="",
            proven_patterns_text="",
            suggestions_text="",
        )

        assert context["mandatory_regression_debt_qids"] is None


def test_adaptive_strategist_llm_call_uses_tolerant_response_validator() -> None:
    """Source-level guard: adaptive strategist call uses the tolerant validator."""
    import inspect

    from genie_space_optimizer.optimization import optimizer

    source = inspect.getsource(optimizer._call_llm_for_adaptive_strategy)

    assert 'span_name="adaptive_strategy"' in source
    assert "response_validator=_adaptive_strategist_response_validator" in source
    assert "response_validator=_extract_json" not in source


def test_adaptive_strategist_validator_accepts_strict_json() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        _adaptive_strategist_response_validator,
    )

    assert _adaptive_strategist_response_validator(
        '{"action_groups": []}'
    ) == {"action_groups": []}


def test_adaptive_strategist_validator_accepts_truncated_but_recoverable_json() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        _adaptive_strategist_response_validator,
    )

    # ``_repair_truncated_strategy_json`` extracts a balanced ``[...]``
    # for ``action_groups`` even when the surrounding object is unclosed.
    # The strict parser fails because the outer object has no closing
    # brace, but the salvage path returns a usable dict.
    truncated = (
        '{"action_groups": ['
        '{"primary_cluster_id": "H001", '
        '"source_cluster_ids": ["H001"], '
        '"affected_questions": ["q014"], '
        '"root_cause_summary": "missing filter", '
        '"patches": []}'
        '], "rationale": "truncated tail follo'
    )

    parsed = _adaptive_strategist_response_validator(truncated)
    # Either the strict parser extracts the inner array, or the salvage
    # wraps it into the strategy dict. Both are recoverable; what matters
    # is that the validator returns parsed structure rather than raising
    # so ``_traced_llm_call`` does not retry the prompt.
    if isinstance(parsed, dict):
        assert "action_groups" in parsed
    else:
        assert isinstance(parsed, list) and parsed
        assert parsed[0]["primary_cluster_id"] == "H001"


def test_adaptive_strategist_validator_rejects_non_json_refusal() -> None:
    import json
    import pytest

    from genie_space_optimizer.optimization.optimizer import (
        _adaptive_strategist_response_validator,
    )

    with pytest.raises(json.JSONDecodeError):
        _adaptive_strategist_response_validator(
            "I'm sorry, I cannot help with that request."
        )
