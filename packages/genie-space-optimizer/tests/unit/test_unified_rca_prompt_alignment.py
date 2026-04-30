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
