"""Tests for the lever-1/2 column-refinement prompt scaffolding and slot pruning.

Plan reference: docs/prompt_improvements/2026-05-17-lever-1-2-column-prompt-hardening.md
"""
from __future__ import annotations

import pytest


def test_lever_1_2_max_tokens_constant_exists_and_is_sized():
    """LEVER_1_2_MAX_TOKENS must exist and be sized per baseline doc.

    Per baseline (2026-05-17-lever1-2-column-prompt-baseline.md):
      - Output shape: changes[≤3] + table_changes[≤2] + rationale[≤300 chars]
      - Conservative content size: ~900 tokens with 50% headroom = 2048
    Therefore the cap must be > 900 and ≤ 2_500.
    """
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_1_2_MAX_TOKENS"), (
        "LEVER_1_2_MAX_TOKENS must be defined in genie_space_optimizer.common.config"
    )
    assert isinstance(config.LEVER_1_2_MAX_TOKENS, int)
    assert 900 < config.LEVER_1_2_MAX_TOKENS <= 2_500, (
        f"LEVER_1_2_MAX_TOKENS={config.LEVER_1_2_MAX_TOKENS} is outside the "
        f"evidence-based band (900, 2500]"
    )


def test_lever_1_2_system_msg_constant_exists_and_frames_domain():
    """LEVER_1_2_SYSTEM_MSG must frame the domain (metadata curation) and
    require strict JSON output.
    """
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_1_2_SYSTEM_MSG"), (
        "LEVER_1_2_SYSTEM_MSG must be defined in genie_space_optimizer.common.config"
    )
    msg = config.LEVER_1_2_SYSTEM_MSG
    assert isinstance(msg, str) and len(msg) > 80, (
        "LEVER_1_2_SYSTEM_MSG should be a meaningful framing message, not boilerplate"
    )
    assert "Genie" in msg and "metadata" in msg.lower(), (
        "system message must frame the Genie Space metadata-curation domain"
    )
    assert "JSON" in msg, "system message must require JSON output"


def test_lever_1_2_prompt_contract_block_is_slim_and_l1_relevant():
    """The inline <unified_rca_engine_contract> block must be slimmed."""
    from genie_space_optimizer.common.config import LEVER_1_2_COLUMN_PROMPT

    prompt = LEVER_1_2_COLUMN_PROMPT

    assert "<unified_rca_engine_contract>" in prompt
    assert "Leakage boundary" in prompt or "leakage boundary" in prompt
    assert "match" in prompt.lower() and "defect" in prompt.lower()
    assert "Precedence" in prompt or "precedence" in prompt

    assert "primary_cluster_id" not in prompt, (
        "primary_cluster_id is an action-group field; L1 LLM cannot emit it"
    )
    assert "regression_debt_qids" not in prompt, (
        "regression_debt_qids is RCA-level; L1 LLM cannot reason about it"
    )

    contract_start = prompt.index("<unified_rca_engine_contract>")
    contract_end = prompt.index("</unified_rca_engine_contract>")
    contract_body = prompt[contract_start:contract_end]
    assert contract_body.count("\n") < 30, (
        f"slim contract should be < 30 lines; got {contract_body.count(chr(10))}"
    )


def test_lever_1_2_prompt_has_counterfactual_fix_hints_section():
    """L1/L2 template must render the counterfactual_fixes slot."""
    from genie_space_optimizer.common.config import LEVER_1_2_COLUMN_PROMPT

    prompt = LEVER_1_2_COLUMN_PROMPT
    assert "## Counterfactual Fix Hints" in prompt
    assert "{{ counterfactual_fixes }}" in prompt
    assert "Counterfactual Fix Hints" in prompt or "counterfactual" in prompt.lower()


def test_lever_1_2_prompt_has_output_bounds():
    """The L1/L2 template <instructions> must include bounded-output rules."""
    from genie_space_optimizer.common.config import LEVER_1_2_COLUMN_PROMPT

    prompt = LEVER_1_2_COLUMN_PROMPT
    assert (
        "at most 3 changes" in prompt
        or "maximum of 3 changes" in prompt
        or "max 3 changes" in prompt
    ), "instructions must bound changes[] to at most 3 entries"
    assert "300 char" in prompt or "300 character" in prompt
    assert "200 char" in prompt or "200 character" in prompt
    assert "2-5" in prompt or "2 to 5" in prompt


def test_lever_1_2_prompt_renames_sql_diffs_section():
    """Section renamed from '## SQL Diffs' to '## Structural Diff Features'."""
    from genie_space_optimizer.common.config import LEVER_1_2_COLUMN_PROMPT

    prompt = LEVER_1_2_COLUMN_PROMPT
    assert "## Structural Diff Features" in prompt
    assert "## SQL Diffs (Expected vs Generated)" not in prompt


def test_lever_1_2_prompt_has_four_canonical_examples():
    """The L1/L2 template <examples> block must contain four canonical
    examples that cover the four decision boundaries."""
    from genie_space_optimizer.common.config import LEVER_1_2_COLUMN_PROMPT

    prompt = LEVER_1_2_COLUMN_PROMPT
    example_count = prompt.count("<example>")
    assert example_count == 4, (
        f"expected 4 canonical examples, got {example_count}"
    )

    examples_section_start = prompt.index("<examples>")
    examples_section_end = prompt.index("</examples>")
    examples_section = prompt[examples_section_start:examples_section_end]

    assert "wrong_column" in examples_section and "synonyms" in examples_section
    assert "missing_definition" in examples_section
    assert "wrong_table_selection" in examples_section or "table_changes" in examples_section
    assert '"changes": []' in examples_section
