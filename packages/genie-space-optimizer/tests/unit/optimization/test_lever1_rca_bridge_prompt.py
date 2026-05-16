"""Tests for the lever-1 RCA-bridge SKILL.md template rendering and content.

Plan reference: docs/prompt_improvements/2026-05-17-lever-1-rca-bridge-hardening.md
"""
from __future__ import annotations

import pytest


def test_lever_1_rca_bridge_max_tokens_constant_exists_and_is_sized():
    """LEVER_1_RCA_BRIDGE_MAX_TOKENS must exist and be sized per baseline."""
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_1_RCA_BRIDGE_MAX_TOKENS"), (
        "LEVER_1_RCA_BRIDGE_MAX_TOKENS must be defined in "
        "genie_space_optimizer.common.config"
    )
    assert isinstance(config.LEVER_1_RCA_BRIDGE_MAX_TOKENS, int)
    assert 250 <= config.LEVER_1_RCA_BRIDGE_MAX_TOKENS <= 600, (
        f"LEVER_1_RCA_BRIDGE_MAX_TOKENS="
        f"{config.LEVER_1_RCA_BRIDGE_MAX_TOKENS} is outside the "
        f"evidence-based band [250, 600]"
    )


def test_lever_1_rca_bridge_prompt_has_xml_structure():
    """The RCA-bridge SKILL.md body must be wrapped in XML tags."""
    from genie_space_optimizer.common.config import LEVER_1_RCA_BRIDGE_PROMPT

    prompt = LEVER_1_RCA_BRIDGE_PROMPT

    required_tag_pairs = [
        ("<role>", "</role>"),
        ("<unified_rca_engine_contract>", "</unified_rca_engine_contract>"),
        ("<context>", "</context>"),
        ("<examples>", "</examples>"),
        ("<instructions>", "</instructions>"),
        ("<output_schema>", "</output_schema>"),
    ]
    for open_tag, close_tag in required_tag_pairs:
        assert open_tag in prompt, f"missing required tag {open_tag}"
        assert close_tag in prompt, f"missing required tag {close_tag}"
        assert prompt.index(open_tag) < prompt.index(close_tag), (
            f"{open_tag} must appear before {close_tag}"
        )
