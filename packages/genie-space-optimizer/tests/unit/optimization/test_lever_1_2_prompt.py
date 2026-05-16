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
