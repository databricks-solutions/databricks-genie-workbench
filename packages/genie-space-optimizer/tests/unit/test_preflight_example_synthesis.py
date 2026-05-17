from __future__ import annotations

from unittest.mock import patch


def test_preflight_synthesis_returns_accepted_examples(monkeypatch):
    from genie_space_optimizer.optimization import preflight_synthesis as ps

    proposal = {
        "patch_type": "add_example_sql",
        "example_question": "Show stores by region",
        "example_sql": "SELECT region, COUNT(*) FROM cat.sch.stores GROUP BY region",
        "usage_guidance": "Use for regional store counts.",
    }

    class _Gate:
        def __init__(self, gate: str, passed: bool, reason: str = ""):
            self.gate = gate
            self.passed = passed
            self.reason = reason

    monkeypatch.setattr(
        ps,
        "plan_asset_coverage",
        lambda metadata_snapshot, need, rng=None: [
            (
                ps.ARCHETYPES[0],
                ps.AssetSlice(tables=[{"identifier": "cat.sch.stores"}]),
            )
        ],
    )
    monkeypatch.setattr(
        ps,
        "synthesize_preflight_candidate",
        lambda *args, **kwargs: dict(proposal),
    )
    monkeypatch.setattr(
        ps,
        "validate_synthesis_proposal",
        lambda *args, **kwargs: (True, [_Gate("parse", True), _Gate("execute", True), _Gate("structural", True), _Gate("arbiter", True), _Gate("firewall", True)]),
    )
    monkeypatch.setattr(
        ps,
        "_apply_preflight_proposals",
        lambda proposals, **kwargs: {"applied_count": len(proposals), "applied_examples": proposals},
    )

    result = ps.run_preflight_example_synthesis(
        w=None,
        spark=None,
        run_id="r1",
        space_id="s1",
        config={"_parsed_space": {"instructions": {"example_question_sqls": []}}},
        metadata_snapshot={
            "instructions": {"example_question_sqls": []},
            "data_sources": {"tables": [{"identifier": "cat.sch.stores", "column_configs": []}]},
        },
        benchmarks=[],
        catalog="cat",
        schema="sch",
        target=1,
    )

    assert result["applied"] == 1
    assert result["accepted_examples"] == [proposal]


# ── Preflight hardening tests (2026-05-17-preflight-example-synthesis-hardening) ──


def test_lever_5b_preflight_max_tokens_constant_exists_and_is_sized():
    """LEVER_5B_PREFLIGHT_MAX_TOKENS must exist and be sized per baseline."""
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_5B_PREFLIGHT_MAX_TOKENS")
    assert isinstance(config.LEVER_5B_PREFLIGHT_MAX_TOKENS, int)
    assert 550 <= config.LEVER_5B_PREFLIGHT_MAX_TOKENS <= 800


def test_preflight_example_synthesis_system_msg_constant_exists():
    """PREFLIGHT_EXAMPLE_SYNTHESIS_SYSTEM_MSG must exist and contain
    the role anchors required by Anthropic context-engineering."""
    from genie_space_optimizer.common import config

    assert hasattr(config, "PREFLIGHT_EXAMPLE_SYNTHESIS_SYSTEM_MSG")
    msg = config.PREFLIGHT_EXAMPLE_SYNTHESIS_SYSTEM_MSG
    assert isinstance(msg, str) and len(msg) >= 200
    assert "example_sqls" in msg
    assert "benchmark" in msg.lower()
    assert "JSON" in msg


def test_preflight_prompt_has_two_canonical_examples():
    """The <examples> block must contain 2 worked few-shot examples
    each demonstrating the 4-field JSON output shape."""
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
    )

    prompt = PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT

    assert "<examples>" in prompt and "</examples>" in prompt
    start = prompt.index("<examples>")
    end = prompt.index("</examples>")
    examples_body = prompt[start:end]

    assert examples_body.count("<example>") == 2
    assert examples_body.count("</example>") == 2
    assert examples_body.count('"example_question":') == 2
    assert examples_body.count('"example_sql":') == 2
    assert examples_body.count('"usage_guidance":') == 2
    assert examples_body.count('"rationale":') == 2


def test_preflight_examples_block_precedes_instructions():
    """The <examples> block must appear AFTER </context> but BEFORE <instructions>."""
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
    )

    prompt = PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT
    context_close = prompt.index("</context>")
    examples_open = prompt.index("<examples>")
    instructions_open = prompt.index("<instructions>")

    assert context_close < examples_open < instructions_open
