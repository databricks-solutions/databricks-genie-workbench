"""Tests for the lever-6 SQL expression prompt scaffolding and validation hooks.

Plan reference: docs/prompt_improvements/2026-05-17-lever-6-prompt-hardening.md
"""
from __future__ import annotations

import pytest


def test_lever_6_max_tokens_constant_exists_and_is_sized():
    """LEVER_6_MAX_TOKENS must exist and be sized per empirical evidence.

    Trial-5 baseline (see 2026-05-17-lever6-empirical-baseline.md):
      - P95 output tokens: 500
      - max observed: 1,011 (single trace, double-snippet outlier)
    Therefore the cap must be > 1,011 (covers the observed max) and
    < 2,000 (stays well under the 2,500 Stage-1 ceiling, so multiple
    concurrent lever-6 calls fit inside the 20K OTPM endpoint budget).
    """
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_6_MAX_TOKENS"), (
        "LEVER_6_MAX_TOKENS must be defined in genie_space_optimizer.common.config"
    )
    assert isinstance(config.LEVER_6_MAX_TOKENS, int)
    assert 1_011 < config.LEVER_6_MAX_TOKENS < 2_000, (
        f"LEVER_6_MAX_TOKENS={config.LEVER_6_MAX_TOKENS} is outside the "
        f"evidence-based band (1011, 2000)"
    )


def test_generate_lever6_proposal_passes_max_tokens_to_llm_call(monkeypatch):
    """_generate_lever6_proposal must forward LEVER_6_MAX_TOKENS to _traced_llm_call.

    Without the explicit forward, Databricks reserves ~4,096 tokens of OTPM per
    call which starves concurrent lever-6 calls inside the 20K OTPM endpoint.
    """
    from genie_space_optimizer.common import config
    from genie_space_optimizer.optimization import optimizer

    captured: dict = {}

    def _fake_traced_llm_call(w, system_msg, prompt, *, span_name, **kwargs):
        captured["max_tokens"] = kwargs.get("max_tokens")
        # Return a valid-shape response so the function continues past the call
        # and the rest of validation logic doesn't fail this test.
        return (
            '{"snippet_type": "measure", "display_name": "X", "alias": "x", '
            '"sql": "SUM(t.c)", "synonyms": [], "instruction": "i", '
            '"rationale": "r", "target_table": "t", "affected_questions": []}',
            None,
        )

    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_traced_llm_call)
    # Force the validation/snippet-count branches that follow to short-circuit:
    monkeypatch.setattr(
        optimizer, "_validate_sql_identifiers", lambda sql, allow: (False, ["forced-stop"]),
    )

    cluster = {
        "cluster_id": "c1", "root_cause": "missing_filter",
        "question_ids": ["q1"], "question_traces": [{"q": "q1"}],
    }
    optimizer._generate_lever6_proposal(
        cluster, metadata_snapshot={"data_sources": {"tables": []}},
        w=None,
    )

    assert captured.get("max_tokens") == config.LEVER_6_MAX_TOKENS, (
        f"Expected max_tokens={config.LEVER_6_MAX_TOKENS}, got {captured.get('max_tokens')!r}"
    )
