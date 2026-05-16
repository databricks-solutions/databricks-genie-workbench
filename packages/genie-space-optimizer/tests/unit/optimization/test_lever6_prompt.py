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


def test_lever6_rejects_proposal_with_affected_questions_outside_cluster(monkeypatch):
    """A Lever-6 proposal whose affected_questions contains an ID not in the
    cluster's question_ids must be rejected with proposal=None.

    Empirical baseline (2026-05-17-lever6-empirical-baseline.md): 7/48 Trial-5
    lever-6 calls generated valid JSON that contained ["H001", "H002"]
    (cluster IDs) instead of real question IDs. The applier downstream
    silently dropped these — this test forces the rejection to happen
    inside _generate_lever6_proposal so the trace records why.
    """
    from genie_space_optimizer.optimization import optimizer

    def _fake_traced_llm_call(w, system_msg, prompt, *, span_name, **kwargs):
        # Simulate the exact failure pattern from Trial-5: model returns
        # valid JSON but affected_questions = ["H001", "H002"] (cluster IDs).
        return (
            '{"snippet_type": "measure", "display_name": "X", "alias": "x", '
            '"sql": "SUM(tkt_document.TOTAL_FARE_USD_AMT)", "synonyms": [], '
            '"instruction": "i", "rationale": "r", '
            '"target_table": "tkt_document", '
            '"affected_questions": ["H001", "H002"]}',
            None,
        )

    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_traced_llm_call)
    # Bypass SQL identifier validation — we want to test the affected_questions
    # gate specifically, not get rejected by an unrelated validator.
    monkeypatch.setattr(
        optimizer, "_validate_sql_identifiers", lambda sql, allow: (True, []),
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "missing_filter",
        "question_ids": ["q42", "q43"],       # real qids — H001 is the CLUSTER id
        "question_traces": [{"q": "q42"}, {"q": "q43"}],
    }
    result = optimizer._generate_lever6_proposal(
        cluster,
        metadata_snapshot={"data_sources": {"tables": []}},
        w=None,
    )
    assert result is None, (
        "Expected None (proposal rejected) when affected_questions contains "
        f"IDs outside cluster.question_ids; got: {result!r}"
    )


def test_lever6_accepts_proposal_with_affected_questions_subset(monkeypatch):
    """The positive complement of the above: a proposal whose affected_questions
    is a subset of cluster.question_ids must NOT be rejected by this gate.
    """
    from genie_space_optimizer.optimization import optimizer

    def _fake_traced_llm_call(w, system_msg, prompt, *, span_name, **kwargs):
        return (
            '{"snippet_type": "measure", "display_name": "X", "alias": "x", '
            '"sql": "SUM(tkt_document.TOTAL_FARE_USD_AMT)", "synonyms": [], '
            '"instruction": "i", "rationale": "r", '
            '"target_table": "tkt_document", '
            '"affected_questions": ["q42"]}',
            None,
        )

    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_traced_llm_call)
    monkeypatch.setattr(
        optimizer, "_validate_sql_identifiers", lambda sql, allow: (True, []),
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "missing_filter",
        "question_ids": ["q42", "q43"],
        "question_traces": [{"q": "q42"}, {"q": "q43"}],
    }
    result = optimizer._generate_lever6_proposal(
        cluster,
        metadata_snapshot={"data_sources": {"tables": []}},
        w=None,
    )
    # With monkeypatched _validate_sql_identifiers we expect to reach the
    # dedupe step which short-circuits on empty existing. Concretely: with
    # metadata_snapshot.sql_snippets absent, dedupe is a no-op and the
    # function returns a proposal dict, not None.
    assert isinstance(result, dict), (
        f"Expected dict (proposal accepted), got: {result!r}"
    )
    assert result["affected_questions"] == ["q42"]


def test_failure_type_to_snippet_type_routing_table_renders_typed_pairs():
    """The routing table must list every typed failure_type with its
    preferred snippet_type, in a stable order, with explanatory rationale.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _render_failure_type_to_snippet_type_table,
    )

    rendered = _render_failure_type_to_snippet_type_table()
    assert isinstance(rendered, str)
    # Each typed failure_type must appear with its snippet_type on the
    # same line so the LLM sees them as a pair.
    expected_pairs = [
        ("plural_top_n_collapse", "expression"),
        ("missing_filter", "filter"),
        ("wrong_filter_condition", "filter"),
        ("wrong_aggregation", "measure"),
        ("missing_dimension", "expression"),
        ("currency_or_unit_mismatch", "measure"),
    ]
    for failure, snippet in expected_pairs:
        matching_lines = [
            ln for ln in rendered.splitlines() if failure in ln
        ]
        assert matching_lines, (
            f"failure_type {failure!r} not present in routing table:\n{rendered}"
        )
        assert any(snippet in ln for ln in matching_lines), (
            f"failure_type {failure!r} found but expected snippet_type "
            f"{snippet!r} not on same line:\n{matching_lines!r}"
        )
    # Free-form prose escape hatch must be documented.
    assert "free-form" in rendered.lower() or "prose" in rendered.lower(), (
        "Routing table must document the prose escape hatch (LLM choice)"
    )


def test_lever6_prompt_template_has_failure_type_routing_slot():
    """The lever-6 prompt must include the {{ failure_type_routing_table }}
    slot in the Task section. Without this, the helper is dead code.
    """
    from genie_space_optimizer.common.config import LEVER_6_SQL_EXPRESSION_PROMPT

    assert "{{ failure_type_routing_table }}" in LEVER_6_SQL_EXPRESSION_PROMPT, (
        "LEVER_6_SQL_EXPRESSION_PROMPT must contain the "
        "{{ failure_type_routing_table }} slot"
    )


def test_generate_lever6_proposal_renders_routing_table_into_prompt(monkeypatch):
    """The caller must thread the rendered routing table into the template.
    Reach this by capturing the prompt argument to _traced_llm_call.
    """
    from genie_space_optimizer.optimization import optimizer

    captured: dict = {}

    def _fake_traced_llm_call(w, system_msg, prompt, *, span_name, **kwargs):
        captured["prompt"] = prompt
        return (
            '{"snippet_type": "filter", "display_name": "X", "alias": "", '
            '"sql": "tkt_document.PNR_LOCATOR_ID IS NOT NULL", "synonyms": [], '
            '"instruction": "i", "rationale": "r", "target_table": "tkt_document", '
            '"affected_questions": []}',
            None,
        )

    monkeypatch.setattr(optimizer, "_traced_llm_call", _fake_traced_llm_call)
    monkeypatch.setattr(
        optimizer, "_validate_sql_identifiers", lambda sql, allow: (False, ["stop"]),
    )
    cluster = {
        "cluster_id": "c1", "root_cause": "missing_filter",
        "question_ids": ["q1"], "question_traces": [{"q": "q1"}],
    }
    optimizer._generate_lever6_proposal(
        cluster, metadata_snapshot={"data_sources": {"tables": []}}, w=None,
    )
    prompt = captured.get("prompt", "")
    # The routing table renders into the prompt — assert one anchor row.
    assert "plural_top_n_collapse" in prompt, (
        "Routing table not rendered into lever-6 prompt"
    )
    assert "| `missing_filter` | `filter` |" in prompt, (
        "Routing table row not rendered correctly"
    )
