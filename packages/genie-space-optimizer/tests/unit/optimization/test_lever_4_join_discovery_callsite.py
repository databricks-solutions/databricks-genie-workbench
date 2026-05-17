"""Callsite integration tests for lever_4_join_discovery.

Plan: docs/prompt_improvements/2026-05-17-lever-4-join-discovery-hardening.md
"""
from __future__ import annotations

import pytest


# ── Task 1: closed observability tag set ──


def test_l4_observability_tag_keys_are_closed():
    """The set of MLflow tags emitted by the L4 callsite is closed.

    Adding a new tag requires extending LEVER_4_OBSERVABILITY_TAG_KEYS
    AND extending this test in lockstep.
    """
    from genie_space_optimizer.common.config import (
        LEVER_4_OBSERVABILITY_TAG_KEYS,
    )

    assert LEVER_4_OBSERVABILITY_TAG_KEYS == frozenset({
        "prompt_version",
        "system_msg_version",
        "pydantic_validation_status",
        "markdown_fence_stripped",
        "instruction_coerced_to_string",
        "input_truncated",
        "sanitize_made_changes",
        "mv_join_emitted",
        "existing_specs_rendered_chars",
        "hints_truncated",
        "raw_evidence_block_version",
        "repair_used",
        "rca_contract_version",
        "alias_overridden",
        "relationship_type_invalid",
    })


def test_set_l4_observability_tags_emits_all_keys_with_prefix():
    """_set_l4_observability_tags emits all 15 keys prefixed with 'l4.'."""
    from genie_space_optimizer.common.config import (
        LEVER_4_OBSERVABILITY_TAG_KEYS,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _set_l4_observability_tags,
    )

    captured: dict = {}

    class _SpanStub:
        def set_attribute(self, k, v):
            captured[k] = v

    _set_l4_observability_tags(_SpanStub())
    keys_no_prefix = {k.removeprefix("l4.") for k in captured if k.startswith("l4.")}
    assert keys_no_prefix == LEVER_4_OBSERVABILITY_TAG_KEYS


def test_set_l4_observability_tags_is_safe_on_none_span():
    """Helper must be a no-op when span is None (legacy test stubs)."""
    from genie_space_optimizer.optimization.optimizer import (
        _set_l4_observability_tags,
    )

    # Should not raise.
    _set_l4_observability_tags(None)


# ── Task 10: _normalize_instruction ──


def test_normalize_instruction_strips_markdown_fence():
    from genie_space_optimizer.optimization.optimizer import (
        _normalize_instruction,
    )
    s, changed, truncated = _normalize_instruction(
        "```\nuse this join when X\n```",
    )
    assert s == "use this join when X"
    assert changed is True
    assert truncated is False


def test_normalize_instruction_coerces_list_input_to_string():
    from genie_space_optimizer.optimization.optimizer import (
        _normalize_instruction,
    )
    s, changed, truncated = _normalize_instruction(["line 1", "line 2"])
    assert s == "line 1\nline 2"
    assert changed is True


def test_normalize_instruction_soft_caps_at_threshold(monkeypatch):
    from genie_space_optimizer.optimization.optimizer import (
        _normalize_instruction,
    )
    from genie_space_optimizer.common import config as cfg
    monkeypatch.setattr(cfg, "LEVER_4_INSTRUCTION_SOFT_CAP", 50)
    long = "x" * 200
    s, changed, truncated = _normalize_instruction(long)
    assert truncated is True
    assert len(s) <= 50 + len(" […truncated]")


def test_normalize_instruction_passes_through_clean_input():
    from genie_space_optimizer.optimization.optimizer import (
        _normalize_instruction,
    )
    s, changed, truncated = _normalize_instruction("Plain string, no markdown.")
    assert s == "Plain string, no markdown."
    assert changed is False
    assert truncated is False


# ── Task 11: _normalize_join_endpoint ──


def test_normalize_join_endpoint_derives_alias_from_identifier():
    from genie_space_optimizer.optimization.optimizer import (
        _normalize_join_endpoint,
    )
    ep, overridden = _normalize_join_endpoint(
        {"identifier": "cat.sch.fact_sales", "alias": "fact"},
    )
    assert ep["alias"] == "fact_sales"
    assert overridden is True


def test_normalize_join_endpoint_passes_through_correct_alias():
    from genie_space_optimizer.optimization.optimizer import (
        _normalize_join_endpoint,
    )
    ep, overridden = _normalize_join_endpoint(
        {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
    )
    assert ep["alias"] == "fact_sales"
    assert overridden is False


def test_normalize_join_endpoint_handles_missing_alias():
    from genie_space_optimizer.optimization.optimizer import (
        _normalize_join_endpoint,
    )
    ep, overridden = _normalize_join_endpoint(
        {"identifier": "cat.sch.fact_sales"},
    )
    assert ep["alias"] == "fact_sales"
    assert overridden is True


# ── Task 12: _format_existing_join_specs_compact ──


def test_format_existing_join_specs_compact_renders_one_line_per_spec():
    from genie_space_optimizer.optimization.optimizer import (
        _format_existing_join_specs_compact,
    )
    specs = [
        {
            "left": {"identifier": "cat.sch.a", "alias": "a"},
            "right": {"identifier": "cat.sch.b", "alias": "b"},
            "sql": [
                "`a`.`x` = `b`.`x`",
                "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
            ],
        },
        {
            "left": {"identifier": "cat.sch.c", "alias": "c"},
            "right": {"identifier": "cat.sch.d", "alias": "d"},
            "sql": [
                "`c`.`y` = `d`.`y`",
                "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_ONE--",
            ],
        },
    ]
    out = _format_existing_join_specs_compact(specs)
    lines = out.strip().splitlines()
    assert len(lines) == 2
    assert "a <-> b" in lines[0]
    assert "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE" in lines[0]
    assert "c <-> d" in lines[1]


def test_format_existing_join_specs_compact_handles_empty():
    from genie_space_optimizer.optimization.optimizer import (
        _format_existing_join_specs_compact,
    )
    assert _format_existing_join_specs_compact([]).strip() == "(none)"


# ── Task 13: _cap_discovery_hints ──


def test_cap_discovery_hints_truncates_past_k(monkeypatch):
    from genie_space_optimizer.optimization.optimizer import (
        _cap_discovery_hints,
    )
    from genie_space_optimizer.common import config as cfg
    monkeypatch.setattr(cfg, "LEVER_4_HINTS_TOP_K", 5)
    hints = [{"i": i} for i in range(10)]
    out, truncated = _cap_discovery_hints(hints)
    assert len(out) == 5
    assert truncated is True


def test_cap_discovery_hints_passes_through_under_k():
    from genie_space_optimizer.optimization.optimizer import (
        _cap_discovery_hints,
    )
    hints = [{"i": i} for i in range(3)]
    out, truncated = _cap_discovery_hints(hints)
    assert len(out) == 3
    assert truncated is False


# ── Task 14: _l4_response_appears_truncated ──


def test_repair_l4_detects_truncation():
    from genie_space_optimizer.optimization.optimizer import (
        _l4_response_appears_truncated,
    )
    assert not _l4_response_appears_truncated(
        '{"join_specs": [], "rationale": "ok"}',
    )
    assert _l4_response_appears_truncated(
        '{"join_specs": [{"left": {"identifier": "cat.sch.a',
    )
    assert _l4_response_appears_truncated(
        '{"join_specs": [{"left": {"identifier": "cat.sch.a", "alias": "a"},',
    )
