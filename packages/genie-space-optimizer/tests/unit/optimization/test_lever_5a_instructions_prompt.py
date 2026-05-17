"""Tests for the lever-5a-instructions SKILL.md template rendering,
post-call pipeline, and MLflow observability.

Anchored in:
  - docs/prompt_improvements/2026-05-17-lever-5a-instructions-baseline.md
  - docs/prompt_improvements/2026-05-17-lever-5a-instructions-hardening.md

Tests are appended task-by-task as the hardening plan lands. Each
test name encodes the gap it closes (e.g.
test_max_tokens_constant_is_sized covers baseline gap A1).
"""
from __future__ import annotations

import pytest


# ── Task 2: LEVER_5A_INSTRUCTION_MAX_TOKENS constant ──


def test_max_tokens_constant_is_sized_per_baseline():
    """LEVER_5A_INSTRUCTION_MAX_TOKENS must exist and be sized per
    baseline §4.3.

    Synthetic envelope:
      post-call truncation cap (MAX_HOLISTIC_INSTRUCTION_CHARS) = 8000 chars
      / ~3.6 chars/token (Anthropic English+JSON) = ~2,220 tokens
      x 1.08 headroom = ~2,400 tokens

    Therefore the cap must be >= 2,000 and <= 3,000.
    """
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_5A_INSTRUCTION_MAX_TOKENS"), (
        "LEVER_5A_INSTRUCTION_MAX_TOKENS must be defined in "
        "genie_space_optimizer.common.config"
    )
    assert isinstance(config.LEVER_5A_INSTRUCTION_MAX_TOKENS, int)
    assert 2000 <= config.LEVER_5A_INSTRUCTION_MAX_TOKENS <= 3000, (
        f"LEVER_5A_INSTRUCTION_MAX_TOKENS="
        f"{config.LEVER_5A_INSTRUCTION_MAX_TOKENS} is outside the "
        f"evidence-based band [2000, 3000]"
    )


def test_max_tokens_constant_aligns_with_post_call_truncation_cap():
    """The output max_tokens cap and the post-call truncation cap
    must be in the same neighborhood — otherwise the LLM is either
    being undercut or oversold.
    """
    from genie_space_optimizer.common import config

    chars_to_tokens = config.MAX_HOLISTIC_INSTRUCTION_CHARS / 3.6
    assert abs(
        config.LEVER_5A_INSTRUCTION_MAX_TOKENS - chars_to_tokens
    ) <= chars_to_tokens * 0.15, (
        f"LEVER_5A_INSTRUCTION_MAX_TOKENS="
        f"{config.LEVER_5A_INSTRUCTION_MAX_TOKENS} is too far from "
        f"the post-call truncation cap {chars_to_tokens:.0f} tokens"
    )


# ── Task 2.7: Schema-preservation regression guards ──
#
# Canonical L5a section vocabulary. The output document
# (instructions.text_instructions[0].content) MUST use only these
# 12 ALL-CAPS plain-text headers, in this order, omitting empty
# sections. This list is the contract with the Fix Agent's
# "preserve existing section headers when patching" rule and the
# IQ Scanner check #4 (text-instructions length + SQL-in-text).
LEVER_5A_CANONICAL_SECTIONS: tuple[str, ...] = (
    "PURPOSE:",
    "ASSET ROUTING:",
    "BUSINESS DEFINITIONS:",
    "DISAMBIGUATION:",
    "AGGREGATION RULES:",
    "FUNCTION ROUTING:",
    "JOIN GUIDANCE:",
    "QUERY RULES:",
    "QUERY PATTERNS:",
    "TEMPORAL FILTERS:",
    "DATA QUALITY NOTES:",
    "CONSTRAINTS:",
)


def test_skill_md_canonical_section_list_intact_and_ordered():
    """Regression guard — locks in the 12 canonical L5a section
    headers AND their documented order inside the <instructions>
    block. Catches accidental drops, renames, or reorders.
    """
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    instr = LEVER_5A_INSTRUCTION_PROMPT
    assert "<instructions>" in instr and "</instructions>" in instr, (
        "SKILL.md MUST contain an <instructions>...</instructions> block"
    )
    instr_block = instr[
        instr.index("<instructions>") : instr.index("</instructions>")
    ]

    positions = [
        instr_block.find(h) for h in LEVER_5A_CANONICAL_SECTIONS
    ]
    missing = [
        h for h, p in zip(LEVER_5A_CANONICAL_SECTIONS, positions) if p < 0
    ]
    assert not missing, (
        f"Canonical section header(s) missing from <instructions> "
        f"block: {missing}. If this is intentional, update "
        f"LEVER_5A_CANONICAL_SECTIONS in this test and coordinate "
        f"with the Fix Agent owner per docs/gsl-instruction-schema.md."
    )
    assert positions == sorted(positions), (
        f"Canonical section ORDER has drifted from documented order. "
        f"Found order: "
        f"{[h for _, h in sorted(zip(positions, LEVER_5A_CANONICAL_SECTIONS))]}. "
        f"Expected order: {list(LEVER_5A_CANONICAL_SECTIONS)}."
    )


def test_skill_md_lever_to_section_alignment_table_intact():
    """Regression guard — locks in the 'Lever-to-section alignment'
    table inside the <instructions> block."""
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    instr = LEVER_5A_INSTRUCTION_PROMPT
    instr_block = instr[
        instr.index("<instructions>") : instr.index("</instructions>")
    ]

    required_lever_mappings = [
        "Lever 1 ->",
        "Lever 2 ->",
        "Lever 3 ->",
        "Lever 4 ->",
        "Lever 5 ->",
        "Lever 6 ->",
    ]
    missing = [m for m in required_lever_mappings if m not in instr_block]
    assert not missing, (
        f"Lever-to-section alignment row(s) missing from "
        f"<instructions> block: {missing}. The 6-lever coverage "
        f"contract MUST be present."
    )


def test_skill_md_output_format_rules_intact():
    """Regression guard — locks in the 3 invariant format rules."""
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    instr = LEVER_5A_INSTRUCTION_PROMPT
    instr_block = instr[
        instr.index("<instructions>") : instr.index("</instructions>")
    ]

    invariants = [
        ("ALL-CAPS SECTION HEADERS", "ALL-CAPS plain-text section headers rule"),
        ("PLAIN TEXT", "plain-text (not Markdown) output rule"),
        ("- for bullet", "dash-bullet rule"),
    ]
    for marker, description in invariants:
        assert marker in instr_block, (
            f"Output {description} MUST be present in the "
            f"<instructions> block (marker not found: {marker!r}). "
            f"This rule is referenced by docs/gsl-instruction-schema.md "
            f"and the Fix Agent's header-preservation logic."
        )


# ── Task 3: Wire max_tokens at L5a callsite ──


def test_callsite_wires_max_tokens_from_constant(monkeypatch):
    """The L5a _traced_llm_call at optimizer.py:9550 MUST pass
    max_tokens=LEVER_5A_INSTRUCTION_MAX_TOKENS so the OTPM
    reservation matches the post-call truncation cap.
    """
    from genie_space_optimizer.optimization import optimizer as opt
    from genie_space_optimizer.common import config

    captured_kwargs: dict = {}

    def _fake_traced_llm_call(w, system_msg, prompt, **kwargs):
        captured_kwargs.update(kwargs)
        return (
            '{"instruction_text":"PURPOSE:\\n- ok","rationale":"test"}',
            None,
        )

    monkeypatch.setattr(opt, "_traced_llm_call", _fake_traced_llm_call)

    metadata_snapshot = {
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "config": {"description": ""},
        "general_instructions": [],
    }
    all_clusters = [
        {
            "cluster_id": "H001", "root_cause": "unknown",
            "judge": "schema_accuracy",
            "affected_questions": ["q1"], "suggested_fixes": ["fix1"],
        },
        {
            "cluster_id": "H002", "root_cause": "unknown",
            "judge": "schema_accuracy",
            "affected_questions": ["q2"], "suggested_fixes": ["fix2"],
        },
    ]
    opt._call_llm_for_lever_5a_instructions(
        all_clusters, metadata_snapshot, lever_changes=[],
        w=None, raw_evidence=(),
    )

    assert "max_tokens" in captured_kwargs, (
        "The L5a _traced_llm_call MUST pass max_tokens explicitly "
        "(currently relies on the Databricks default ~2000 which "
        "silently collides with the post-call 8000-char truncation cap)."
    )
    assert (
        captured_kwargs["max_tokens"] == config.LEVER_5A_INSTRUCTION_MAX_TOKENS
    ), (
        f"max_tokens passed to _traced_llm_call="
        f"{captured_kwargs['max_tokens']} but LEVER_5A_INSTRUCTION_MAX_TOKENS="
        f"{config.LEVER_5A_INSTRUCTION_MAX_TOKENS}; wire the constant."
    )


# ── Task 4: MLflow observability tag scaffold ──


def test_callsite_emits_observability_tags(monkeypatch):
    """The L5a callsite MUST set every tag in
    LEVER_5A_OBSERVABILITY_TAG_KEYS on the active MLflow span so
    the post-call pipeline state is visible in MLflow's UI.
    """
    import mlflow
    from genie_space_optimizer.optimization import optimizer as opt
    from genie_space_optimizer.common import config

    captured_tags: dict = {}

    def _fake_update_current_trace(tags=None, **_kw):
        if tags:
            captured_tags.update(tags)

    def _fake_traced_llm_call(w, system_msg, prompt, **kwargs):
        return (
            '{"instruction_text":"PURPOSE:\\n- ok","rationale":"test"}',
            None,
        )

    monkeypatch.setattr(opt, "_traced_llm_call", _fake_traced_llm_call)
    monkeypatch.setattr(mlflow, "update_current_trace", _fake_update_current_trace)

    metadata_snapshot = {
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "config": {"description": ""},
        "general_instructions": [],
    }
    opt._call_llm_for_lever_5a_instructions(
        [{
            "cluster_id": "H001", "root_cause": "unknown",
            "judge": "schema_accuracy",
            "affected_questions": ["q1"], "suggested_fixes": ["fix1"],
        }],
        metadata_snapshot, lever_changes=[], w=None, raw_evidence=(),
    )

    for key in config.LEVER_5A_OBSERVABILITY_TAG_KEYS:
        assert key in captured_tags, (
            f"L5a span MUST set tag '{key}' "
            f"(see LEVER_5A_OBSERVABILITY_TAG_KEYS in common/config.py)."
        )
    assert captured_tags["validate_no_sql_result"] == "pass"
    assert captured_tags["rca_contract_version"] == "v1"
    assert captured_tags["system_msg_version"] in {"v1", "v2-slim"}
    assert (
        captured_tags["max_section_chars"] == "0"
        or captured_tags["max_section_chars"].isdigit()
    ), (
        f"max_section_chars MUST be a non-negative integer string. "
        f"Got: {captured_tags['max_section_chars']!r}"
    )


# ── Task 5: _repair_truncated_l5a_json ──


def test_repair_truncated_l5a_json_extracts_clean_pair():
    """When the L5a LLM response is cut off mid-string, the repair
    function MUST salvage the instruction_text + rationale pair."""
    from genie_space_optimizer.optimization.optimizer import (
        _repair_truncated_l5a_json,
    )

    truncated = (
        '{"instruction_text":"PURPOSE:\\nThis space covers booking '
        'analytics.\\n\\nASSET ROUTING:\\n- Use fact_bookings for"'
    )
    result = _repair_truncated_l5a_json(truncated)
    assert isinstance(result, dict)
    assert "instruction_text" in result
    assert "PURPOSE:" in result["instruction_text"]
    assert "rationale" in result
    assert isinstance(result["rationale"], str)


def test_repair_truncated_l5a_json_does_not_include_example_sql_proposals():
    """Defensive: L5a's schema has no example_sql_proposals field."""
    from genie_space_optimizer.optimization.optimizer import (
        _repair_truncated_l5a_json,
    )

    bogus = (
        '{"instruction_text":"PURPOSE: ok","example_sql_proposals":'
        '[{"foo":"bar"}],"rationale":"x"}'
    )
    result = _repair_truncated_l5a_json(bogus)
    assert "example_sql_proposals" not in result


def test_repair_truncated_l5a_json_returns_empty_dict_on_unparseable_input():
    """Last-ditch: if even the regex can't extract instruction_text,
    return an empty-string default."""
    from genie_space_optimizer.optimization.optimizer import (
        _repair_truncated_l5a_json,
    )

    result = _repair_truncated_l5a_json("not even JSON-ish")
    assert isinstance(result, dict)
    assert result.get("instruction_text", "MISSING") == ""


def test_l5a_callsite_uses_l5a_specific_repair_not_holistic(monkeypatch):
    """Regression: optimizer.py was calling _repair_truncated_holistic_json
    on L5a output. This test fails if the L5a path reverts."""
    from genie_space_optimizer.optimization import optimizer as opt

    repair_calls: list[str] = []

    def _fake_l5a_repair(text):
        repair_calls.append("l5a")
        return {"instruction_text": "PURPOSE: ok", "rationale": "salvaged"}

    def _fake_holistic_repair(text):
        repair_calls.append("holistic")
        return {
            "instruction_text": "x",
            "example_sql_proposals": [],
            "rationale": "y",
        }

    def _fake_traced_llm_call(w, system_msg, prompt, **kwargs):
        return ('{"instruction_text":"PURPOSE:\\n- truncated mid-stri', None)

    monkeypatch.setattr(opt, "_repair_truncated_l5a_json", _fake_l5a_repair)
    monkeypatch.setattr(
        opt, "_repair_truncated_holistic_json", _fake_holistic_repair,
    )
    monkeypatch.setattr(opt, "_traced_llm_call", _fake_traced_llm_call)

    metadata_snapshot = {
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "config": {"description": ""},
        "general_instructions": [],
    }
    opt._call_llm_for_lever_5a_instructions(
        [{"cluster_id": "H001"}], metadata_snapshot,
        lever_changes=[], w=None, raw_evidence=(),
    )

    assert "l5a" in repair_calls, (
        "L5a callsite MUST call _repair_truncated_l5a_json"
    )
    assert "holistic" not in repair_calls, (
        "L5a callsite MUST NOT call _repair_truncated_holistic_json "
        "(wrong output shape — see baseline §6.A2)."
    )


# ── Task 6: Slim system message ──


def test_lever_5a_system_message_constants_defined():
    """Both versions of the system message MUST be exported as
    constants so the deprecated version is grep-able for one release
    and the active version is single-sourced.
    """
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_5A_INSTRUCTION_SYSTEM_MSG_V1_DEPRECATED")
    assert hasattr(config, "LEVER_5A_INSTRUCTION_SYSTEM_MSG")
    assert len(config.LEVER_5A_INSTRUCTION_SYSTEM_MSG) < len(
        config.LEVER_5A_INSTRUCTION_SYSTEM_MSG_V1_DEPRECATED
    )
    assert (
        "example_sql_proposals" not in config.LEVER_5A_INSTRUCTION_SYSTEM_MSG
    ), (
        "The 'example_sql_proposals' guard is obsolete with strict: "
        "true typed-IO + additionalProperties: false."
    )
    assert any(
        kw in config.LEVER_5A_INSTRUCTION_SYSTEM_MSG.lower()
        for kw in ("plain text", "no markdown", "no formatting")
    ), (
        "V2 system message MUST explicitly tell the LLM that "
        "instruction_text contents are plain text only (baseline §6.D3)."
    )


def test_callsite_uses_v2_slim_system_message(monkeypatch):
    """The L5a callsite MUST pass the v2-slim system message to
    _traced_llm_call AND tag the trace with system_msg_version=v2-slim."""
    import mlflow
    from genie_space_optimizer.optimization import optimizer as opt
    from genie_space_optimizer.common import config

    captured: dict = {"system_msg": None, "tags": {}}

    def _fake_traced_llm_call(w, system_msg, prompt, **kwargs):
        captured["system_msg"] = system_msg
        return ('{"instruction_text":"PURPOSE: ok","rationale":"t"}', None)

    def _fake_update_current_trace(tags=None, **_kw):
        if tags:
            captured["tags"].update(tags)

    monkeypatch.setattr(opt, "_traced_llm_call", _fake_traced_llm_call)
    monkeypatch.setattr(
        mlflow, "update_current_trace", _fake_update_current_trace,
    )

    metadata_snapshot = {
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "config": {"description": ""},
        "general_instructions": [],
    }
    opt._call_llm_for_lever_5a_instructions(
        [{"cluster_id": "H001"}], metadata_snapshot,
        lever_changes=[], w=None, raw_evidence=(),
    )

    assert captured["system_msg"] == config.LEVER_5A_INSTRUCTION_SYSTEM_MSG, (
        "L5a callsite must source system_msg from the constant in "
        "common/config.py — do not inline."
    )
    assert captured["tags"].get("system_msg_version") == "v2-slim"


# ── Task 8: XML sub-tag migration for <context> slots ──


def test_lever_5a_skill_md_uses_xml_subtag_slots():
    """The 8 <context> slots MUST be wrapped in XML sub-tags so the
    model can attend to slot boundaries (Anthropic context
    engineering).
    """
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    expected_open_tags = [
        "<raw_failure_evidence>",
        "<space_description>",
        "<eval_summary>",
        "<failure_clusters>",
        "<lever_summary>",
        "<current_instructions>",
        "<existing_example_sqls>",
        "<identifier_allowlist>",
    ]
    for tag in expected_open_tags:
        assert tag in LEVER_5A_INSTRUCTION_PROMPT, (
            f"SKILL.md MUST wrap the {tag[1:-1]} slot in {tag}...</{tag[1:-1]}> "
            f"XML sub-tags per Anthropic context engineering "
            f"(baseline §6.B2)."
        )
        close = f"</{tag[1:-1]}>"
        assert close in LEVER_5A_INSTRUCTION_PROMPT, (
            f"SKILL.md MUST have matching closing {close} tag."
        )


def test_lever_5a_skill_md_does_not_use_markdown_h2_headers_in_context():
    """Regression: Markdown h2 headers must NOT appear in the
    <context> block after Task 8."""
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    ctx_start = LEVER_5A_INSTRUCTION_PROMPT.index("<context>")
    ctx_end = LEVER_5A_INSTRUCTION_PROMPT.index("</context>")
    context_block = LEVER_5A_INSTRUCTION_PROMPT[ctx_start:ctx_end]

    forbidden = [
        "## Raw Failure Evidence",
        "## Genie Space Purpose",
        "## Evaluation Summary",
        "## Failure Clusters from Evaluation",
        "## Changes Already Applied by Earlier Levers",
        "## Current Text Instructions",
        "## Existing Example SQL Queries",
        "## Identifier Allowlist",
    ]
    for header in forbidden:
        assert header not in context_block, (
            f"Markdown header '{header}' still present in <context> "
            f"block after Task 8. Replace with XML sub-tag."
        )


# ── Task 9: Output budget label + soft sizing + max_section_chars tag ──


def test_skill_md_uses_output_budget_not_input_budget_for_output_guidance():
    """SKILL.md MUST reference the OUTPUT budget for output sizing,
    not the input prompt budget."""
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    assert (
        "{{ instruction_output_char_budget }}" in LEVER_5A_INSTRUCTION_PROMPT
    ), (
        "SKILL.md MUST reference {{ instruction_output_char_budget }} "
        "for output sizing guidance (baseline §6.C1)."
    )
    instr_start = LEVER_5A_INSTRUCTION_PROMPT.index("<instructions>")
    instr_end = LEVER_5A_INSTRUCTION_PROMPT.index("</instructions>")
    instr_block = LEVER_5A_INSTRUCTION_PROMPT[instr_start:instr_end]
    assert "{{ instruction_char_budget }}" not in instr_block, (
        "SKILL.md <instructions> block MUST use "
        "instruction_output_char_budget — instruction_char_budget "
        "is the INPUT prompt budget (mislabeled per baseline §6.C1)."
    )


def test_callsite_populates_output_budget_slot(monkeypatch):
    """The L5a callsite MUST populate the new slot with
    MAX_HOLISTIC_INSTRUCTION_CHARS (the actual output cap)."""
    from genie_space_optimizer.optimization import optimizer as opt
    from genie_space_optimizer.common.config import MAX_HOLISTIC_INSTRUCTION_CHARS

    captured: dict = {"prompt": None}

    def _fake_traced_llm_call(w, system_msg, prompt, **kwargs):
        captured["prompt"] = prompt
        return ('{"instruction_text":"PURPOSE: ok","rationale":"t"}', None)

    monkeypatch.setattr(opt, "_traced_llm_call", _fake_traced_llm_call)

    metadata_snapshot = {
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "config": {"description": ""},
        "general_instructions": [],
    }
    opt._call_llm_for_lever_5a_instructions(
        [{"cluster_id": "H001"}], metadata_snapshot,
        lever_changes=[], w=None, raw_evidence=(),
    )

    assert str(MAX_HOLISTIC_INSTRUCTION_CHARS) in captured["prompt"], (
        f"The rendered L5a prompt MUST contain the output budget "
        f"{MAX_HOLISTIC_INSTRUCTION_CHARS} so the LLM knows the "
        f"truncation cap."
    )


def test_skill_md_output_sizing_guidance_is_terser():
    """Output guidance MUST encourage terse bullets without
    introducing a hard per-section cap."""
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    must_contain = ["Target 30-60 lines", "Each bullet ≤ 200 chars"]
    for marker in must_contain:
        assert marker in LEVER_5A_INSTRUCTION_PROMPT, (
            f"SKILL.md output sizing guidance MUST contain '{marker}' "
            f"per baseline §6.C2."
        )

    forbidden_hard_caps = [
        "MAXIMUM per section",
        "Each section MUST be",
        "MUST NOT exceed 1000 chars per section",
        "MUST NOT exceed 800 chars per section",
    ]
    for phrase in forbidden_hard_caps:
        assert phrase not in LEVER_5A_INSTRUCTION_PROMPT, (
            f"Per-section sizing MUST be SOFT guidance, not a hard "
            f"cap. Found forbidden phrasing: {phrase!r}."
        )
    assert (
        "may legitimately need more space" in LEVER_5A_INSTRUCTION_PROMPT
    ), (
        "The 'may legitimately need more space' escape-hatch phrase "
        "MUST be present to prevent over-trimming."
    )


def test_callsite_emits_max_section_chars_observability_tag(monkeypatch):
    """Task 9 adds a max_section_chars MLflow span tag."""
    import mlflow
    from genie_space_optimizer.optimization import optimizer as opt

    captured: dict = {"tags": {}}

    def _fake_traced_llm_call(w, system_msg, prompt, **kwargs):
        return (
            '{"instruction_text":"PURPOSE:\\n- short.\\n\\n'
            'BUSINESS DEFINITIONS:\\n- bullet one with some content.\\n'
            '- bullet two with more content.\\n- bullet three.\\n\\n'
            'QUERY RULES:\\n- one rule.","rationale":"t"}',
            None,
        )

    def _fake_update_current_trace(tags=None, **_kw):
        if tags:
            captured["tags"].update(tags)

    monkeypatch.setattr(opt, "_traced_llm_call", _fake_traced_llm_call)
    monkeypatch.setattr(mlflow, "update_current_trace", _fake_update_current_trace)

    metadata_snapshot = {
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "config": {"description": ""},
        "general_instructions": [],
    }
    opt._call_llm_for_lever_5a_instructions(
        [{"cluster_id": "H001"}], metadata_snapshot,
        lever_changes=[], w=None, raw_evidence=(),
    )

    assert "max_section_chars" in captured["tags"]
    max_section_chars = int(captured["tags"]["max_section_chars"])
    assert max_section_chars > 0, (
        f"max_section_chars MUST be > 0 for a non-empty multi-section "
        f"response, got {max_section_chars}."
    )


def test_compute_max_section_chars_for_l5a_helper():
    """Helper computes the size of the longest canonical section."""
    from genie_space_optimizer.optimization.optimizer import (
        _compute_max_section_chars_for_l5a,
    )

    text = (
        "PURPOSE:\nshort\n\n"
        "BUSINESS DEFINITIONS:\n"
        "- a long bullet line\n- another\n- one more\n\n"
        "QUERY RULES:\nx"
    )
    longest = _compute_max_section_chars_for_l5a(text)
    # BUSINESS DEFINITIONS body is the longest (3 bullets).
    assert longest > len("PURPOSE:\nshort\n\n")
    # Empty / no sections returns 0.
    assert _compute_max_section_chars_for_l5a("") == 0
    assert _compute_max_section_chars_for_l5a("no headers here") == 0


# ── Task 10: Expand examples from 1 to 3 canonical patterns ──


def test_skill_md_has_at_least_three_canonical_examples():
    """Per Anthropic context engineering, 2+ examples spanning input
    variance outperform a single generic example."""
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    example_count = LEVER_5A_INSTRUCTION_PROMPT.count("<example>")
    assert example_count >= 3, (
        f"SKILL.md MUST have >=3 <example> entries (current count: "
        f"{example_count}). Baseline §6.B3 targets a ROUTING/TEMPORAL "
        f"example, a DISAMBIGUATION/JOIN example, and an empty-output."
    )


def test_skill_md_examples_cover_distinct_pattern_keywords():
    """Each example MUST cover a distinct pattern."""
    from genie_space_optimizer.common.config import LEVER_5A_INSTRUCTION_PROMPT

    examples_start = LEVER_5A_INSTRUCTION_PROMPT.index("<examples>")
    examples_end = LEVER_5A_INSTRUCTION_PROMPT.index("</examples>")
    block = LEVER_5A_INSTRUCTION_PROMPT[examples_start:examples_end]

    assert "DISAMBIGUATION" in block, (
        "At least one example MUST cover DISAMBIGUATION (most common "
        "production pattern per the baseline trace sample)."
    )
    assert "JOIN GUIDANCE" in block, (
        "At least one example MUST cover JOIN GUIDANCE."
    )
    assert (
        '"instruction_text": ""' in block
        or '\\"instruction_text\\": \\"\\"' in block
    ), (
        "At least one example MUST demonstrate the empty-output case "
        "(instruction_text=\"\") to ground the no-fix sentinel."
    )
