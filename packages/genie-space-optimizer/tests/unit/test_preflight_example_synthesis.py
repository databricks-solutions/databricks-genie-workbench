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


def test_preflight_worked_example_uses_real_identifier_pattern():
    """Worked-example BAD line must use a realistic identifier pattern
    — NOT the literal 'mv_<domain>_dim_date' placeholder which is not
    a real identifier in any production schema."""
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
    )

    assert "mv_<domain>_dim_date" not in PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT


def test_preflight_output_schema_no_literal_double_braces():
    """The <output_schema> block must NOT contain a literal `{{...}}`
    JSON sketch (Task 3 replaced it with a field-by-field description)."""
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
    )

    prompt = PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT
    start = prompt.index("<output_schema>")
    end = prompt.index("</output_schema>")
    schema_body = prompt[start:end]

    assert "{{" not in schema_body
    assert "}}" not in schema_body


# ── Task 10: XML migration of <context> sub-headers ──


def test_preflight_context_uses_xml_subtags():
    """The <context> block must use XML sub-tags, not Markdown ## headers.

    This is the structural correctness anchor for Anthropic context-
    engineering: a model recognizes nested XML tags much more reliably
    than free-text Markdown sub-headers, especially when the surrounding
    contract is also in XML.
    """
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
    )

    prompt = PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT
    ctx_start = prompt.index("<context>")
    ctx_end = prompt.index("</context>")
    ctx = prompt[ctx_start:ctx_end]

    # Required XML sub-tags inside <context>.
    for tag in (
        "<coverage_focus>",
        "</coverage_focus>",
        "<identifier_qualification_constraint>",
        "</identifier_qualification_constraint>",
        "<filter_values_constraint>",
        "</filter_values_constraint>",
        "<archetype>",
        "</archetype>",
        "<schema>",
        "</schema>",
        "<existing_questions>",
        "</existing_questions>",
    ):
        assert tag in ctx, f"missing XML sub-tag {tag} in <context>"

    # The migrated ## sub-headers must be gone from <context>.
    for legacy_header in (
        "## Coverage focus",
        "## Constraint: identifier qualification",
        "## Constraint: filter values",
        "## Archetype",
        "## Schema",
        "## Existing questions",
    ):
        assert legacy_header not in ctx, (
            f"legacy markdown header {legacy_header!r} still in <context>"
        )


# ── Task 9.4: data_profile_section is conditional + never emits "(no profile available)" ──


def test_preflight_body_uses_data_profile_section_placeholder():
    """The body template must use ``{{ data_profile_section }}`` rather
    than ``## Column value profile`` + ``{{ slice_data_profile }}`` — the
    helper now wraps the profile in ``<column_value_profile>`` only when
    a real profile is present, and emits an empty string otherwise.
    """
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
    )

    prompt = PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT
    assert "{{ data_profile_section }}" in prompt
    assert "{{ slice_data_profile }}" not in prompt
    # The static markdown header must be gone — the helper renders its
    # own XML wrapper when a profile is present.
    assert "## Column value profile" not in prompt


def test_render_preflight_prompt_omits_no_profile_available_when_no_profile():
    """When ``data_profile`` is ``None``, the rendered prompt must NOT
    contain the literal ``(no profile available)`` — that placeholder
    only confuses the LLM. The new helper drops the section entirely.
    """
    from genie_space_optimizer.optimization.preflight_synthesis import (
        AssetSlice,
        ARCHETYPES,
        render_preflight_prompt,
    )

    slice_ = AssetSlice(
        tables=[{"identifier": "cat.sch.stores"}],
        columns=[("cat.sch.stores", "region")],
    )
    prompt = render_preflight_prompt(ARCHETYPES[0], slice_, [])

    assert "(no profile available)" not in prompt
    assert "<column_value_profile>" not in prompt


def test_render_preflight_prompt_wraps_profile_in_xml_when_present():
    """When ``data_profile`` IS supplied, the helper must wrap the
    profile in ``<column_value_profile>`` so the LLM can locate it.
    """
    from genie_space_optimizer.optimization.preflight_synthesis import (
        AssetSlice,
        ARCHETYPES,
        render_preflight_prompt,
    )

    slice_ = AssetSlice(
        tables=[{"identifier": "cat.sch.stores"}],
        columns=[("cat.sch.stores", "region")],
    )
    data_profile = {
        "cat.sch.stores": {
            "columns": {
                "region": {"distinct_values": ["NA", "EMEA", "APAC"]},
            },
        }
    }
    prompt = render_preflight_prompt(
        ARCHETYPES[0], slice_, [], data_profile=data_profile,
    )

    assert "<column_value_profile>" in prompt
    assert "</column_value_profile>" in prompt
    # Profile content survives inside the wrapper.
    assert "NA" in prompt


# ── Task 9.3: empty-slice assertion ──


def test_render_preflight_prompt_raises_on_empty_slice():
    """Rendering with an empty slice is a programming error — the
    planner should never produce a slice with no assets. We want a
    loud failure now rather than a confusing LLM output later.
    """
    import pytest

    from genie_space_optimizer.optimization.preflight_synthesis import (
        AssetSlice,
        ARCHETYPES,
        render_preflight_prompt,
    )

    empty_slice = AssetSlice(tables=[])
    with pytest.raises(ValueError, match="empty slice"):
        render_preflight_prompt(ARCHETYPES[0], empty_slice, [])


# ── Task 10: retry feedback wrapped in <retry_feedback> ──


def test_render_preflight_prompt_wraps_retry_feedback_in_xml():
    """When ``retry_feedback`` is supplied, the rendered prompt must
    wrap it in ``<retry_feedback>...</retry_feedback>`` rather than the
    legacy ``## Retry feedback`` markdown heading.
    """
    from genie_space_optimizer.optimization.preflight_synthesis import (
        AssetSlice,
        ARCHETYPES,
        render_preflight_prompt,
    )

    slice_ = AssetSlice(
        tables=[
            {
                "identifier": "cat.sch.stores",
                "column_configs": [
                    {"identifier": "region", "data_type": "STRING"},
                ],
            }
        ]
    )
    prompt = render_preflight_prompt(
        ARCHETYPES[0],
        slice_,
        [],
        retry_feedback="Your last SQL returned 0 rows.",
    )

    assert "<retry_feedback>" in prompt
    assert "</retry_feedback>" in prompt
    assert "## Retry feedback" not in prompt
    assert "Your last SQL returned 0 rows." in prompt
