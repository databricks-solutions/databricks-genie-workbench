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


# ── Task 7: retry_reason kwarg + span tagging ──


def test_synthesize_preflight_candidate_accepts_retry_reason_kwarg():
    """Task 7: ``synthesize_preflight_candidate`` must accept a
    ``retry_reason`` kwarg so the orchestrator can tag retries by
    their root cause (empty_result, qualification, measure_function,
    categorical_cast). The kwarg is informational only — no behavior
    change on the prompt or proposal."""
    import inspect

    from genie_space_optimizer.optimization import preflight_synthesis as ps

    sig = inspect.signature(ps.synthesize_preflight_candidate)
    assert "retry_reason" in sig.parameters, (
        "Task 7: synthesize_preflight_candidate must accept retry_reason"
    )


def test_synthesize_preflight_candidate_tags_span_with_retry_reason(monkeypatch):
    """Task 7: when retry_reason is non-None, the helper must tag the
    current span via _set_preflight_span_tag so MLflow / OTel traces
    can filter retries by class."""
    from genie_space_optimizer.optimization import preflight_synthesis as ps
    from genie_space_optimizer.optimization.preflight_synthesis import (
        ARCHETYPES,
        AssetSlice,
    )

    # The helper must exist (and ideally be callable).
    assert hasattr(ps, "_set_preflight_span_tag"), (
        "Task 7: define _set_preflight_span_tag helper so the retry tag "
        "wiring lives in one place."
    )

    tag_calls: list[tuple[str, str]] = []

    def _capture(key: str, value: str) -> None:
        tag_calls.append((key, value))

    monkeypatch.setattr(ps, "_set_preflight_span_tag", _capture)

    slice_ = AssetSlice(
        tables=[{"identifier": "cat.sch.t"}],
        columns=[("cat.sch.t", "region")],
    )
    # Use a stub llm_caller so we don't hit the network.
    ps.synthesize_preflight_candidate(
        ARCHETYPES[0],
        slice_,
        [],
        llm_caller=lambda _prompt: "",  # empty raw → returns None
        retry_reason="empty_result",
    )

    keys = {k for k, _ in tag_calls}
    assert "is_retry" in keys
    assert "retry_reason" in keys
    # And the values are what we passed.
    for k, v in tag_calls:
        if k == "retry_reason":
            assert v == "empty_result"
        if k == "is_retry":
            assert v in {"true", "True", "1"}


# ── Task 8: retry feedback dedup — don't embed allowlist twice ──


# ── Task 13: harness-side span tagging ──


def test_run_preflight_example_synthesis_accepts_preflight_path_kwarg():
    """Task 13: ``run_preflight_example_synthesis`` must accept a
    ``preflight_path`` kwarg so the harness can tag traces with which
    of the two harness entry points fired (the modern fallback vs the
    legacy path)."""
    import inspect

    from genie_space_optimizer.optimization import preflight_synthesis as ps

    sig = inspect.signature(ps.run_preflight_example_synthesis)
    assert "preflight_path" in sig.parameters, (
        "Task 13: run_preflight_example_synthesis must accept preflight_path"
    )


def test_run_preflight_example_synthesis_tags_span_with_path(monkeypatch):
    """Task 13: when called with ``preflight_path``, the entry tags the
    active span with both ``preflight_path`` and ``leak_safe_header_enabled``.
    The latter is recorded as a documented fact about the prompt: the
    contract header that prevents benchmark leakage is unconditionally
    enabled today."""
    from genie_space_optimizer.optimization import preflight_synthesis as ps

    tag_calls: list[tuple[str, str]] = []

    def _capture(key: str, value: str) -> None:
        tag_calls.append((key, value))

    monkeypatch.setattr(ps, "_set_preflight_span_tag", _capture)

    result = ps.run_preflight_example_synthesis(
        w=None,
        spark=None,
        run_id="r1",
        space_id="s1",
        config={"_parsed_space": {"instructions": {"example_question_sqls": []}}},
        metadata_snapshot={
            "instructions": {"example_question_sqls": []},
            "data_sources": {"tables": []},
        },
        benchmarks=[],
        catalog="cat",
        schema="sch",
        target=0,  # short-circuit before any LLM
        preflight_path="modern_fallback",
    )
    assert result["applied"] == 0  # short-circuited

    tagged = dict(tag_calls)
    assert tagged.get("preflight_path") == "modern_fallback"
    assert tagged.get("leak_safe_header_enabled") in {"true", "True", "1"}


def test_qualification_retry_feedback_does_not_embed_allowlist_when_rendered():
    """Task 8: when rendered into the prompt, the qualification retry
    feedback must NOT carry a full second copy of the allowlist — the
    <schema> section above already contains it. Instead the feedback
    references the existing allowlist."""
    from genie_space_optimizer.optimization.preflight_synthesis import (
        AssetSlice,
        ARCHETYPES,
        _render_preflight_retry_feedback,
        render_preflight_prompt,
    )

    # Build a qualification-feedback-shaped block; the helper should
    # strip the duplicate allowlist before it's wrapped.
    slice_ = AssetSlice(
        tables=[{"identifier": "cat.sch.distinctive_table_name_42"}],
        columns=[("cat.sch.distinctive_table_name_42", "region")],
    )
    raw_feedback = (
        "Your previous query failed validation:\n"
        "  UNRESOLVED_TABLE\n\n"
        "Your SQL was:\n"
        "  SELECT * FROM dim_date\n\n"
        "The ONLY legal table identifiers for this example are:\n"
        "- `cat`.`sch`.`distinctive_table_name_42` "
        "(columns: `region`)\n\n"
        "Regenerate the example_sql ..."
    )

    dedup = _render_preflight_retry_feedback(raw_feedback)
    # The redundant allowlist line is gone.
    assert "distinctive_table_name_42" not in dedup
    # Reference text replaces it.
    assert "<schema>" in dedup

    prompt = render_preflight_prompt(
        ARCHETYPES[0],
        slice_,
        [],
        retry_feedback=dedup,
    )
    # In the final prompt, the table identifier appears (in <schema>)
    # but the dedup'd retry block does NOT carry its own copy.
    retry_start = prompt.index("<retry_feedback>")
    retry_end = prompt.index("</retry_feedback>")
    retry_section = prompt[retry_start:retry_end]
    assert "distinctive_table_name_42" not in retry_section


def test_synthesize_preflight_candidate_no_retry_tags_on_first_call(monkeypatch):
    """When ``retry_reason`` is None (first attempt), the helper must
    NOT tag the span as a retry. Tags only fire on actual retries."""
    from genie_space_optimizer.optimization import preflight_synthesis as ps
    from genie_space_optimizer.optimization.preflight_synthesis import (
        ARCHETYPES,
        AssetSlice,
    )

    tag_calls: list[tuple[str, str]] = []

    def _capture(key: str, value: str) -> None:
        tag_calls.append((key, value))

    monkeypatch.setattr(ps, "_set_preflight_span_tag", _capture)

    slice_ = AssetSlice(
        tables=[{"identifier": "cat.sch.t"}],
        columns=[("cat.sch.t", "region")],
    )
    ps.synthesize_preflight_candidate(
        ARCHETYPES[0],
        slice_,
        [],
        llm_caller=lambda _prompt: "",
        # no retry_reason kwarg
    )

    keys = {k for k, _ in tag_calls}
    assert "retry_reason" not in keys, (
        "first attempt must not tag retry_reason — only retries do"
    )
