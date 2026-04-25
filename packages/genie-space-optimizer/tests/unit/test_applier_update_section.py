"""Regression tests for the applier ``update_section`` list-vs-string fix.

The Lever Loop crashed with::

    AttributeError: 'list' object has no attribute 'strip'

inside ``_apply_action_to_config`` (op=update_section). Root cause:
``_ensure_structured`` returns ``dict[str, list[str]]`` (one entry per
non-blank line) but the surrounding render loop was calling ``.strip()``
directly on each value. Whenever the user-authored config had any of
``INSTRUCTION_SECTION_ORDER`` populated, those values were lists and the
loop crashed.

The tests below seed a config with multi-section instructions, drive the
applier through the same code path that crashed in production, and assert
the renderer produces a string with all original sections preserved.
"""

from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization import applier


def _fresh_config(seed_text: str) -> dict:
    """Build a minimal Genie-space-like config dict with seeded instructions.

    Mirrors the helper used in ``test_applier_quality_instructions.py`` so
    we exercise the same plumbing the production lever loop does.
    """
    cfg: dict = {"instructions": {"text_instructions": []}}
    if seed_text:
        applier._set_general_instructions(cfg, seed_text)
    return cfg


_SEED_INSTRUCTIONS = (
    "PURPOSE:\n"
    "- This space supports retail sales analysis across markets.\n"
    "- Audience: sales analysts and area leaders.\n"
    "\n"
    "ASSET ROUTING:\n"
    "- Use mv_esr_store_sales for enterprise APSD reporting.\n"
    "- Use mv_7now_store_sales for 7NOW delivery KPIs.\n"
    "\n"
    "CONSTRAINTS:\n"
    "- Never mix CY and PY columns in the same aggregate.\n"
    "- Always filter same_store_7now = 'Y' for same-store comparisons.\n"
)


def _build_update_section_action(section_name: str, new_text: str) -> dict:
    """Render the same action shape ``render_patch`` emits for an
    ``update_instruction_section`` patch."""
    return {
        "command": json.dumps({
            "op": "update_section",
            "section": "instructions",
            "section_name": section_name,
            "new_text": new_text,
            "lever": 5,
        }),
    }


def test_update_section_does_not_crash_on_list_valued_structured_dict() -> None:
    """The exact production failure mode.

    A config whose instructions parse into ``dict[str, list[str]]`` (i.e.
    every multi-section instruction block we ship) must not crash the
    update_section render loop.
    """
    cfg = _fresh_config(_SEED_INSTRUCTIONS)
    action = _build_update_section_action(
        "QUERY RULES",
        "- Always project location_number alongside any APSD measure.",
    )

    ok = applier._apply_action_to_config(cfg, action)

    assert ok is True
    rendered = applier._get_general_instructions(cfg)
    assert "QUERY RULES" in rendered
    assert "location_number alongside any APSD" in rendered


def test_update_section_preserves_existing_sections_verbatim() -> None:
    """Updating one section must not drop or corrupt the other sections.

    Pre-fix, even when the renderer didn't crash, list-typed values were
    silently coerced to ``str(list)`` (which would have rendered ugly
    bracketed Python repr). Now the helper joins list lines with ``\n``,
    so the original lines must appear verbatim in the rendered output.
    """
    cfg = _fresh_config(_SEED_INSTRUCTIONS)
    action = _build_update_section_action(
        "QUERY RULES",
        "- Always project location_number alongside any APSD measure.",
    )

    applier._apply_action_to_config(cfg, action)
    rendered = applier._get_general_instructions(cfg)

    # PURPSE / ASSET ROUTING / CONSTRAINTS lines must survive intact.
    assert "supports retail sales analysis" in rendered
    assert "Use mv_esr_store_sales for enterprise APSD" in rendered
    assert "Use mv_7now_store_sales for 7NOW delivery KPIs" in rendered
    assert "Never mix CY and PY columns" in rendered
    assert "Always filter same_store_7now = 'Y'" in rendered
    # The newly-added bullet lands under QUERY RULES.
    assert "QUERY RULES" in rendered
    assert "location_number alongside any APSD measure" in rendered
    # No Python repr leaked through (the list-to-str fix).
    assert "['" not in rendered
    assert "', '" not in rendered


def test_update_section_merges_into_existing_section() -> None:
    """When the target section already has content, new_text is appended,
    not used to overwrite the section."""
    cfg = _fresh_config(_SEED_INSTRUCTIONS)
    action = _build_update_section_action(
        "CONSTRAINTS",
        "- Round all currency outputs to two decimal places.",
    )

    applier._apply_action_to_config(cfg, action)
    rendered = applier._get_general_instructions(cfg)

    assert "Never mix CY and PY columns" in rendered
    assert "Round all currency outputs to two decimal places" in rendered


def test_update_section_creates_section_when_absent() -> None:
    """When the target section doesn't exist yet, the renderer adds it."""
    cfg = _fresh_config(_SEED_INSTRUCTIONS)
    action = _build_update_section_action(
        "TEMPORAL FILTERS",
        "- Default time_window column to 'MTD' unless the user asks for daily.",
    )

    ok = applier._apply_action_to_config(cfg, action)

    assert ok is True
    rendered = applier._get_general_instructions(cfg)
    assert "TEMPORAL FILTERS" in rendered
    assert "Default time_window column to 'MTD'" in rendered


def test_update_section_with_empty_new_text_is_noop() -> None:
    """Empty new_text returns False and leaves the config untouched."""
    cfg = _fresh_config(_SEED_INSTRUCTIONS)
    before = applier._get_general_instructions(cfg)

    action = _build_update_section_action("CONSTRAINTS", "")
    ok = applier._apply_action_to_config(cfg, action)

    assert ok is False
    after = applier._get_general_instructions(cfg)
    assert before == after


def test_rewrite_instruction_split_children_apply_without_crashing() -> None:
    """End-to-end of the AG1 P009 path that crashed in production.

    ``_split_rewrite_instruction_patch`` expands a ``rewrite_instruction``
    into per-section ``update_instruction_section`` children. Applying each
    child must succeed even when prior children have already mutated the
    structured instruction sections — the regression here is that earlier
    sections still held ``list[str]`` values while later iterations tried
    to ``.strip()`` them.
    """
    cfg = _fresh_config(_SEED_INSTRUCTIONS)

    rewritten_body = (
        "PURPOSE:\n"
        "- This space supports retail sales analysis across markets.\n"
        "- Audience: sales analysts, area leaders, and market managers.\n"
        "\n"
        "ASSET ROUTING:\n"
        "- Use mv_esr_store_sales for enterprise APSD reporting.\n"
        "- Use mv_7now_store_sales for 7NOW delivery KPIs.\n"
        "- Use mv_esr_fact_sales for daily granular fact rollups.\n"
        "\n"
        "QUERY RULES:\n"
        "- The time_window column drives DAY vs MTD aggregation.\n"
        "\n"
        "CONSTRAINTS:\n"
        "- Never mix CY and PY columns in the same aggregate.\n"
    )

    rewrite_patch = {
        "type": "rewrite_instruction",
        "lever": 5,
        "invoked_levers": [1, 5],
        "proposed_value": rewritten_body,
        "old_value": _SEED_INSTRUCTIONS,
        "cluster_id": "C_test",
        "rationale": "test",
    }

    children = applier._split_rewrite_instruction_patch(rewrite_patch)
    assert children is not None and len(children) >= 1

    for child in children:
        action = _build_update_section_action(
            child["section_name"], child["new_text"],
        )
        ok = applier._apply_action_to_config(cfg, action)
        assert ok is True, (
            f"applier crashed/skipped child for section "
            f"{child.get('section_name')!r}"
        )

    rendered = applier._get_general_instructions(cfg)
    # Newly-added content from at least one section landed.
    assert "DAY vs MTD" in rendered or "market managers" in rendered
    # Pre-existing content from non-targeted sections survived.
    assert "Never mix CY and PY columns" in rendered
    # No Python list repr leaked through.
    assert "['" not in rendered
    assert "', '" not in rendered
