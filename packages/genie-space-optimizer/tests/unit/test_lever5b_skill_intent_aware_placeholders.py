"""Plan 5 Task 9 — L5b SKILL.md uses intent-aware placeholders.

The roadmap (line 345-351) calls for swapping the archetype block for
the repair-intent block. We rename FOUR placeholders unconditionally
(no flag gating the template) — the renderer (Task 10) feeds intent
fields from either intent_from_archetype (deterministic) OR the Plan-5
LLM. Rollback = flip the flag; renderer fills the same template with
the deterministic intent.
"""
from __future__ import annotations

from genie_space_optimizer.skills._loader import _SKILL_LOADER


SKILL_ID = "lever-5b-example-sql"


def test_lever5b_skill_body_uses_intent_placeholders() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="LEVER_5B_EXAMPLE_SQL_PROMPT",
    )
    for placeholder in (
        "{{ repair_intent_name }}",
        "{{ repair_intent_description }}",
        "{{ repair_shape }}",
        "{{ repair_rationale }}",
    ):
        assert placeholder in body, (
            f"L5b SKILL.md must carry intent-aware placeholder "
            f"{placeholder!r} after Plan 5 Task 9"
        )


def test_lever5b_skill_body_drops_old_archetype_placeholders() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="LEVER_5B_EXAMPLE_SQL_PROMPT",
    )
    for placeholder in (
        "{{ archetype_name }}",
        "{{ archetype_output_shape }}",
        "{{ archetype_prompt_template }}",
    ):
        assert placeholder not in body, (
            f"L5b SKILL.md must drop old archetype placeholder "
            f"{placeholder!r} after Plan 5 Task 9 (renderer now feeds "
            f"intent fields uniformly from either deterministic or LLM "
            f"producer)"
        )


def test_lever5b_skill_body_preserves_afs_and_allowlist_placeholders() -> None:
    """The non-archetype placeholders (AFS block + identifier allowlist)
    stay byte-stable. Renderer continues to fill these from the
    deterministic AFS / identifier-allowlist derivation."""
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="LEVER_5B_EXAMPLE_SQL_PROMPT",
    )
    for placeholder in (
        "{{ afs_block }}",
        "{{ identifier_allowlist }}",
    ):
        assert placeholder in body, (
            f"L5b SKILL.md must preserve {placeholder!r} after Plan 5 Task 9"
        )


def test_lever5b_skill_output_contract_section_byte_stable() -> None:
    """The L5b output contract (example_question / example_sql /
    usage_guidance / rationale) is unchanged — Plan 5 only changes WHAT
    drives the prompt, not what the prompt asks the LLM to produce."""
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="LEVER_5B_EXAMPLE_SQL_PROMPT",
    )
    for required in (
        '"example_question"',
        '"example_sql"',
        '"usage_guidance"',
        '"rationale"',
    ):
        assert required in body
