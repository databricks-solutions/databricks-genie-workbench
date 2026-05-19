"""Plan 7 Task 12 — Plan 5's SKILL.md documents the new optional
``last_attempt_hypothesis`` <context_inputs> field.
"""
from __future__ import annotations

from genie_space_optimizer.skills._loader import _SKILL_LOADER


def test_repair_intent_synthesis_skill_md_documents_last_attempt_hypothesis() -> None:
    body = _SKILL_LOADER.load_prompt(
        "repair_intent_synthesis",
        expected_constant_name="REPAIR_INTENT_SYNTHESIS_PROMPT",
    )
    assert "last_attempt_hypothesis" in body, (
        "Plan 5 SKILL.md must document the optional "
        "last_attempt_hypothesis <context_inputs> field added by Plan 7"
    )
    assert "null when" in body.lower() or "optional" in body.lower()


def test_repair_intent_synthesis_skill_md_explains_hypothesis_grounding_use() -> None:
    body = _SKILL_LOADER.load_prompt(
        "repair_intent_synthesis",
        expected_constant_name="REPAIR_INTENT_SYNTHESIS_PROMPT",
    )
    assert (
        "previous iteration" in body.lower()
        or "prior iteration" in body.lower()
    )
