"""Trial 13h — pin the Stage 1 ``plan11_diagnose`` SKILL.md tightening.

The post-13g workbench replay surfaced a Stage 1 QID where the LLM emitted
``confidence: "high"`` with ``blame_set: []``, which the non-actionable gate
correctly but unnecessarily terminated. Trial 13h tightens the SKILL prompt
with a ``<blame_set_requirements>`` block, replaces the placeholder example
with a populated one, and adds an explicit INVALID example. These tests pin
each anchor so future prompt edits cannot silently regress the tightening.

See the trial-13h plan for context.
"""
from __future__ import annotations

from pathlib import Path

import pytest


_SKILL_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
    / "skills"
    / "plan11_diagnose"
    / "SKILL.md"
)


@pytest.fixture(scope="module")
def skill_text() -> str:
    return _SKILL_PATH.read_text(encoding="utf-8")


def test_skill_contains_blame_set_requirements_block(skill_text: str) -> None:
    assert "<blame_set_requirements>" in skill_text
    assert "</blame_set_requirements>" in skill_text


def test_skill_requires_non_empty_blame_set(skill_text: str) -> None:
    # The rules block must explicitly state the non-empty requirement so
    # the LLM cannot interpret "high confidence + empty blame_set" as valid.
    assert "MUST be non-empty for every diagnosed QID" in skill_text


def test_skill_documents_grounding_priority_with_seed_fallback(
    skill_text: str,
) -> None:
    # Grounding priority must explicitly route to blame_set_seed as a
    # fallback (the load-bearing fact behind Trial 13h's seed backfill).
    assert "judge_rationale" in skill_text
    assert "blame_set_seed" in skill_text
    # The fallback is the seed, and the SKILL must say so.
    assert "fallback" in skill_text.lower()


def test_skill_documents_insufficient_blame_set_decline(skill_text: str) -> None:
    # When nothing valid can be grounded, the SKILL must instruct the LLM
    # to emit the typed decline rather than a populated diagnosis with
    # blame_set: []. Pins the contract surface so postmortems get a real
    # decline marker instead of a silent non-actionable drop.
    assert 'reason: "insufficient_blame_set"' in skill_text


def test_output_envelope_uses_populated_blame_set_example(
    skill_text: str,
) -> None:
    # The placeholder ["<catalog.schema.table.column>", ...] left readers
    # interpreting it as "any string is fine". Replace with a concrete
    # 4-part FQN example so the LLM sees the expected shape.
    assert "main.airline.fact_tickets.payment_amt" in skill_text
    # The placeholder must be gone.
    assert '"<catalog.schema.table.column>"' not in skill_text


def test_skill_contains_invalid_blame_set_example(skill_text: str) -> None:
    # The INVALID example must call out the exact failure mode the post-13g
    # workbench replay surfaced: confidence: "high" paired with
    # blame_set: [].
    assert "INVALID" in skill_text
    # Look for the specific anti-pattern by approximate text match.
    assert '"blame_set": []' in skill_text
    assert '"confidence": "high"' in skill_text


def test_instructions_warn_about_silent_drop(skill_text: str) -> None:
    # Instruction #3 must make the consequence of an empty post-validation
    # blame_set explicit, so the LLM cannot rationalize emitting an empty
    # field "just to fill the schema".
    assert "silently dropped" in skill_text
    assert "non-actionable" in skill_text or "non_actionable" in skill_text
