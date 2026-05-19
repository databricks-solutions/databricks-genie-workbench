"""Plan 2 Task 12 — reasoning-skill conformance test.

Every skill with ``llm_call_kind: reasoning`` frontmatter MUST pass
this suite. Plans 3-7 each add a new reasoning skill and inherit
these checks automatically — the test discovers skills by walking
the ``skills/`` directory and filtering on frontmatter.

Conformance rules:
  R1. ``output_schema_class`` resolves to a subclass of
      ``prompt_io.LLMOutputContract``.
  R2. ``max_tokens`` is set and is a positive int.
  R3. ``abstain_supported`` is True (Plan 2's framework requires
      every reasoning skill to support abstain).
  R4. ``examples_dir`` exists and has ≤4 .json files.
  R5. ``eval_dir`` exists and has at least one .jsonl test case.
  R6. The skill body contains the canonical ``<output_envelope>``
      block (gate against prompt edits that drop the envelope
      instructions).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.skills._loader import _SKILL_LOADER


def _discover_reasoning_skill_ids() -> list[str]:
    root = Path(_SKILL_LOADER._root)
    out: list[str] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        skill_md = child / "SKILL.md"
        if not skill_md.is_file():
            continue
        try:
            meta = _SKILL_LOADER.load_metadata(child.name)
        except Exception:
            continue
        if meta.get("llm_call_kind") == "reasoning":
            out.append(child.name)
    return out


_REASONING_SKILL_IDS = _discover_reasoning_skill_ids()


@pytest.mark.parametrize("skill_id", _REASONING_SKILL_IDS)
def test_R1_output_schema_class_subclasses_llm_output_contract(
    skill_id: str,
) -> None:
    cls = _SKILL_LOADER.load_output_schema_class(skill_id)
    assert issubclass(cls, LLMOutputContract), (
        f"skill {skill_id!r} output_schema_class {cls.__name__} must "
        f"subclass LLMOutputContract"
    )


@pytest.mark.parametrize("skill_id", _REASONING_SKILL_IDS)
def test_R2_max_tokens_is_positive_int(skill_id: str) -> None:
    rsm = _SKILL_LOADER.load_reasoning_metadata(skill_id)
    assert rsm is not None
    assert isinstance(rsm.max_tokens, int)
    assert rsm.max_tokens > 0


@pytest.mark.parametrize("skill_id", _REASONING_SKILL_IDS)
def test_R3_abstain_supported_is_true(skill_id: str) -> None:
    """The framework requires every reasoning skill to support
    abstain. A skill that cannot decline is brittle."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(skill_id)
    assert rsm is not None
    assert rsm.abstain_supported is True


@pytest.mark.parametrize("skill_id", _REASONING_SKILL_IDS)
def test_R4_examples_dir_has_at_most_four_json_files(skill_id: str) -> None:
    examples = list(_SKILL_LOADER.iter_examples(skill_id))
    assert 0 <= len(examples) <= 4, (
        f"skill {skill_id!r} has {len(examples)} examples; framework "
        f"enforces ≤4 (Anthropic context engineering)"
    )


@pytest.mark.parametrize("skill_id", _REASONING_SKILL_IDS)
def test_R5_eval_dir_has_at_least_one_test_case(skill_id: str) -> None:
    rsm = _SKILL_LOADER.load_reasoning_metadata(skill_id)
    assert rsm is not None
    eval_dir = rsm.eval_dir or "./eval"
    root = Path(_SKILL_LOADER._root) / skill_id
    eval_path = (root / eval_dir).resolve()
    jsonl_files = sorted(eval_path.glob("*.jsonl"))
    assert jsonl_files, (
        f"skill {skill_id!r}: no .jsonl files under {eval_dir}; every "
        f"reasoning skill must ship at least one held-out test case "
        f"so prompt changes can be regression-gated"
    )
    total_cases = 0
    for f in jsonl_files:
        for line in f.read_text(encoding="utf-8").splitlines():
            if line.strip():
                total_cases += 1
    assert total_cases >= 1


@pytest.mark.parametrize("skill_id", _REASONING_SKILL_IDS)
def test_R6_skill_body_contains_output_envelope_block(skill_id: str) -> None:
    meta = _SKILL_LOADER.load_metadata(skill_id)
    body = _SKILL_LOADER.load_prompt(
        skill_id, expected_constant_name=meta["prompt_constant_name"],
    )
    assert "<output_envelope>" in body, (
        f"skill {skill_id!r}: SKILL.md body must include an "
        f"<output_envelope> block instructing the LLM to use the "
        f"AbstainableEnvelope shape"
    )
