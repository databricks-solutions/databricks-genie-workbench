"""Regression tests for lever-4-join-discovery prompt content & schema.

These tests lock the SKILL.md <-> inline prompt parity and the wire
JSON schema. They are designed to PASS on the current code so they
function as guards against future drift.

Plan: docs/prompt_improvements/2026-05-17-lever-4-join-discovery-hardening.md
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from genie_space_optimizer.common.config import (
    LEVER_4_JOIN_DISCOVERY_PROMPT,
)

_SKILL_PATH = (
    Path(__file__).resolve().parents[3]
    / "src" / "genie_space_optimizer"
    / "skills" / "lever-4-join-discovery" / "SKILL.md"
)


def _extract_section(text: str, tag: str) -> str:
    """Return the inner content of an XML-style block <tag>...</tag>."""
    m = re.search(rf"<{tag}>(.*?)</{tag}>", text, re.DOTALL)
    return m.group(1).strip() if m else ""


def _normalize(s: str) -> str:
    lines = [ln.rstrip() for ln in s.splitlines()]
    out: list[str] = []
    for ln in lines:
        if ln == "" and out and out[-1] == "":
            continue
        out.append(ln)
    return "\n".join(out).strip()


def test_skill_md_exists_and_loads():
    assert _SKILL_PATH.exists(), f"SKILL.md missing at {_SKILL_PATH}"
    assert _SKILL_PATH.read_text(encoding="utf-8").strip(), "SKILL.md is empty"


@pytest.mark.parametrize(
    "tag", ["role", "examples", "instructions", "output_schema"],
)
def test_skill_md_and_inline_prompt_have_section(tag: str):
    """Both sources must contain each active section."""
    skill_text = _SKILL_PATH.read_text(encoding="utf-8")
    assert _extract_section(skill_text, tag), f"<{tag}> missing in SKILL.md"
    assert _extract_section(LEVER_4_JOIN_DISCOVERY_PROMPT, tag), (
        f"<{tag}> missing in inline LEVER_4_JOIN_DISCOVERY_PROMPT"
    )


@pytest.mark.parametrize(
    "tag", ["role", "examples", "instructions", "output_schema"],
)
def test_skill_md_and_inline_prompt_agree_on_section_content(tag: str):
    """SKILL.md is documentation-grade; lock its content to the inline string."""
    skill_text = _SKILL_PATH.read_text(encoding="utf-8")
    a = _normalize(_extract_section(skill_text, tag))
    b = _normalize(_extract_section(LEVER_4_JOIN_DISCOVERY_PROMPT, tag))
    assert a == b, (
        f"SKILL.md and LEVER_4_JOIN_DISCOVERY_PROMPT diverge on <{tag}>\n"
        f"--- SKILL.md ---\n{a[:1000]}\n--- INLINE ---\n{b[:1000]}"
    )


# ── Wire-schema lock ──────────────────────────────────────────────

LEVER_4_REQUIRED_JOIN_SPEC_FIELDS: tuple[str, ...] = (
    "left", "right", "sql", "instruction",
)


def test_lever4_joinspec_pydantic_fields_locked():
    """Lever4JoinSpec field names are part of the wire contract.

    Renaming any of these breaks downstream Genie API patch shape.
    """
    from genie_space_optimizer.optimization.prompt_io import Lever4JoinSpec

    fields = set(Lever4JoinSpec.model_fields.keys())
    for required in LEVER_4_REQUIRED_JOIN_SPEC_FIELDS:
        assert required in fields, (
            f"required field {required!r} missing from Lever4JoinSpec"
        )


def test_lever4_joindiscovery_output_pydantic_fields_locked():
    """Lever4JoinDiscoveryOutput must expose `join_specs` and `rationale`."""
    from genie_space_optimizer.optimization.prompt_io import (
        Lever4JoinDiscoveryOutput,
    )

    fields = set(Lever4JoinDiscoveryOutput.model_fields.keys())
    assert "join_specs" in fields
    assert "rationale" in fields


# ── Task 3: Pydantic contract tightening ──


def test_lever4_join_spec_coerces_list_instruction_to_string():
    """Coerce instruction list → newline-joined string."""
    from genie_space_optimizer.optimization.prompt_io import Lever4JoinSpec

    spec = Lever4JoinSpec.model_validate({
        "left": {"identifier": "cat.sch.a", "alias": "a"},
        "right": {"identifier": "cat.sch.b", "alias": "b"},
        "sql": [
            "`a`.`x` = `b`.`x`",
            "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
        ],
        "instruction": ["use this when joining a and b", "be sure to filter on x"],
    })
    assert isinstance(spec.instruction, str)
    assert "use this when joining" in spec.instruction
    assert "be sure to filter" in spec.instruction
    assert "\n" in spec.instruction


def test_lever4_join_spec_accepts_str_instruction_unchanged():
    from genie_space_optimizer.optimization.prompt_io import Lever4JoinSpec

    spec = Lever4JoinSpec.model_validate({
        "left": {"identifier": "cat.sch.a", "alias": "a"},
        "right": {"identifier": "cat.sch.b", "alias": "b"},
        "sql": [
            "`a`.`x` = `b`.`x`",
            "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_ONE--",
        ],
        "instruction": "use this when joining a and b",
    })
    assert spec.instruction == "use this when joining a and b"


def test_lever4_join_spec_rejects_invalid_relationship_type():
    """Reject MANY_TO_MANY (not a valid Genie sentinel)."""
    import pydantic
    from genie_space_optimizer.optimization.prompt_io import Lever4JoinSpec

    with pytest.raises(pydantic.ValidationError):
        Lever4JoinSpec.model_validate({
            "left": {"identifier": "cat.sch.a", "alias": "a"},
            "right": {"identifier": "cat.sch.b", "alias": "b"},
            "sql": [
                "`a`.`x` = `b`.`x`",
                "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_MANY--",
            ],
            "instruction": "test",
        })


def test_lever4_join_spec_requires_at_least_two_sql_elements():
    """sql must contain the equijoin predicate AND the cardinality sentinel."""
    import pydantic
    from genie_space_optimizer.optimization.prompt_io import Lever4JoinSpec

    with pytest.raises(pydantic.ValidationError):
        Lever4JoinSpec.model_validate({
            "left": {"identifier": "cat.sch.a", "alias": "a"},
            "right": {"identifier": "cat.sch.b", "alias": "b"},
            "sql": ["`a`.`x` = `b`.`x`"],  # missing sentinel
            "instruction": "test",
        })


# ── Task 4: typed-IO end-to-end smoke (Pydantic > _extract_json) ──


def test_l4_typed_io_coerces_instruction_list_via_pydantic():
    """A response with instruction=[...] must be coerced to a string
    by Pydantic *before* the patch shape reaches the callsite return.
    """
    from genie_space_optimizer.optimization.prompt_io import (
        Lever4JoinDiscoveryOutput,
    )
    raw = (
        '{"join_specs": [{'
        '"left": {"identifier": "cat.sch.a", "alias": "a"}, '
        '"right": {"identifier": "cat.sch.b", "alias": "b"}, '
        '"sql": ["`a`.`x` = `b`.`x`", '
        '"--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"], '
        '"instruction": ["line 1", "line 2"]'
        '}], "rationale": "test"}'
    )
    parsed = Lever4JoinDiscoveryOutput.model_validate_json(raw)
    dumped = parsed.model_dump()
    assert isinstance(dumped["join_specs"][0]["instruction"], str)
    assert "line 1" in dumped["join_specs"][0]["instruction"]
    assert "line 2" in dumped["join_specs"][0]["instruction"]


# ── Task 5: LEVER_4_MAX_TOKENS constant exists and sized per baseline ──


def test_lever_4_max_tokens_constant_exists_and_is_sized():
    from genie_space_optimizer.common import config

    assert hasattr(config, "LEVER_4_MAX_TOKENS")
    assert isinstance(config.LEVER_4_MAX_TOKENS, int)
    assert 1000 <= config.LEVER_4_MAX_TOKENS <= 2500, (
        f"LEVER_4_MAX_TOKENS={config.LEVER_4_MAX_TOKENS} outside "
        f"baseline band [1000, 2500]"
    )


# ── Task 6: trimmed system message ──


def test_l4_system_message_is_terse_v2():
    from genie_space_optimizer.optimization.optimizer import (
        _LEVER_4_SYSTEM_MSG,
        _LEVER_4_SYSTEM_MSG_VERSION,
    )
    assert len(_LEVER_4_SYSTEM_MSG) <= 150, (
        f"system message too long: {len(_LEVER_4_SYSTEM_MSG)} chars"
    )
    assert _LEVER_4_SYSTEM_MSG_VERSION == "v2"
    for phrase in ("JSON API", "json.loads", "ONLY a valid JSON object"):
        assert phrase not in _LEVER_4_SYSTEM_MSG, (
            f"system message still contains obsolete guard: '{phrase}'"
        )


# ── Task 7: examples expanded to 3 canonical cases ──


def test_examples_section_contains_three_canonical_cases():
    ex = _extract_section(LEVER_4_JOIN_DISCOVERY_PROMPT, "examples")
    n = ex.count("<example>")
    assert n >= 3, f"<examples> must contain >=3 canonical cases, found {n}"
    assert ex.count("</example>") == n


@pytest.mark.parametrize(
    "required_marker",
    [
        "fact_sales",
        "MEASURE()",
        "METRIC_VIEW_JOIN_NOT_SUPPORTED",
        "INVALID — incompatible types",
    ],
)
def test_examples_cover_diverse_patterns(required_marker: str):
    ex = _extract_section(LEVER_4_JOIN_DISCOVERY_PROMPT, "examples")
    assert required_marker in ex, (
        f"<examples> must include canonical marker: '{required_marker}'"
    )


# ── Task 8: dedicated <metric_view_rule> block ──


def test_metric_view_rule_is_dedicated_block():
    mv = _extract_section(LEVER_4_JOIN_DISCOVERY_PROMPT, "metric_view_rule")
    assert mv, "<metric_view_rule> must exist as a dedicated top-level block"
    assert "METRIC_VIEW_JOIN_NOT_SUPPORTED" in mv
    assert "MEASURE()" in mv
    assert "GROUP BY ALL" in mv
    assert "CTE" in mv


def test_metric_view_rule_is_NOT_inside_instructions_anymore():
    inst = _extract_section(LEVER_4_JOIN_DISCOVERY_PROMPT, "instructions")
    assert "## Metric View Join Rule" not in inst, (
        "Stale subsection inside <instructions>; Task 8 moved this out."
    )


# ── Task 9: <empty_evidence_directive> block ──


def test_empty_evidence_directive_is_dedicated_block():
    d = _extract_section(
        LEVER_4_JOIN_DISCOVERY_PROMPT, "empty_evidence_directive",
    )
    assert d, "<empty_evidence_directive> block missing"
    assert "do NOT invent failure narratives" in d.lower() or (
        "do not invent failure narratives" in d.lower()
    )
    assert "rely on" in d.lower()
