"""Strict-mode toggle in build_response_format().

Plan: docs/prompt_improvements/2026-05-17-active-callsite-typed-output-wiring.md Task 1
"""
from __future__ import annotations

from typing import Any

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
)


class _StrictModel(LLMOutputContract):
    name: str
    count: int = 0


class _PermissiveBase(LLMOutputContract):
    model_config = {"extra": "allow", "str_strip_whitespace": True}
    tag: str = Field(default="")


class _StrictTopWithPermissiveChildren(LLMOutputContract):
    items: list[_PermissiveBase] = Field(default_factory=list)


def test_strict_model_emits_strict_envelope():
    rf = build_response_format(_StrictModel)
    assert rf["json_schema"]["strict"] is True
    schema = rf["json_schema"]["schema"]
    assert schema.get("additionalProperties") is False


def test_permissive_model_emits_non_strict_envelope():
    rf = build_response_format(_PermissiveBase)
    assert rf["json_schema"]["strict"] is False


def test_strict_top_with_permissive_children_emits_non_strict_envelope():
    """If ANY nested object permits additionalProperties, the envelope
    must drop strict mode."""
    rf = build_response_format(_StrictTopWithPermissiveChildren)
    assert rf["json_schema"]["strict"] is False


def _walk(node: Any):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def test_strict_envelope_pins_additionalProperties_False_at_every_object():
    rf = build_response_format(_StrictModel)
    assert rf["json_schema"]["strict"] is True
    object_nodes = [
        n for n in _walk(rf["json_schema"]["schema"])
        if n.get("type") == "object"
    ]
    for node in object_nodes:
        assert node.get("additionalProperties") is False, node
