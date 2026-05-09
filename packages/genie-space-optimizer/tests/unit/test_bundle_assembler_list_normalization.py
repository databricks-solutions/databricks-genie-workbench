"""Cycle 14-V Task 5 — bundle assembler list normalization.

Anchor: airline run 833709971504406 F7 — assembler raised
``AttributeError: 'list' object has no attribute 'get'`` on a
list-valued stage capture. The new ``_normalize_stage_capture``
helper coerces list-of-dict / list-of-non-dict / non-dict /
``None`` to a dict so downstream ``.get()`` access is safe.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.run_output_bundle import (
    _normalize_stage_capture,
)


def test_dict_input_returned_unchanged() -> None:
    payload = {"decisions": ["a", "b"], "summary": {"k": 1}}
    assert _normalize_stage_capture(payload) is payload


def test_list_of_dict_returns_first_dict() -> None:
    a = {"decisions": ["x"]}
    b = {"decisions": ["y"]}
    out = _normalize_stage_capture([a, b])
    assert out is a


def test_list_of_non_dict_returns_empty_dict() -> None:
    assert _normalize_stage_capture([1, 2, "three"]) == {}


def test_empty_list_returns_empty_dict() -> None:
    assert _normalize_stage_capture([]) == {}


def test_none_returns_empty_dict() -> None:
    assert _normalize_stage_capture(None) == {}


def test_non_dict_scalar_returns_empty_dict() -> None:
    assert _normalize_stage_capture("oops") == {}
    assert _normalize_stage_capture(42) == {}


def test_normalized_value_supports_get_access() -> None:
    """The whole point of the helper: downstream code can call
    ``.get(...)`` without raising AttributeError."""
    norm = _normalize_stage_capture(["not-a-dict"])
    # No AttributeError.
    assert norm.get("decisions") is None
    assert norm.get("missing", "default") == "default"
