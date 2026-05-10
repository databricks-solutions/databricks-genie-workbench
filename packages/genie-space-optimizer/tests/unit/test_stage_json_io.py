# tests/unit/test_stage_json_io.py
from dataclasses import dataclass, field

from genie_space_optimizer.optimization.stages._json_io import (
    JsonRoundTrip,
    pretty_block,
)


@dataclass(frozen=True, slots=True)
class _Sample(JsonRoundTrip):
    name: str
    qids: tuple[str, ...] = ()
    counts: dict[str, int] = field(default_factory=dict)


def test_round_trip_through_json() -> None:
    s = _Sample(name="x", qids=("a", "b"), counts={"k": 1})
    payload = s.to_json()
    restored = _Sample.from_json(payload)
    assert restored == s


def test_to_pretty_renders_known_fields() -> None:
    s = _Sample(name="x", qids=("a", "b"), counts={"k": 1})
    text = s.to_pretty()
    assert "name" in text
    assert "qids" in text
    assert "counts" in text


def test_pretty_block_renders_label_and_body() -> None:
    body = pretty_block("Input", "name : x\nqids : (a, b)")
    assert body.startswith("─ Input")
    assert "name : x" in body


import json

from hypothesis import given
from hypothesis import strategies as st


@given(
    name=st.text(min_size=1, max_size=8),
    qids=st.lists(st.text(min_size=1, max_size=4), max_size=10).map(tuple),
    counts=st.dictionaries(st.text(min_size=1, max_size=4), st.integers(), max_size=5),
)
def test_round_trip_through_actual_json_text(
    name: str, qids: tuple[str, ...], counts: dict[str, int]
) -> None:
    s = _Sample(name=name, qids=qids, counts=counts)
    payload = json.loads(json.dumps(s.to_json()))
    assert _Sample.from_json(payload) == s


# ---------------------------------------------------------------------------
# I1 — capital-T Tuple / FrozenSet string type hints round-trip correctly
# ---------------------------------------------------------------------------

from genie_space_optimizer.optimization.stages._json_io import _from_json_value


def test_from_json_handles_capital_T_tuple_hint() -> None:
    """PEP-563 string hints in capital-T form must round-trip via _from_json_value."""
    # Simulate a field annotated as `Tuple[str, ...]` under
    # `from __future__ import annotations` — the hint arrives as a string.
    result = _from_json_value(["a", "b", "c"], "Tuple[str, ...]")
    assert result == ("a", "b", "c")
    assert isinstance(result, tuple)


def test_from_json_handles_capital_FrozenSet_hint() -> None:
    """Capital-F FrozenSet string hints must also round-trip."""
    result = _from_json_value(["x", "y"], "FrozenSet[str]")
    assert result == frozenset({"x", "y"})
    assert isinstance(result, frozenset)


def test_from_json_handles_lowercase_tuple_hint() -> None:
    """Lower-case `tuple[...]` (PEP-585) string hints must still work."""
    result = _from_json_value([1, 2], "tuple[int, ...]")
    assert result == (1, 2)
    assert isinstance(result, tuple)
