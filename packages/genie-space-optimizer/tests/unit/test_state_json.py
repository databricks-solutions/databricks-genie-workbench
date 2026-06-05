"""Phase 2 Hotfix — unit tests for ``state_json.GsoJsonEncoder``.

The encoder is the canonical JSON-serialization boundary for every
``json.dumps`` site in ``optimization/state.py``. It must handle:

* ``frozenset`` and ``set`` — convert to canonically sorted list
* ``TerminalSignature`` — convert via its canonical ``to_jsonable`` helper
* ``NamedTuple`` — convert via ``_asdict()``
* Plain ``dict`` / ``list`` / scalars — pass through unchanged (byte-stable)
* Nested combinations of the above
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.state_json import (
    GsoJsonEncoder,
    dumps_state_json,
)
from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
    to_jsonable,
)


def test_frozenset_serializes_as_sorted_list():
    result = dumps_state_json({"levers": frozenset({5, 3, 7})})
    assert json.loads(result) == {"levers": [3, 5, 7]}


def test_set_serializes_as_sorted_list():
    result = dumps_state_json({"qids": {"gs_002", "gs_001"}})
    assert json.loads(result) == {"qids": ["gs_001", "gs_002"]}


def test_terminal_signature_serializes_via_canonical_shape():
    sig = build_terminal_signature(
        root_cause="propagation_lag",
        blame_set=["cat.s.tbl.col"],
        lever_set=[5, 6],
        target_qids=["gs_009"],
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    payload = dumps_state_json({"terminal_signature": sig})
    decoded = json.loads(payload)
    assert decoded == {"terminal_signature": to_jsonable(sig)}


def test_nested_reflection_buffer_entry_with_terminal_signature():
    sig = build_terminal_signature(
        root_cause="propagation_lag",
        blame_set=(),
        lever_set=[5],
        target_qids=["gs_009"],
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    entry = {
        "iteration": 1,
        "ag_id": "AG1",
        "accepted": False,
        "terminal_signature": sig,
        "levers": frozenset({5}),
    }
    payload = dumps_state_json({"reflection_buffer": [entry]})
    decoded = json.loads(payload)
    assert decoded["reflection_buffer"][0]["terminal_signature"]["lever_set"] == [5]
    assert decoded["reflection_buffer"][0]["levers"] == [5]


def test_plain_dict_passthrough_byte_stable():
    payload = {"a": 1, "b": "two", "c": [1, 2, 3], "d": None, "e": True}
    assert dumps_state_json(payload) == json.dumps(payload)


def test_encoder_class_directly_usable():
    """Operators / future code that needs to pass ``cls=`` directly."""
    out = json.dumps({"x": frozenset({1, 2})}, cls=GsoJsonEncoder)
    assert json.loads(out) == {"x": [1, 2]}


def test_unknown_type_still_raises():
    """Defensive: types we don't know how to handle should still raise
    so we surface unhandled cases instead of silently swallowing them."""
    class _Opaque:
        pass

    with pytest.raises(TypeError):
        dumps_state_json({"obj": _Opaque()})


def test_unsortable_set_falls_back_to_repr():
    """A frozenset of mixed types is rare but must not crash the
    encoder; we fall back to ``key=repr`` ordering."""
    result = dumps_state_json({"mixed": frozenset([1, "a"])})
    decoded = json.loads(result)
    assert sorted(decoded["mixed"], key=repr) == decoded["mixed"]


def test_encoder_terminal_signature_matches_canonical_to_jsonable():
    """The encoder's TerminalSignature path MUST produce the same dict shape
    as ``terminal_signature.to_jsonable`` (spec Section 4.4). This
    pins the contract so any future change to ``to_jsonable`` (or
    the encoder) surfaces as a single failing test instead of
    silent drift between GSO's JSON surfaces.
    """
    sig = build_terminal_signature(
        root_cause="instruction_not_scoped_to_qid",
        blame_set=["cat.sch.tbl_a.col", "cat.sch.tbl_b.col"],
        lever_set=[5, 6, 7],
        target_qids=["gs_001", "gs_002", "gs_003"],
        terminal_reason=TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY,
    )
    canonical = to_jsonable(sig)
    encoded = json.loads(dumps_state_json(sig))
    assert encoded == canonical, (
        "GsoJsonEncoder's TerminalSignature output drifted from "
        "terminal_signature.to_jsonable. Both surfaces must produce "
        "the same Section 4.4 shape."
    )
