"""B2 Task 1 — _record_to_mapping helper unit tests."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest


def test_record_to_mapping_dict_passes_through() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    src = {"decision_type": "patch_applied", "ag_id": "AG1"}
    out = _record_to_mapping(src)
    assert out == {"decision_type": "patch_applied", "ag_id": "AG1"}
    assert isinstance(out, dict)


def test_record_to_mapping_decisionrecord_dataclass_uses_to_dict() -> None:
    """Real DecisionRecord instances must flow through .to_dict()."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )
    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    record = DecisionRecord(
        run_id="r-123",
        iteration=1,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode.NONE,
        ag_id="AG1",
    )
    out = _record_to_mapping(record)
    assert isinstance(out, dict)
    # to_dict() emits .value strings, not enum instances
    assert out["decision_type"] == "patch_applied"
    assert out["ag_id"] == "AG1"
    # the dict supports .get() — the contract the invariants require
    assert out.get("decision_type") == "patch_applied"


def test_record_to_mapping_returns_none_for_unrecognized_types() -> None:
    """A string, an int, a None — none of these can be coerced to a record."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    assert _record_to_mapping(None) is None
    assert _record_to_mapping("not a record") is None
    assert _record_to_mapping(42) is None
    assert _record_to_mapping(["list", "is", "not", "a", "record"]) is None


def test_record_to_mapping_arbitrary_dataclass_with_to_dict() -> None:
    """Defense in depth: a non-DecisionRecord dataclass that has to_dict()
    flows through too — guarantees the helper is duck-typed, not coupled
    to DecisionRecord."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    @dataclass(frozen=True)
    class _Custom:
        a: str = "x"
        b: int = 1

        def to_dict(self) -> dict:
            return {"a": self.a, "b": self.b}

    out = _record_to_mapping(_Custom())
    assert out == {"a": "x", "b": 1}


def test_record_to_mapping_dataclass_without_to_dict_returns_none() -> None:
    """If a dataclass slipped through without to_dict, drop it cleanly."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    @dataclass(frozen=True)
    class _BareDataclass:
        a: str = "x"

    assert _record_to_mapping(_BareDataclass()) is None


def test_record_to_mapping_handles_to_dict_raising() -> None:
    """If to_dict raises (a buggy dataclass), drop cleanly rather than
    propagate the exception to the projection."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    class _RaisingRecord:
        def to_dict(self) -> dict:
            raise RuntimeError("synthesized failure")

    assert _record_to_mapping(_RaisingRecord()) is None


def test_record_to_mapping_handles_to_dict_returning_non_dict() -> None:
    """to_dict() that returns a string or list must be dropped, not coerced."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    class _BadShape:
        def to_dict(self):
            return ["this", "is", "not", "a", "dict"]

    assert _record_to_mapping(_BadShape()) is None


def test_record_to_mapping_mapping_subclass_is_coerced() -> None:
    """A non-dict Mapping (e.g. types.MappingProxyType) is coerced to dict."""
    import types

    from genie_space_optimizer.optimization.invariant_projection import (
        _record_to_mapping,
    )

    src = types.MappingProxyType({"decision_type": "patch_applied"})
    out = _record_to_mapping(src)
    assert isinstance(out, dict)
    assert out["decision_type"] == "patch_applied"
