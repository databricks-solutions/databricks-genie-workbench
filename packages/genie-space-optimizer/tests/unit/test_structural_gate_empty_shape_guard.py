"""WU-5 — harness-level guard around structural_repair_gate."""
from __future__ import annotations

import os
from unittest.mock import patch


# ── Task 12: flag ────────────────────────────────────────────────────


def test_flag_default_on() -> None:
    """WU-5 ships default-ON for the production rollout. Pairs with
    WU-3 to catch the both-empty card-metadata signature when the
    preflight is bypassed or a card-builder regression strips
    metadata."""
    from genie_space_optimizer.common.config import (
        structural_gate_guard_empty_shape_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE", None)
        assert structural_gate_guard_empty_shape_enabled() is True


def test_flag_off_when_explicit_zero() -> None:
    """Rollback path — operators can restore the gate's pre-WU-5
    fails-open contract by setting
    GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE=0 in app.yaml."""
    from genie_space_optimizer.common.config import (
        structural_gate_guard_empty_shape_enabled,
    )
    for val in ("0", "false", "no", "off"):
        with patch.dict(
            os.environ, {"GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE": val}
        ):
            assert structural_gate_guard_empty_shape_enabled() is False, val


def test_flag_on_when_explicit_one() -> None:
    from genie_space_optimizer.common.config import (
        structural_gate_guard_empty_shape_enabled,
    )
    with patch.dict(
        os.environ, {"GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE": "1"}
    ):
        assert structural_gate_guard_empty_shape_enabled() is True


# ── Task 13: guard helper ────────────────────────────────────────────


def test_guard_returns_original_verdict_when_flag_off(monkeypatch) -> None:
    """Guard is a no-op when GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE=0
    (rollback path) — caller gets back the same verdict the gate
    returned. Post default-ON flip, the rollback path must be
    exercised explicitly via env var."""
    from genie_space_optimizer.optimization.structural_repair_gate import (
        StructuralRepairGateVerdict,
    )
    from genie_space_optimizer.optimization.structural_repair_guard import (
        apply_empty_shape_backstop,
    )

    monkeypatch.setenv("GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE", "0")
    original = StructuralRepairGateVerdict.admitted(score=None)
    out = apply_empty_shape_backstop(
        verdict=original,
        intended_patch_shape="",
        rca_root_cause="",
    )
    assert out is original


def test_guard_overrides_to_rejected_when_both_empty(monkeypatch) -> None:
    """Flag ON + both metadata fields empty → REJECTED with
    terminal_reason no_rca_ground."""
    from genie_space_optimizer.optimization.structural_repair_gate import (
        StructuralRepairGateVerdict,
    )
    from genie_space_optimizer.optimization.structural_repair_guard import (
        apply_empty_shape_backstop,
    )
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )

    monkeypatch.setenv("GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE", "1")
    original = StructuralRepairGateVerdict.admitted(score=None)
    out = apply_empty_shape_backstop(
        verdict=original,
        intended_patch_shape="",
        rca_root_cause="",
    )
    assert out.outcome == "rejected"
    assert out.terminal_reason == TerminalReason.NO_RCA_GROUND.value
    assert out.repairability == original.repairability


def test_guard_no_op_when_intended_shape_set(monkeypatch) -> None:
    """Flag ON but intended_patch_shape is non-empty → no override.
    Tightens only the both-empty path."""
    from genie_space_optimizer.optimization.structural_repair_gate import (
        StructuralRepairGateVerdict,
    )
    from genie_space_optimizer.optimization.structural_repair_guard import (
        apply_empty_shape_backstop,
    )

    monkeypatch.setenv("GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE", "1")
    original = StructuralRepairGateVerdict.admitted(score=None)
    out = apply_empty_shape_backstop(
        verdict=original,
        intended_patch_shape="structural",
        rca_root_cause="",
    )
    assert out is original


def test_guard_no_op_when_root_cause_set(monkeypatch) -> None:
    """Flag ON but rca_root_cause is non-empty → no override."""
    from genie_space_optimizer.optimization.structural_repair_gate import (
        StructuralRepairGateVerdict,
    )
    from genie_space_optimizer.optimization.structural_repair_guard import (
        apply_empty_shape_backstop,
    )

    monkeypatch.setenv("GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE", "1")
    original = StructuralRepairGateVerdict.admitted(score=None)
    out = apply_empty_shape_backstop(
        verdict=original,
        intended_patch_shape="",
        rca_root_cause="rank without partition",
    )
    assert out is original


def test_guard_passes_through_rejected_verdict(monkeypatch) -> None:
    """Flag ON but the gate already rejected → no further override
    (the gate's terminal_reason is more specific)."""
    from genie_space_optimizer.optimization.structural_repair_gate import (
        StructuralRepairGateVerdict,
    )
    from genie_space_optimizer.optimization.structural_repair_guard import (
        apply_empty_shape_backstop,
    )
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )

    monkeypatch.setenv("GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE", "1")
    original = StructuralRepairGateVerdict(
        outcome="rejected",
        terminal_reason=TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value,
        repairability=None,
    )
    out = apply_empty_shape_backstop(
        verdict=original,
        intended_patch_shape="",
        rca_root_cause="",
    )
    assert out is original


# ── Harness wiring assertion ─────────────────────────────────────────


def test_harness_imports_empty_shape_backstop() -> None:
    """WU-5 wiring contract: harness.py must import
    apply_empty_shape_backstop from structural_repair_guard."""
    from genie_space_optimizer.optimization import harness

    src = open(harness.__file__).read()
    assert (
        "apply_empty_shape_backstop" in src
        or "_empty_shape_backstop" in src
    ), (
        "harness.py must wire structural_repair_guard."
        "apply_empty_shape_backstop at the structural-repair-gate "
        "consumer block for WU-5 to fire"
    )
