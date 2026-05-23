"""Phase 3 — per-iteration parity marker + fail-closed contract.

Three cases:

* **Parity (Case A)** — harness/SM/Plan-11 all see the same hard qids; one
  ``GSO_INPUT_PROJECTION_PARITY_V1`` marker is emitted with empty diffs;
  no exception.
* **Drift (Case B)** — Plan-11 sees a subset of the harness hard qids; the
  parity marker reports the drift via ``missing_from_plan11`` but does NOT
  raise (drift is observable; only total starvation is fatal).
* **Starvation (Case C)** — harness sees hard rows; SM AND Plan-11 both see
  zero; the violation marker fires AND ``InputProjectionContractViolation``
  raises so the lever-loop cannot silently continue on the legacy lane.
"""
from __future__ import annotations

import json

import pytest


def _capture_marker_payload(captured: str, marker: str) -> dict:
    for line in captured.splitlines():
        if line.startswith(marker + " "):
            return json.loads(line.split(" ", 1)[1])
    raise AssertionError(f"expected to find {marker} in captured output")


def test_parity_case_no_raise_no_drift(capsys: pytest.CaptureFixture[str]) -> None:
    from genie_space_optimizer.optimization.input_projection_contract import (
        assert_input_projection_parity,
    )

    harness_hard = {"gs_009", "gs_024"}
    sm_hard = {"gs_009", "gs_024"}
    plan11_hard = {"gs_009", "gs_024"}

    assert_input_projection_parity(
        iteration=1,
        harness_hard_qids=harness_hard,
        plan11_hard_qids=plan11_hard,
        state_machine_hard_qids=sm_hard,
    )

    captured = capsys.readouterr().out
    payload = _capture_marker_payload(captured, "GSO_INPUT_PROJECTION_PARITY_V1")
    assert sorted(payload["harness_hard_qids"]) == ["gs_009", "gs_024"]
    assert sorted(payload["plan11_hard_qids"]) == ["gs_009", "gs_024"]
    assert sorted(payload["state_machine_hard_qids"]) == ["gs_009", "gs_024"]
    assert payload["missing_from_plan11"] == []
    assert payload["missing_from_sm"] == []
    assert "GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1" not in captured


def test_drift_case_marker_shows_missing_but_no_raise(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from genie_space_optimizer.optimization.input_projection_contract import (
        assert_input_projection_parity,
    )

    assert_input_projection_parity(
        iteration=2,
        harness_hard_qids={"gs_009", "gs_024"},
        plan11_hard_qids={"gs_009"},
        state_machine_hard_qids={"gs_009", "gs_024"},
    )

    captured = capsys.readouterr().out
    payload = _capture_marker_payload(captured, "GSO_INPUT_PROJECTION_PARITY_V1")
    assert payload["missing_from_plan11"] == ["gs_024"]
    assert payload["missing_from_sm"] == []
    # Drift is observable but not fatal: the violation marker must NOT fire.
    assert "GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1" not in captured


def test_starvation_case_raises_and_emits_violation_marker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from genie_space_optimizer.optimization.input_projection_contract import (
        InputProjectionContractViolation,
        assert_input_projection_parity,
    )

    with pytest.raises(InputProjectionContractViolation) as excinfo:
        assert_input_projection_parity(
            iteration=3,
            harness_hard_qids={"gs_009", "gs_024"},
            plan11_hard_qids=set(),
            state_machine_hard_qids=set(),
        )

    assert "starved" in str(excinfo.value).lower()
    captured = capsys.readouterr().out
    # The parity marker still fires so post-mortem tooling can read both signals.
    parity = _capture_marker_payload(captured, "GSO_INPUT_PROJECTION_PARITY_V1")
    assert sorted(parity["missing_from_plan11"]) == ["gs_009", "gs_024"]
    assert sorted(parity["missing_from_sm"]) == ["gs_009", "gs_024"]

    violation = _capture_marker_payload(
        captured, "GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1"
    )
    assert violation["iteration"] == 3
    assert violation["harness_hard_count"] == 2
    assert violation["plan11_hard_count"] == 0
    assert violation["sm_hard_count"] == 0


def test_no_hard_rows_emits_no_marker_and_no_raise(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When the harness sees zero hard rows there is nothing to assert; the
    parity helper must be silent (no marker, no raise)."""
    from genie_space_optimizer.optimization.input_projection_contract import (
        assert_input_projection_parity,
    )

    assert_input_projection_parity(
        iteration=4,
        harness_hard_qids=set(),
        plan11_hard_qids=set(),
        state_machine_hard_qids=set(),
    )

    captured = capsys.readouterr().out
    assert "GSO_INPUT_PROJECTION_PARITY_V1" not in captured
    assert "GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1" not in captured
