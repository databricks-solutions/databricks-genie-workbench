"""RCO-4 Task 11 — fixture-driven parity test for the three extracted
helpers.

Each helper has at least one production-shape input.json /
expected_output.json pair. The test runs the helper against the input
and asserts the output matches expected_output. A floor test enforces
the minimum-case set.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


_FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures" / "rco4"


def _load_pairs(helper_name: str) -> list[tuple[str, dict, dict]]:
    out: list[tuple[str, dict, dict]] = []
    helper_dir = _FIXTURES_ROOT / helper_name
    if not helper_dir.is_dir():
        return out
    for case_dir in sorted(p for p in helper_dir.iterdir() if p.is_dir()):
        input_path = case_dir / "input.json"
        expected_path = case_dir / "expected_output.json"
        if not (input_path.exists() and expected_path.exists()):
            continue
        out.append((
            case_dir.name,
            json.loads(input_path.read_text()),
            json.loads(expected_path.read_text()),
        ))
    return out


_BLAST_CASES = _load_pairs("blast_radius_production")
_NARROW_CASES = _load_pairs("narrow_replacement")
_APPLY_CASES = _load_pairs("applyability")


def _id(case: tuple[str, Any, Any]) -> str:
    return case[0]


@pytest.mark.parametrize("case", _BLAST_CASES, ids=_id)
def test_blast_radius_production_fixture_parity(case) -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        BlastRadiusProductionInput,
    )
    from genie_space_optimizer.optimization.stages.gates import (
        run_blast_radius_production_gate,
    )

    name, payload, expected = case
    inp = BlastRadiusProductionInput.from_json(payload)
    out = run_blast_radius_production_gate(inp)
    actual = out.to_json()
    assert actual == expected, (
        f"RCO-4 blast-radius fixture '{name}' drifted. "
        f"Expected={expected!r} Actual={actual!r}."
    )


@pytest.mark.parametrize("case", _NARROW_CASES, ids=_id)
def test_narrow_replacement_fixture_parity(case) -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        NarrowReplacementInput,
    )
    from genie_space_optimizer.optimization.stages.gates import (
        resolve_narrow_replacement,
    )

    name, payload, expected = case
    inp = NarrowReplacementInput.from_json(payload["input"])
    survivors = tuple(payload.get("narrow_survivors") or ())
    out = resolve_narrow_replacement(inp, narrow_survivors=survivors)
    actual = out.to_json()
    assert actual == expected, (
        f"RCO-4 narrow-replacement fixture '{name}' drifted. "
        f"Expected={expected!r} Actual={actual!r}."
    )


@pytest.mark.parametrize("case", _APPLY_CASES, ids=_id)
def test_applyability_fixture_parity(case) -> None:
    from genie_space_optimizer.optimization.stages.gate_types import (
        ApplyabilityGateInput,
    )
    from genie_space_optimizer.optimization.stages.gates import (
        run_applyability_gate,
    )

    name, payload, expected = case
    inp = ApplyabilityGateInput.from_json(payload)
    out = run_applyability_gate(inp)
    actual = out.to_json()
    assert actual == expected, (
        f"RCO-4 applyability fixture '{name}' drifted. "
        f"Expected={expected!r} Actual={actual!r}."
    )


def test_floor_minimum_fixture_set() -> None:
    """RCO-4 floor — required cases must be present."""
    blast_names = {c[0] for c in _BLAST_CASES}
    narrow_names = {c[0] for c in _NARROW_CASES}
    apply_names = {c[0] for c in _APPLY_CASES}
    assert "airline_f9_two_dropped_one_kept" in blast_names
    assert "all_kept_no_dropped" in blast_names
    assert "branch_c_halt_no_survivor" in narrow_names
    assert "branch_a_survivor_replaces" in narrow_names
    assert "two_columns_one_applyable_one_not" in apply_names
