"""RCO-2a Task 12 — production-shape fixture tests.

Mirrors the RCO-8 pattern (``tests/unit/fixtures/rco8/_loader.py``).
Each fixture directory contains an ``input.json`` (the four evidence
inputs to ``build_contract_health_summary``) and an
``expected_output.json`` (the ``ContractHealthSummary.to_json_dict()``
result).
"""
from __future__ import annotations

import json
import pathlib

import pytest


FIXTURE_ROOT = pathlib.Path(__file__).parent / "fixtures" / "rco2a"


def _list_fixture_dirs():
    return sorted(p for p in FIXTURE_ROOT.iterdir() if p.is_dir())


@pytest.mark.parametrize("fixture_dir", _list_fixture_dirs(), ids=lambda p: p.name)
def test_builder_matches_expected_output(fixture_dir: pathlib.Path) -> None:
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    inp = json.loads((fixture_dir / "input.json").read_text(encoding="utf-8"))
    expected = json.loads((fixture_dir / "expected_output.json").read_text(encoding="utf-8"))
    summary = build_contract_health_summary(
        optimization_run_id=inp["optimization_run_id"],
        invariant_violations=inp["invariant_violations"],
        phase_h_strict_validation=inp.get("phase_h_strict_validation"),
        bundle_assembly_failed=tuple(inp.get("bundle_assembly_failed") or ()),
        bundle_assembly_incomplete=inp.get("bundle_assembly_incomplete"),
        replay_validation=inp.get("replay_validation"),
    )
    assert summary.to_json_dict() == expected
