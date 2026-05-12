"""RCO-2b — captured-trial-anchor byte-stable parity.

The May-12 consolidating trial captured two real ``GSO_CONTRACT_HEALTH_V1``
payloads. This test asserts that ``build_contract_health_summary``
reproduces those payloads byte-for-byte from inputs reconstructed from
the captured stdout (see fixture README and the RCO-2b plan for the
reconstruction logic).

If this test fails, either:

  1. The classifier semantics drifted (regression). Investigate before
     accepting the new output as canonical.
  2. The captured payloads were re-captured against a new trial run
     with different inputs. Update both ``input.json`` and
     ``expected_output.json`` for the affected anchor.
"""
from __future__ import annotations

import json
import pathlib

import pytest


FIXTURE_ROOT = pathlib.Path(__file__).parent / "fixtures" / "rco2b"


def _list_trial_anchor_dirs():
    if not FIXTURE_ROOT.exists():
        return []
    return sorted(p for p in FIXTURE_ROOT.iterdir() if p.is_dir())


@pytest.mark.parametrize(
    "fixture_dir",
    _list_trial_anchor_dirs(),
    ids=lambda p: p.name,
)
def test_builder_matches_captured_trial_payload(
    fixture_dir: pathlib.Path,
) -> None:
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    inp = json.loads((fixture_dir / "input.json").read_text(encoding="utf-8"))
    expected = json.loads(
        (fixture_dir / "expected_output.json").read_text(encoding="utf-8")
    )
    summary = build_contract_health_summary(
        optimization_run_id=inp["optimization_run_id"],
        invariant_violations=inp["invariant_violations"],
        phase_h_strict_validation=inp.get("phase_h_strict_validation"),
        bundle_assembly_failed=tuple(inp.get("bundle_assembly_failed") or ()),
        bundle_assembly_incomplete=inp.get("bundle_assembly_incomplete"),
        replay_validation=inp.get("replay_validation"),
    )
    assert summary.to_json_dict() == expected
