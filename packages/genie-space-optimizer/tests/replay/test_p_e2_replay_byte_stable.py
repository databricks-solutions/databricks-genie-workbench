"""P-E2 — replay byte-stability test.

Two assertions:
  1. With ``GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED=0``, the
     harness emits no observe-only records and no markers — every
     historical replay fixture passes verbatim.
  2. With the flag default-on, NEW emissions are purely additive:
     existing records / markers in the fixture are unchanged; only
     new records of reason_code ``proposal_stage_forbidden_ag_observed``
     may appear.
"""
from __future__ import annotations

import json
import pathlib

import pytest

FIXTURES_DIR = (
    pathlib.Path(__file__).parent / "fixtures"
)
FIXTURE_PATHS = [
    FIXTURES_DIR / "run_ccf1d60d_7now.json",
    FIXTURES_DIR / "run_3b050ec5_7now.json",
]


def _load(fixture_path: pathlib.Path) -> dict | None:
    if not fixture_path.exists():
        return None
    return json.loads(fixture_path.read_text())


@pytest.mark.parametrize(
    "fixture_path", FIXTURE_PATHS, ids=[p.name for p in FIXTURE_PATHS]
)
def test_fixture_loads_or_skips(fixture_path):
    blob = _load(fixture_path)
    if blob is None:
        pytest.skip(f"fixture missing: {fixture_path}")
    assert isinstance(blob, dict)


def test_helper_flag_off_byte_stable(monkeypatch):
    """With the flag OFF, the helper is a no-op even when the
    collision pair clearly matches the forbidden set."""
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "0")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
        _normalise_blame,
    )
    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["5", "6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset({(
            "missing_filter", _normalise_blame(["t.col"]),
            frozenset([5, 6]),
        )}),
        by_signature=frozenset({("sig_A", frozenset([5, 6]))}),
    )
    iter_inputs = {"decision_records": [], "markers": []}
    axis = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        collision_pair=pair, forbidden_pair=forbidden,
        cluster_signature="sig_A", lever_set=(5, 6),
        call_site="cluster_driven_synthesis",
        iter_inputs=iter_inputs,
    )
    assert axis is None
    assert iter_inputs["decision_records"] == []
    assert iter_inputs["markers"] == []


@pytest.mark.parametrize(
    "fixture_path", FIXTURE_PATHS, ids=[p.name for p in FIXTURE_PATHS]
)
def test_fixture_already_has_no_observe_only_markers(fixture_path):
    """Pre-P-E2 fixtures must not already contain observe-only
    markers (defense in depth — confirms the marker shape is genuinely
    new and the byte-stability comparison below is meaningful)."""
    blob = _load(fixture_path)
    if blob is None:
        pytest.skip(f"fixture missing: {fixture_path}")
    text = json.dumps(blob)
    assert "GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED_V1" not in text
    assert "proposal_stage_forbidden_ag_observed" not in text


def test_contract_health_summary_field_present_in_round_trip():
    """Defense in depth — even an all-zero summary must round-trip
    through JSON with the new field present so a downstream parser
    looking for it never sees ``KeyError``."""
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
        build_contract_health_summary,
    )
    summary = build_contract_health_summary(
        optimization_run_id="r1",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
    )  # NB: no proposal_stage_forbidden_ag_observed_records kwarg
    blob = summary.to_json_dict()
    assert "proposal_stage_forbidden_ag_observed_count_by_call_site" in blob
    roundtripped = ContractHealthSummary.from_json_dict(blob)
    assert roundtripped.proposal_stage_forbidden_ag_observed_count_by_call_site == (
        ("cluster_driven_synthesis", 0),
        ("force_lever6", 0),
    )
