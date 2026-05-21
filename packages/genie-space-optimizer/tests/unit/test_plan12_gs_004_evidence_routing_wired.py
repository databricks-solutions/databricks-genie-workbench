"""Plan 12 PR 6 Task 6.2.3 deferred follow-up — gs_004 against the
WIRED-IN evidence-routing helper.

Anchors the gs_004 postmortem regression at the harness wire-in
layer: with the flag ON, ``_resolve_effective_lever_with_evidence_policy``
sees wrong_aggregation evidence on the gs_004 AG, refuses Lever 1,
and reroutes to Lever 5 (add_example_sql). One
``GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1`` marker carries the audit
trail.

The existing scaffold test ``test_l6_anchor_applyability_gs_004.py``
exercises Stage 1→2→3 directly (bypassing the production AG router).
This test exercises the harness wire-in directly against the gs_004
fixture so the regression cannot reoccur silently at the routing
layer.
"""
import json
from pathlib import Path


ANCHOR_FIXTURE = (
    Path(__file__).parent.parent
    / "replay" / "active" / "fixtures" / "anchor_qids" / "gs_004.json"
)


def _parse_marker(out: str) -> dict:
    """Extract the single GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1
    marker from captured stdout."""
    for line in out.splitlines():
        if line.startswith("GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1 "):
            return json.loads(line.partition(" ")[2])
    raise AssertionError(
        "Expected one GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1 marker; "
        f"captured stdout: {out!r}"
    )


def test_gs_004_wired_evidence_routing_reroutes_lever_1_to_5(
    capsys, monkeypatch,
):
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "1")

    fx = json.loads(ANCHOR_FIXTURE.read_text())
    # Build a gs_004-shaped AG: source cluster H001, wrong_aggregation
    # evidence, strategist chose Lever 1 (the postmortem regression).
    ag = {
        "id": "AG_gs_004",
        "source_cluster_ids": ["H001"],
        "asi_failure_type": fx["expected_evidence_kind"],
        "root_cause": fx["expected_evidence_kind"],
        "lever_directives": {"1": {"column_descriptions": []}},
    }

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=ag,
        optimization_run_id="run_gs_004",
        iteration=1,
        ag_id="AG_gs_004",
        cluster_id="H001",
    )
    # The crux: the policy MUST refuse Lever 1 for wrong_aggregation
    # and route to Lever 5 (add_example_sql via 5b → 5 mapping).
    assert effective == 5, (
        f"gs_004 wrong_aggregation evidence must route AWAY from Lever 1; "
        f"got effective={effective}"
    )

    marker = _parse_marker(capsys.readouterr().out)
    assert marker["evidence_kind"] == "wrong_aggregation"
    assert marker["target_lever_before"] == 1
    assert marker["target_lever_after"] == 5
    assert marker["reroute_applied"] is True
    assert marker["ag_id"] == "AG_gs_004"
    assert marker["cluster_id"] == "H001"
    assert marker["optimization_run_id"] == "run_gs_004"
    assert marker["iteration"] == 1


def test_gs_004_flag_off_preserves_postmortem_reproduction(
    capsys, monkeypatch,
):
    """Flag OFF is the byte-stable replay path. With the flag off,
    the helper returns ``target_lever=1`` unchanged — which is the
    EXACT regression the postmortem caught. This test anchors that
    behaviour so anyone disabling the flag inherits the original
    bug, not a half-fixed state."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "0")

    fx = json.loads(ANCHOR_FIXTURE.read_text())
    ag = {
        "id": "AG_gs_004",
        "source_cluster_ids": ["H001"],
        "asi_failure_type": fx["expected_evidence_kind"],
        "lever_directives": {"1": {"column_descriptions": []}},
    }

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=ag,
        optimization_run_id="run_gs_004",
        iteration=1,
        ag_id="AG_gs_004",
        cluster_id="H001",
    )
    # Flag OFF = postmortem regression behavior preserved.
    assert effective == 1
    out = capsys.readouterr().out
    assert "GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1" not in out
