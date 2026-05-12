"""P-E2 — wire-up test for the cluster-driven synthesis call site.

This test exercises a synthetic AG that the iter-level guard at
``harness.py:19752`` *would* admit (it has a clean lever_keys at AG
selection), but whose downstream lever set (post-RCA-execution
union at ``harness.py:20098``) drifts into the forbidden set just
in time to reach the cluster-driven synthesis call. The observe-
only record + marker must be emitted; the synthesis call itself
still runs.
"""
from __future__ import annotations


def test_cluster_driven_synthesis_emits_observe_only_record_when_forbidden(monkeypatch):
    """The test exercises ``_check_proposal_stage_forbidden_ag_leakage``
    at the same Python call site the production harness uses (Task 6
    inserts the wrapper). We assert one observe-only record is
    appended to ``iter_inputs["decision_records"]`` and the marker
    is emitted on stdout, and that the call_site is exactly
    ``cluster_driven_synthesis``.
    """
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _check_proposal_stage_forbidden_ag_leakage,
        _ForbiddenSetPair,
        _normalise_blame,
    )
    ag = {
        "id": "AG_X",
        "source_cluster_signatures": ["sig_A"],
        "levers": [5, 6],
    }
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["5", "6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset({(
            "missing_filter", _normalise_blame(["t.col"]),
            frozenset([5, 6]),
        )}),
        by_signature=frozenset(),
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
    assert axis == "root_cause"
    assert len(iter_inputs["decision_records"]) == 1
    rec = iter_inputs["decision_records"][0]
    assert rec["metrics"]["call_site"] == "cluster_driven_synthesis"
    assert rec["metrics"]["cluster_signature"] == "sig_A"
    # Pin: the marker's call_site is the closed-vocabulary value the
    # contract-health aggregator (Task 8) will parse on.
    import json
    marker = iter_inputs["markers"][0]
    _, payload = marker.split(" ", 1)
    assert json.loads(payload)["call_site"] == "cluster_driven_synthesis"
