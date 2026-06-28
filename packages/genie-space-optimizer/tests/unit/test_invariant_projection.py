"""Pre-step Cycle 11: contract tests for invariant_projection.

These tests pin the shape that ``project_iter_evidence`` must
emit so the live invariant runner in ``_run_iteration_invariants_and_append_records``
can exercise I2/I3/I4/I7 against real iteration state. The replay
fixture in ``tests/replay/test_invariants_against_fixtures.py``
asserts the integration against the run-900000000000001 capture.
"""

from __future__ import annotations

import pytest


def _build_minimal_iter_inputs() -> dict:
    return {
        "iteration": 1,
        "eval_rows": [],
        "clusters": [
            {
                "cluster_id": "H002",
                "recommended_levers": [6, 1],
                "qids": ["7now_delivery_analytics_space_gs_026"],
            }
        ],
        "soft_clusters": [],
        "strategist_response": {
            "action_groups": [
                {
                    "id": "AG1",
                    "Levers": [1, 6],
                    "source_cluster_ids": ["H002"],
                    "root_cause": "plural_top_n_collapse",
                }
            ]
        },
        "ag_outcomes": {"AG1": "rolled_back"},
        "post_eval_passing_qids": [],
        "journey_validation": None,
        "decision_records": [
            {
                "decision_type": "control_plane_acceptance",
                "ag_id": "AG1",
                "iteration": 1,
                "target_qids": ["7now_delivery_analytics_space_gs_026"],
                "target_fixed_qids": [],
                "target_still_hard_qids": [
                    "7now_delivery_analytics_space_gs_026"
                ],
                "reason_code": "target_qids_not_improved",
            }
        ],
    }


def test_project_iter_evidence_returns_iterations_list_with_one_entry() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    out = project_iter_evidence(
        current_iter_inputs=_build_minimal_iter_inputs(),
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    iters = out.get("iterations") or []
    assert len(iters) == 1
    assert int(iters[0]["iteration"]) == 1


def test_project_iter_evidence_carries_clusters_and_recommended_levers() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    out = project_iter_evidence(
        current_iter_inputs=_build_minimal_iter_inputs(),
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    cluster = out["iterations"][0]["clusters"][0]
    assert str(cluster["cluster_id"]) == "H002"
    assert sorted(int(x) for x in cluster["recommended_levers"]) == [1, 6]


def test_project_iter_evidence_carries_ags_with_levers_and_source_clusters() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    out = project_iter_evidence(
        current_iter_inputs=_build_minimal_iter_inputs(),
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    ag = out["iterations"][0]["ags"][0]
    assert str(ag["id"]) == "AG1"
    assert sorted(int(x) for x in ag["levers"]) == [1, 6]
    assert list(ag["source_cluster_ids"]) == ["H002"]


def test_project_iter_evidence_extracts_acceptance_decision_from_decision_records() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    out = project_iter_evidence(
        current_iter_inputs=_build_minimal_iter_inputs(),
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    ad = out["iterations"][0]["acceptance_decision"]
    assert list(ad["target_qids"]) == [
        "7now_delivery_analytics_space_gs_026"
    ]
    assert list(ad["target_still_hard_qids"]) == [
        "7now_delivery_analytics_space_gs_026"
    ]
    assert str(ad["reason_code"]) == "target_qids_not_improved"


def test_project_iter_evidence_records_open_hard_clusters_and_rca_presence() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    inputs = _build_minimal_iter_inputs()
    inputs["clusters"].append(
        {"cluster_id": "H001", "recommended_levers": [6], "qids": ["q13"]}
    )
    inputs["rca_cards_present"] = {"H002": True, "H001": False}
    out = project_iter_evidence(
        current_iter_inputs=inputs,
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    it = out["iterations"][0]
    assert sorted(it["open_hard_cluster_ids"]) == ["H001", "H002"]
    assert it["rca_cards_present"]["H001"] is False
    assert it["rca_cards_present"]["H002"] is True


def test_project_iter_evidence_appends_to_prior_iterations_for_i4_history() -> None:
    """I4 (no silent retry) needs prev + curr iteration in the same evidence."""
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    iter1 = project_iter_evidence(
        current_iter_inputs=_build_minimal_iter_inputs(),
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )
    iter2_inputs = _build_minimal_iter_inputs()
    iter2_inputs["iteration"] = 2
    iter2 = project_iter_evidence(
        current_iter_inputs=iter2_inputs,
        iteration=2,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=iter1,
    )

    assert [int(x["iteration"]) for x in iter2["iterations"]] == [1, 2]


def test_project_iter_evidence_carries_decision_records_for_i7_blocked_clusters() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    inputs = _build_minimal_iter_inputs()
    inputs["decision_records"].append(
        {
            "decision_type": "cluster_blocked_no_rca",
            "cluster_id": "H001",
            "iteration": 1,
            "reason": "no_fit_card",
        }
    )
    out = project_iter_evidence(
        current_iter_inputs=inputs,
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    drs = out["iterations"][0]["decision_records"]
    assert any(
        str(r.get("decision_type")) == "cluster_blocked_no_rca"
        and str(r.get("cluster_id")) == "H001"
        for r in drs
    )


def test_project_iter_evidence_is_pure_does_not_mutate_inputs() -> None:
    """Projector must be safe to call from a finally block."""
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    inputs = _build_minimal_iter_inputs()
    snapshot_keys = set(inputs.keys())
    snapshot_records = list(inputs["decision_records"])

    project_iter_evidence(
        current_iter_inputs=inputs,
        iteration=1,
        run_id="r1",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    assert set(inputs.keys()) == snapshot_keys
    assert inputs["decision_records"] == snapshot_records


def test_project_iter_evidence_returns_empty_iterations_when_run_id_blank() -> None:
    """Mirrors the early-return contract of the harness wrapper."""
    from genie_space_optimizer.optimization.invariant_projection import (
        project_iter_evidence,
    )

    out = project_iter_evidence(
        current_iter_inputs=_build_minimal_iter_inputs(),
        iteration=1,
        run_id="",
        iter_producer_exceptions=None,
        prior_iter_evidence=None,
    )

    assert out == {
        "phase_b": {"total_records": 1, "producer_exceptions": {}},
        "replay_fixture_records": 0,
        "iterations": [],
        "manifest": {"declared_paths": [], "materialized_paths": []},
        "convergence": {},
    }
