"""Phase 0 — ``skipped_reason`` must reach decision records and
stdout markers end-to-end.

``ClusterSynthesisResult.skipped_reason`` carries the typed cause
when synthesis returns no candidate. Two live runs both emitted
``GSO_NO_STRUCTURAL_CANDIDATE_V1`` with ``"skipped_reason": ""``
for every iteration; the reason was lost at the gate-drop /
safety-net record emitters and at the harness stdout marker emit
site.
"""

import json
from unittest.mock import MagicMock

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    ClusterSynthesisResult,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    no_structural_candidate_marker,
)


def test_gate_drop_path_emits_record_with_skipped_reason():
    """forced_synthesis_dispatch gate-drop path must thread
    _synth_result.skipped_reason into no_structural_candidate_record."""
    from genie_space_optimizer.optimization import forced_synthesis_dispatch

    from genie_space_optimizer.optimization import decision_emitters

    captured_kwargs = []
    original = decision_emitters.no_structural_candidate_record

    def _spy(**kwargs):
        captured_kwargs.append(kwargs)
        return original(**kwargs)

    decision_emitters.no_structural_candidate_record = _spy
    try:
        cluster = {
            "cluster_id": "H001",
            "question_ids": ["7now_delivery_analytics_space_gs_013"],
            "root_cause": "wrong_filter_condition",
            "rca_card": {"id": "rca-1", "root_cause_summary": "ok"},
        }
        ag = {
            "id": "AG_DECOMPOSED_H001",
            "source_cluster_ids": ["H001"],
            "affected_questions": ["7now_delivery_analytics_space_gs_013"],
        }
        drop = {
            "ag_id": "AG_DECOMPOSED_H001",
            "source_clusters": ["H001"],
            "root_causes": ("wrong_filter_condition",),
        }

        def _synth_returns_no_candidate(*_args, **_kwargs):
            return ClusterSynthesisResult(
                proposal=None,
                attempted_archetypes=(),
                skipped_reason="no_archetype_or_slice",
            )

        forced_synthesis_dispatch.dispatch_forced_structural_synthesis(
            ag=ag,
            iteration=1,
            l5_ag_drops=[drop],
            reflection_buffer=[],
            iter_source_clusters_by_id={"H001": cluster},
            iter_rca_id_by_cluster={"H001": "rca-1"},
            w=MagicMock(),
            benchmarks=[],
            run_id="test-run",
            metadata_snapshot={
                "data_sources": {"tables": [], "metric_views": []},
            },
            catalog="",
            schema="",
            spark=None,
            lever_keys=[5],
            current_iter_inputs={},
            synthesize=_synth_returns_no_candidate,
            ag_proposals_so_far=[],
        )
    finally:
        decision_emitters.no_structural_candidate_record = original

    assert captured_kwargs, "no_structural_candidate_record was not called"
    assert captured_kwargs[0].get("skipped_reason") == (
        "no_archetype_or_slice"
    ), (
        f"gate-drop path did not pass skipped_reason; got "
        f"{captured_kwargs[0].get('skipped_reason')!r}"
    )


def test_safety_net_path_emits_record_with_skipped_reason():
    """forced_synthesis_dispatch safety-net path must thread
    _synth_result.skipped_reason into no_structural_candidate_record."""
    from genie_space_optimizer.optimization import forced_synthesis_dispatch

    from genie_space_optimizer.optimization import decision_emitters

    captured_kwargs = []
    original = decision_emitters.no_structural_candidate_record

    def _spy(**kwargs):
        captured_kwargs.append(kwargs)
        return original(**kwargs)

    decision_emitters.no_structural_candidate_record = _spy
    try:
        cluster = {
            "cluster_id": "H002",
            "question_ids": ["airline_ticketing_and_fare_analysis_gs_024"],
            "asi_failure_type": "missing_filter",
            "root_cause": "missing_filter",
            "rca_card": {"id": "rca-2", "root_cause_summary": "ok"},
        }
        ag = {
            "id": "AG_DECOMPOSED_H002",
            "source_cluster_ids": ["H002"],
            "affected_questions": [
                "airline_ticketing_and_fare_analysis_gs_024",
            ],
        }

        def _synth_returns_no_candidate(*_args, **_kwargs):
            return ClusterSynthesisResult(
                proposal=None,
                attempted_archetypes=(),
                skipped_reason="validate_afs_rejected",
            )

        forced_synthesis_dispatch.dispatch_forced_structural_synthesis(
            ag=ag,
            iteration=1,
            l5_ag_drops=[],
            reflection_buffer=[],
            iter_source_clusters_by_id={"H002": cluster},
            iter_rca_id_by_cluster={"H002": "rca-2"},
            w=MagicMock(),
            benchmarks=[],
            run_id="test-run",
            metadata_snapshot={
                "data_sources": {"tables": [], "metric_views": []},
            },
            catalog="",
            schema="",
            spark=None,
            lever_keys=[5],
            current_iter_inputs={},
            synthesize=_synth_returns_no_candidate,
            ag_proposals_so_far=[],
        )
    finally:
        decision_emitters.no_structural_candidate_record = original

    safety_net_calls = [
        kw for kw in captured_kwargs
        if kw.get("root_cause") == "missing_filter"
    ]
    assert safety_net_calls, "safety-net path did not emit a record"
    assert safety_net_calls[0].get("skipped_reason") == (
        "validate_afs_rejected"
    ), (
        f"safety-net path did not pass skipped_reason; got "
        f"{safety_net_calls[0].get('skipped_reason')!r}"
    )


def test_marker_carries_skipped_reason_from_record():
    """The harness NSC marker emission must read skipped_reason from
    the record's metrics dict so postmortems see the typed cause."""
    record_dict = {
        "ag_id": "AG_DECOMPOSED_H001",
        "iteration": 1,
        "metrics": {
            "attempted_archetypes": [],
            "skipped_reason": "no_archetype_or_slice",
        },
    }
    marker = no_structural_candidate_marker(
        ag_id=str(record_dict.get("ag_id") or ""),
        iteration=int(record_dict.get("iteration") or 0),
        attempted_archetypes=(
            record_dict.get("metrics", {}).get("attempted_archetypes")
            or ()
        ),
        skipped_reason=str(
            record_dict.get("metrics", {}).get("skipped_reason") or ""
        ),
    )
    assert "GSO_NO_STRUCTURAL_CANDIDATE_V1" in marker
    payload_json = marker.split(" ", 1)[1]
    payload = json.loads(payload_json)
    assert payload["skipped_reason"] == "no_archetype_or_slice", (
        f"marker skipped_reason was {payload['skipped_reason']!r}; "
        "expected typed cause from the record metrics dict."
    )
