"""Defect Plan 1 — unit tests for the harness wiring of the
``CLUSTER_BLOCKED_NO_RCA`` producer.

The harness logic itself is integration-heavy, so this test file
exercises the pure helper extracted in Task 4 step 3 below
(``collect_blocked_clusters``). The end-to-end behaviour is locked
in by Task 7's replay test.
"""

from __future__ import annotations


def test_collect_blocked_clusters_returns_ids_with_no_rca_card():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {"cluster_id": "H001", "rca_card": False, "root_cause": "wrong_agg"},
        {"cluster_id": "H002", "rca_card": True, "root_cause": "missing_filter"},
        {"cluster_id": "H003", "rca_card": None, "root_cause": "join_mismatch"},
        {"cluster_id": "H004", "rca_card": {}, "root_cause": "join_mismatch"},
        {"cluster_id": "H005", "rca_card": {"sections": [{}]}, "root_cause": "x"},
    ]

    result = collect_blocked_clusters(clusters)

    # H001 (False), H003 (None), H004 (empty dict) — all falsy → blocked.
    # H002 (True), H005 (non-empty dict) — truthy → grounded.
    assert sorted(result.blocked_cluster_ids) == ["H001", "H003", "H004"]
    assert len(result.records_payload) == 3
    assert {r["cluster_id"] for r in result.records_payload} == {
        "H001", "H003", "H004",
    }
    for payload in result.records_payload:
        assert payload["decision_type"] == "cluster_blocked_no_rca"
        assert payload["reason_code"] == "rca_ungrounded"


def test_collect_blocked_clusters_passes_root_cause_into_record():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {
            "cluster_id": "H001",
            "rca_card": False,
            "root_cause": "wrong_aggregation",
            "base_question_ids": [
                "airline_ticketing_and_fare_analysis_gs_009",
            ],
        }
    ]

    result = collect_blocked_clusters(clusters, run_id="r1", iteration=2)
    payload = result.records_payload[0]
    assert payload["root_cause"] == "wrong_aggregation"
    assert payload["target_qids"] == [
        "airline_ticketing_and_fare_analysis_gs_009"
    ]
    assert payload["iteration"] == 2


def test_collect_blocked_clusters_returns_empty_when_all_grounded():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {"cluster_id": "H001", "rca_card": {"sections": [{}]}},
        {"cluster_id": "H002", "rca_card": True},
    ]
    result = collect_blocked_clusters(clusters)
    assert result.blocked_cluster_ids == []
    assert result.records_payload == []


def test_collect_blocked_clusters_skips_clusters_without_id():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {"cluster_id": "", "rca_card": False},
        {"rca_card": False, "root_cause": "x"},
        {"cluster_id": "H001", "rca_card": False},
    ]
    result = collect_blocked_clusters(clusters)
    assert result.blocked_cluster_ids == ["H001"]
