"""Phase 0 — production-shape regression for retired-AG admission.

Two independent live runs (airline 59a173d3, 7now ab65fefe) both
showed the same retired AG (H001 + Lever 6 or H001 + Lever 5)
being re-attempted after retirement. The bug is that the
candidate-side terminal-signature key was built from
``ag.get("source_cluster_ids")`` (cluster ids like H001) while
the retired side keys on ``target_qids`` (qids like
``..._gs_013``). Production AGs carry the qids on
``affected_questions``.
"""

from genie_space_optimizer.optimization.harness import (
    _ag_collision_key_pair,
    _collision_pair_matches,
    _ForbiddenSetPair,
)


def test_decomposed_ag_with_cluster_ids_and_qids_matches_retired_signature():
    """A production-shape decomposed AG must collide with a
    retired terminal signature keyed on the same qids."""
    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": [
            "7now_delivery_analytics_space_gs_013",
        ],
    }
    lever_keys = ["6"]
    candidate = _ag_collision_key_pair(
        ag=ag,
        ag_root_cause="wrong_filter_condition",
        ag_blame_set=[],
        lever_keys=lever_keys,
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset(),
        by_terminal_signature=frozenset({
            (
                frozenset({"7now_delivery_analytics_space_gs_013"}),
                frozenset({6}),
            ),
        }),
    )
    assert _collision_pair_matches(candidate, forbidden), (
        "Retired AG must be blocked at admission. Candidate key "
        f"terminal_signature_keys={candidate.terminal_signature_keys!r} "
        "did not match retired by_terminal_signature."
    )


def test_decomposed_ag_h002_lever5_matches_retired_signature():
    """Mirror test for H002/Lever 5 (airline gs_024, 7now gs_026)."""
    ag = {
        "id": "AG_DECOMPOSED_H002",
        "source_cluster_ids": ["H002"],
        "affected_questions": [
            "airline_ticketing_and_fare_analysis_gs_024",
        ],
    }
    candidate = _ag_collision_key_pair(
        ag=ag,
        ag_root_cause="missing_filter",
        ag_blame_set=[],
        lever_keys=["5"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset(),
        by_terminal_signature=frozenset({
            (
                frozenset({"airline_ticketing_and_fare_analysis_gs_024"}),
                frozenset({5}),
            ),
        }),
    )
    assert _collision_pair_matches(candidate, forbidden)


def test_unrelated_ag_still_proceeds():
    """Negative control: a different AG must NOT be blocked."""
    ag = {
        "id": "AG_DECOMPOSED_H003",
        "source_cluster_ids": ["H003"],
        "affected_questions": [
            "7now_delivery_analytics_space_gs_099",
        ],
    }
    candidate = _ag_collision_key_pair(
        ag=ag,
        ag_root_cause="wrong_filter_condition",
        ag_blame_set=[],
        lever_keys=["6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset(),
        by_terminal_signature=frozenset({
            (
                frozenset({"7now_delivery_analytics_space_gs_013"}),
                frozenset({6}),
            ),
        }),
    )
    assert not _collision_pair_matches(candidate, forbidden)
