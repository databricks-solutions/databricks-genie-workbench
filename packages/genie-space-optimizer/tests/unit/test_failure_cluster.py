"""Phase 1 — FailureCluster dataclass behavior tests.

The FailureCluster dataclass is the typed contract that carries
all five identifier aliases for a Genie Space failure across the
critical synthesis path. Tests verify identity invariants
(target_qids and affected_questions must agree), the refuse-on-empty
projection behavior, and round-tripping with the legacy dict form.
"""

import pytest

from genie_space_optimizer.optimization.failure_cluster import (
    FailureCluster,
    FailureClusterIdentityError,
)


def test_failure_cluster_canonical_construction():
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("7now_delivery_analytics_space_gs_013",),
        root_cause="wrong_filter_condition",
        asi_failure_type="other",
        failure_keys=("wrong_filter_condition", "wrong_aggregation"),
        blame_set_raw=("[FILTER]", "time_window"),
        blame_set_normalized=(),
        rca_card_id="",
        rca_card_summary="",
        is_grounded=False,
    )
    assert fc.cluster_id == "H001"
    assert fc.target_qids == ("7now_delivery_analytics_space_gs_013",)
    # affected_questions is a derived alias of target_qids
    assert fc.affected_questions == fc.target_qids


def test_failure_cluster_from_legacy_dicts():
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["7now_delivery_analytics_space_gs_013"],
        "root_cause": "wrong_filter_condition",
        "asi_failure_type": "other",
        "rca_card": {},
        "asi_blame_set": ["[FILTER]"],
    }
    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["7now_delivery_analytics_space_gs_013"],
    }
    fc = FailureCluster.from_legacy(cluster, ag)
    assert fc.cluster_id == "H001"
    assert fc.target_qids == ("7now_delivery_analytics_space_gs_013",)
    assert fc.root_cause == "wrong_filter_condition"
    assert fc.asi_failure_type == "other"
    assert fc.blame_set_raw == ("[FILTER]",)
    assert fc.is_grounded is False  # empty rca_card


def test_failure_cluster_identity_mismatch_raises():
    """If cluster.question_ids and ag.affected_questions disagree, raise.

    The whole point of FailureCluster is to make identity mismatches
    Python errors at construction time, not silent zeroes in postmortems.
    """
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["7now_delivery_analytics_space_gs_013"],
    }
    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["7now_delivery_analytics_space_gs_999"],
    }
    with pytest.raises(FailureClusterIdentityError) as exc_info:
        FailureCluster.from_legacy(cluster, ag)
    assert "target_qids" in str(exc_info.value)
    assert "H001" in str(exc_info.value)


def test_failure_cluster_collision_key_uses_target_qids():
    """The terminal-signature collision key must use target_qids,
    matching what _compute_forbidden_ag_set_pair produces. This is
    the typed replacement for the Phase 0.1 fix."""
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("7now_delivery_analytics_space_gs_013",),
        root_cause="wrong_filter_condition",
        asi_failure_type="other",
        failure_keys=("wrong_filter_condition",),
        blame_set_raw=(),
        blame_set_normalized=(),
        rca_card_id="",
        rca_card_summary="",
        is_grounded=False,
    )
    key = fc.collision_key_pair(lever_keys=[6])
    assert key.terminal_signature_keys == (
        (
            frozenset({"7now_delivery_analytics_space_gs_013"}),
            frozenset({6}),
        ),
    )


def test_nsc_marker_payload_refuses_when_synthesizer_reported_nothing():
    """Projection refuses construction when synthesizer claimed
    neither a skipped_reason nor any attempted archetypes. This is
    the architectural invariant: the synthesizer always knows
    something."""
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("7now_delivery_analytics_space_gs_013",),
        root_cause="wrong_filter_condition",
        asi_failure_type="other",
        failure_keys=("wrong_filter_condition",),
        blame_set_raw=(),
        blame_set_normalized=(),
        rca_card_id="",
        rca_card_summary="",
        is_grounded=False,
    )
    with pytest.raises(ValueError) as exc_info:
        fc.to_nsc_marker_payload(
            ag_id="AG_DECOMPOSED_H001",
            iteration=1,
            skipped_reason="",
            attempted_archetypes=(),
        )
    msg = str(exc_info.value)
    assert "skipped_reason" in msg or "attempted_archetypes" in msg
    assert "H001" in msg


def test_nsc_marker_payload_passes_with_skipped_reason():
    """Positive case: a typed skipped_reason is sufficient."""
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("7now_delivery_analytics_space_gs_013",),
        root_cause="wrong_filter_condition",
        asi_failure_type="other",
        failure_keys=("wrong_filter_condition",),
        blame_set_raw=(),
        blame_set_normalized=(),
        rca_card_id="",
        rca_card_summary="",
        is_grounded=False,
    )
    payload = fc.to_nsc_marker_payload(
        ag_id="AG_DECOMPOSED_H001",
        iteration=1,
        skipped_reason="missing_rca_card",
        attempted_archetypes=(),
    )
    assert payload["ag_id"] == "AG_DECOMPOSED_H001"
    assert payload["iteration"] == 1
    assert payload["skipped_reason"] == "missing_rca_card"
    assert payload["attempted_archetypes"] == []
