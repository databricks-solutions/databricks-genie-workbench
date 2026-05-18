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


def test_synthesizer_accepts_failure_cluster_input():
    """Phase 1.2 — run_cluster_driven_synthesis_for_single_cluster
    must accept a FailureCluster in addition to the legacy dict.

    The new signature is positional-compatible (FailureCluster OR
    Mapping). Internally the function calls
    FailureCluster.from_legacy() if a Mapping was passed."""
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        run_cluster_driven_synthesis_for_single_cluster,
    )

    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("7now_delivery_analytics_space_gs_013",),
        root_cause="wrong_filter_condition",
        asi_failure_type="other",
        failure_keys=("wrong_filter_condition",),
        blame_set_raw=("[FILTER]",),
        blame_set_normalized=(),
        rca_card_id="rca-test",
        rca_card_summary="test",
        is_grounded=True,
    )
    # Use the function with empty downstream context so it should
    # short-circuit with a typed skipped_reason. We are not asserting
    # the specific reason here — just that the function accepts a
    # FailureCluster input without raising a TypeError.
    result = run_cluster_driven_synthesis_for_single_cluster(
        fc,
        metadata_snapshot={"data_sources": {"tables": [], "metric_views": []}},
        benchmarks=None,
    )
    # ClusterSynthesisResult is the return type; it must have a
    # non-empty skipped_reason when proposal is None.
    assert result.proposal is None
    assert result.skipped_reason != "", (
        "When proposal is None, skipped_reason must be non-empty "
        "per the Phase 1 refuse-on-empty invariant."
    )


def test_dispatcher_passes_failure_cluster_to_synthesizer():
    """Phase 1.3 — dispatch_forced_structural_synthesis must build
    a FailureCluster at entry and pass it to the synthesizer.

    Verified by spying on the synthesize callable: its first
    positional argument must be a FailureCluster, not a raw dict.
    """
    from unittest.mock import MagicMock
    from genie_space_optimizer.optimization import forced_synthesis_dispatch
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )

    received_first_args: list = []

    def _spy_synth(cluster_arg, *args, **kwargs):
        received_first_args.append(cluster_arg)
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=(),
            skipped_reason="no_archetype_or_slice",
        )

    cluster = {
        "cluster_id": "H001",
        "question_ids": ["7now_delivery_analytics_space_gs_013"],
        "root_cause": "wrong_filter_condition",
        "asi_failure_type": "wrong_filter_condition",
        "rca_card": {"id": "rca-1", "root_cause_summary": "test"},
    }
    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["7now_delivery_analytics_space_gs_013"],
    }
    drop = {
        "drop_reason": "lever5_structural_sql_shape_no_example_sql",
        "root_causes": ["wrong_filter_condition"],
        "source_clusters": ["H001"],
    }

    forced_synthesis_dispatch.dispatch_forced_structural_synthesis(
        ag=ag,
        run_id="test-run",
        iteration=1,
        l5_ag_drops=[drop],
        reflection_buffer=[],
        iter_source_clusters_by_id={"H001": cluster},
        iter_rca_id_by_cluster={"H001": "rca-1"},
        w=MagicMock(),
        benchmarks=[],
        metadata_snapshot={
            "data_sources": {"tables": [], "metric_views": []},
        },
        catalog="",
        schema="",
        spark=None,
        lever_keys=[5],
        current_iter_inputs={},
        synthesize=_spy_synth,
        ag_proposals_so_far=[],
    )

    assert received_first_args, "synthesize callable was not invoked"
    assert isinstance(received_first_args[0], FailureCluster), (
        f"dispatcher must pass a FailureCluster to synthesize; "
        f"got type={type(received_first_args[0]).__name__}"
    )
    assert received_first_args[0].cluster_id == "H001"
    assert received_first_args[0].target_qids == (
        "7now_delivery_analytics_space_gs_013",
    )


def test_collision_key_pair_built_from_failure_cluster_matches_retired():
    """Phase 1.4 — FailureCluster.collision_key_pair must produce
    a key that matches the retired-signature producer's projection
    end-to-end. Typed-contract replacement for the Phase 0.1 fix."""
    from genie_space_optimizer.optimization.harness import (
        _ForbiddenSetPair,
        _collision_pair_matches,
    )

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
    candidate_key = fc.collision_key_pair(lever_keys=[6])
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
    assert _collision_pair_matches(candidate_key, forbidden)


def test_ag_collision_key_pair_from_failure_cluster_matches_dict_version():
    """The typed helper and the legacy dict helper must produce
    byte-identical terminal_signature_keys for production-shaped
    inputs. This proves the migration is a refactor, not a
    behavior change."""
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ag_collision_key_pair_from_failure_cluster,
    )

    cluster = {
        "cluster_id": "H001",
        "question_ids": ["7now_delivery_analytics_space_gs_013"],
        "root_cause": "wrong_filter_condition",
        "asi_failure_type": "other",
    }
    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["7now_delivery_analytics_space_gs_013"],
    }

    legacy_key = _ag_collision_key_pair(
        ag, "wrong_filter_condition", [], ["6"],
    )
    fc = FailureCluster.from_legacy(cluster, ag=ag)
    typed_key = _ag_collision_key_pair_from_failure_cluster(
        fc,
        ag=ag,
        ag_root_cause="wrong_filter_condition",
        ag_blame_set=[],
        lever_keys=["6"],
    )
    assert legacy_key.terminal_signature_keys == typed_key.terminal_signature_keys


def test_harness_collision_guard_helper_is_callable():
    """Phase 1.6 — the typed helper from Phase 1.4 is importable
    and callable from the harness module. The Phase 1.6 collision
    guard swap at line ~22283 will use it when a source cluster is
    available for the AG."""
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair_from_failure_cluster,
    )
    assert callable(_ag_collision_key_pair_from_failure_cluster)


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
