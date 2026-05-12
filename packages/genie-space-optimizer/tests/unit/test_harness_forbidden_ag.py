"""Defect Plan 1 G2 — unit tests for the cluster-signature collision
key.

Today's collision is keyed on ``(root_cause, blame, frozenset(
lever_set))``. The 7now-trial defect was that the LLM regenerated
``root_cause`` text on iteration N+1 so the same cluster's AG slipped
through. These tests pin the broadened lookup that ALSO keys on
``source_cluster_signatures`` (clusterer-derived, stable across
iterations by construction — see harness.py sha1 over
base_question_ids + root_cause + blame).
"""

from __future__ import annotations


def _reflection_entry(
    *,
    root_cause: str,
    cluster_signature: str,
    lever_set: list[int],
    rollback_class: str = "no_action",
    accepted: bool = False,
) -> dict:
    """Build a NO_ACTION reflection entry that the admission predicate
    accepts (non-empty root_cause + lever_set; escalation_handled
    False; matches the live shape from _build_reflection_entry).
    """
    return {
        "rollback_class": rollback_class,
        "rollback_reason": "no_proposals",
        "accepted": accepted,
        "escalation_handled": False,
        "root_cause": root_cause,
        "blame_set": ("gs_026",),
        "lever_set": lever_set,
        "source_cluster_signatures": [cluster_signature],
        "iteration": 1,
    }


def test_compute_forbidden_ag_set_pair_returns_root_and_signature_subsets(
    monkeypatch,
):
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    from genie_space_optimizer.optimization.harness import (
        _compute_forbidden_ag_set_pair,
    )

    buf = [
        _reflection_entry(
            root_cause="plural top-N collapse on zone_combination",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    pair = _compute_forbidden_ag_set_pair(buf)
    assert (
        "plural top-N collapse on zone_combination",
        ("gs_026",),
        frozenset({6}),
    ) in pair.by_root_cause
    assert ("sha1-cluster-026", frozenset({6})) in pair.by_signature


def test_ag_collision_key_pair_returns_both_root_and_signature_keys():
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
    )

    ag = {
        "id": "AG1",
        "source_cluster_signatures": ["sha1-cluster-026"],
    }
    pair = _ag_collision_key_pair(
        ag,
        ag_root_cause="plural top-N collapse on zone_combination",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )
    assert pair.root_cause_key == (
        "plural top-N collapse on zone_combination",
        ("gs_026",),
        frozenset({6}),
    )
    assert pair.signature_keys == (("sha1-cluster-026", frozenset({6})),)


def test_signature_collision_matches_even_when_root_cause_text_differs(
    monkeypatch,
):
    """The keystone behaviour pinned by this defect plan."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    # Iteration N: a NO_ACTION reflection lands with root_cause "A".
    buf = [
        _reflection_entry(
            root_cause="A — plural top-N collapse on zone_combination",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    forbidden_pair = _compute_forbidden_ag_set_pair(buf)

    # Iteration N+1: the strategist regenerates an AG for the same
    # cluster but the LLM phrased the root_cause as "B" (text drift).
    candidate_pair = _ag_collision_key_pair(
        {
            "id": "AG1",
            "source_cluster_signatures": ["sha1-cluster-026"],
        },
        ag_root_cause="B — top-N collapse / wrong table routing",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )

    # The root_cause key DOES NOT match (LLM drift).
    assert candidate_pair.root_cause_key not in forbidden_pair.by_root_cause

    # But the SIGNATURE key DOES match → overall collision.
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is True


def test_signature_collision_disabled_when_flag_off(monkeypatch):
    """Replay byte-stability — flag-off uses only the legacy
    root_cause key."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "0")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    buf = [
        _reflection_entry(
            root_cause="A",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    forbidden_pair = _compute_forbidden_ag_set_pair(buf)
    candidate_pair = _ag_collision_key_pair(
        {
            "id": "AG1",
            "source_cluster_signatures": ["sha1-cluster-026"],
        },
        ag_root_cause="B",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )

    # Flag off → only root_cause axis is consulted → no collision.
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is False


def test_signature_match_requires_lever_set_to_align():
    """A cluster collision still requires the lever family to match,
    otherwise a lever-family change correctly bypasses the gate."""
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    buf = [
        _reflection_entry(
            root_cause="A",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    forbidden_pair = _compute_forbidden_ag_set_pair(buf)

    # Same cluster signature, different lever family → no collision.
    candidate_pair = _ag_collision_key_pair(
        {"id": "AG1", "source_cluster_signatures": ["sha1-cluster-026"]},
        ag_root_cause="A",
        ag_blame_set=("gs_026",),
        lever_keys=["5"],
    )
    # The root_cause axis matches the same root_cause text but the
    # frozenset of levers differs, so root_cause_key isn't in
    # forbidden_pair.by_root_cause either. Both axes correctly miss.
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is False


def test_legacy_axis_still_matches_when_signatures_absent(monkeypatch):
    """Pre-existing behaviour — when AG has no source_cluster_signatures,
    the legacy root_cause axis is the only collision path and must
    still fire. Replay byte-stability for pre-defect-plan-1 fixtures.
    """
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    buf = [
        _reflection_entry(
            root_cause="A",
            cluster_signature="sha1-026",
            lever_set=[6],
        ),
    ]
    forbidden = _compute_forbidden_ag_set_pair(buf)

    # AG with NO source_cluster_signatures — must still collide via
    # the legacy axis.
    candidate = _ag_collision_key_pair(
        {"id": "AG1"},
        ag_root_cause="A",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )
    assert _collision_pair_matches(candidate, forbidden) is True
