"""Plan 1 Task 11 — ClusterFindings.cluster_records typed sidecar.

The legacy ``clusters: tuple[dict, ...]`` field remains untouched
(byte-stable). A new derived field ``cluster_records:
tuple[FailureCluster, ...]`` is populated by __post_init__ from the
same dicts using ``FailureCluster.from_legacy``. Downstream stages
can read the typed records without changing their dict reads.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.stages.clustering import ClusterFindings


def _legacy_cluster_dict() -> dict:
    return {
        "cluster_id": "H001",
        "question_ids": ["gs_009"],
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "plural_top_n_collapse",
        "failure_keys": ["plural_top_n_collapse"],
        "asi_blame_set": ["flights.carrier"],
        "asi_blame_set_normalized": ["flights.carrier"],
        "rca_card": {"id": "rca_v1", "root_cause_summary": "needs top-n"},
    }


def test_cluster_findings_has_cluster_records_field() -> None:
    cf = ClusterFindings(clusters=())
    assert hasattr(cf, "cluster_records")
    assert cf.cluster_records == ()


def test_cluster_records_derived_from_clusters_on_construction() -> None:
    """When ``cluster_records`` is not supplied explicitly,
    ClusterFindings derives it from the legacy ``clusters`` tuple via
    FailureCluster.from_legacy."""
    cf = ClusterFindings(clusters=(_legacy_cluster_dict(),))
    assert len(cf.cluster_records) == 1
    fc = cf.cluster_records[0]
    assert isinstance(fc, FailureCluster)
    assert fc.cluster_id == "H001"
    assert fc.target_qids == ("gs_009",)


def test_explicit_cluster_records_not_overwritten() -> None:
    """If the caller supplies cluster_records explicitly, the
    derivation does not overwrite it."""
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("gs_009",),
        root_cause="plural_top_n_collapse",
        asi_failure_type="plural_top_n_collapse",
        failure_keys=("plural_top_n_collapse",),
        blame_set_raw=("flights.carrier",),
        blame_set_normalized=("flights.carrier",),
        rca_card_id="rca_v1",
        rca_card_summary="",
        is_grounded=True,
    )
    cf = ClusterFindings(
        clusters=({"cluster_id": "H001"},),
        cluster_records=(fc,),
    )
    assert cf.cluster_records[0] is fc


def test_round_trip_preserves_cluster_records() -> None:
    cf = ClusterFindings(clusters=(_legacy_cluster_dict(),))
    payload = cf.to_json()
    restored = ClusterFindings.from_json(payload)
    assert len(restored.cluster_records) == 1
    assert restored.cluster_records[0].cluster_id == "H001"


def test_cluster_records_skips_invalid_clusters_silently() -> None:
    """A malformed legacy cluster (e.g. missing cluster_id) must not
    crash derivation. The legacy clusters tuple keeps it; the typed
    records tuple just omits it."""
    cf = ClusterFindings(
        clusters=(_legacy_cluster_dict(), {"no_id_field": True}),
    )
    valid = [r for r in cf.cluster_records if r.cluster_id == "H001"]
    assert len(valid) == 1
    assert len(cf.clusters) == 2
