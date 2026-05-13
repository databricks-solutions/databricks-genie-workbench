"""Phase 3 Action 3.3.2 — lift_soft_evidence_to_kit_lookup tests."""

from __future__ import annotations

from genie_space_optimizer.optimization.soft_evidence_lift import (
    lift_soft_evidence_to_kit_lookup,
)


def _cluster(
    *,
    primary_cluster_id: str,
    target_qids: tuple[str, ...],
    repair_kit: dict | None,
    soft_evidence: list[dict] | None,
) -> dict:
    out = {
        "primary_cluster_id": primary_cluster_id,
        "target_qids": target_qids,
    }
    if repair_kit is not None:
        out["_repair_kit"] = repair_kit
    if soft_evidence is not None:
        out["rca_card_supporting_soft_evidence"] = soft_evidence
    return out


def test_lift_returns_empty_when_no_clusters_have_soft_evidence() -> None:
    clusters = [
        _cluster(
            primary_cluster_id="cluster_h001",
            target_qids=("gs_001",),
            repair_kit={"kit_id": "kit_1"},
            soft_evidence=None,
        ),
    ]
    assert lift_soft_evidence_to_kit_lookup(clusters) == {}


def test_lift_pairs_kit_id_with_soft_qids_from_supporting_evidence() -> None:
    clusters = [
        _cluster(
            primary_cluster_id="cluster_h002",
            target_qids=("gs_021",),
            repair_kit={"kit_id": "kit_h002"},
            soft_evidence=[
                {"soft_qid": f"gs_{i:03d}", "soft_cluster_id": "S001",
                 "match_kind": "matching_counterfactual",
                 "evidence_token": "time_window",
                 "soft_counterfactual": "Add filter on time_window"}
                for i in range(1, 12)
            ],
        ),
    ]
    out = lift_soft_evidence_to_kit_lookup(clusters)
    assert "kit_h002" in out
    assert set(out["kit_h002"]) == {f"gs_{i:03d}" for i in range(1, 12)}


def test_lift_returns_sorted_deterministic_qid_tuple() -> None:
    clusters = [
        _cluster(
            primary_cluster_id="cluster_h002",
            target_qids=("gs_021",),
            repair_kit={"kit_id": "kit_h002"},
            soft_evidence=[
                {"soft_qid": "gs_005"},
                {"soft_qid": "gs_001"},
                {"soft_qid": "gs_003"},
            ],
        ),
    ]
    out = lift_soft_evidence_to_kit_lookup(clusters)
    assert out["kit_h002"] == ("gs_001", "gs_003", "gs_005")


def test_lift_skips_clusters_without_repair_kit() -> None:
    clusters = [
        _cluster(
            primary_cluster_id="cluster_h003",
            target_qids=("gs_030",),
            repair_kit=None,
            soft_evidence=[{"soft_qid": "gs_888"}],
        ),
    ]
    assert lift_soft_evidence_to_kit_lookup(clusters) == {}


def test_lift_dedupes_qids_across_clusters_into_same_kit() -> None:
    clusters = [
        _cluster(
            primary_cluster_id="c_a", target_qids=("gs_a",),
            repair_kit={"kit_id": "kit_shared"},
            soft_evidence=[{"soft_qid": "gs_001"}, {"soft_qid": "gs_002"}],
        ),
        _cluster(
            primary_cluster_id="c_b", target_qids=("gs_b",),
            repair_kit={"kit_id": "kit_shared"},
            soft_evidence=[{"soft_qid": "gs_002"}, {"soft_qid": "gs_003"}],
        ),
    ]
    out = lift_soft_evidence_to_kit_lookup(clusters)
    assert out["kit_shared"] == ("gs_001", "gs_002", "gs_003")
