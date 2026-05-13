"""Phase 1 Addendum — tests for SoftEvidenceMatch + supporting_soft_evidence."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import (
    RCACard,
    RcaKind,
    SoftEvidenceMatch,
)


def test_soft_evidence_match_dataclass_fields() -> None:
    match = SoftEvidenceMatch(
        soft_qid="gs_001",
        soft_cluster_id="S001",
        match_kind="matching_counterfactual",
        evidence_token="time_window",
        soft_counterfactual="Add filter on time_window = mtd",
    )
    assert match.soft_qid == "gs_001"
    assert match.match_kind == "matching_counterfactual"


def test_rca_card_carries_supporting_soft_evidence_default_empty() -> None:
    """Phase 1 Addendum — supporting_soft_evidence defaults to ``()``
    so existing card construction (Phase 1 baseline) is byte-stable."""
    card = RCACard(
        card_id="card_x", cluster_id="cluster_h",
        qids=("gs_021",),
        root_cause=RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
        grounding_terms=frozenset({"time_window"}),
        intended_patch_shape="apply_default_time_window_filter",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="...",
    )
    assert card.supporting_soft_evidence == ()


def test_rca_card_supporting_soft_evidence_populated() -> None:
    soft_match = SoftEvidenceMatch(
        soft_qid="gs_001",
        soft_cluster_id="S001",
        match_kind="matching_counterfactual",
        evidence_token="time_window",
        soft_counterfactual="Add filter on time_window",
    )
    card = RCACard(
        card_id="card_h002", cluster_id="cluster_h002",
        qids=("gs_021",),
        root_cause=RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
        grounding_terms=frozenset({"time_window"}),
        intended_patch_shape="apply_default_time_window_filter",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="...",
        supporting_soft_evidence=(soft_match,),
    )
    assert len(card.supporting_soft_evidence) == 1
    assert card.supporting_soft_evidence[0].soft_qid == "gs_001"
