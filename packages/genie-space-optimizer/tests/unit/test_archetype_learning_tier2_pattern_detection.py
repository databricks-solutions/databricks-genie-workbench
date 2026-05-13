"""Tests for Section E Tier 2 pattern-candidate detection."""

from __future__ import annotations

from genie_space_optimizer.optimization.archetype_learning import (
    UnmatchedPatternRecord,
)


def _rec(*, sig: str, cluster_id: str, qids: tuple[str, ...] = ("gs_x",)) -> UnmatchedPatternRecord:
    return UnmatchedPatternRecord(
        signature_hash=sig,
        cluster_id=cluster_id,
        root_cause_label="SYNONYM_OR_ENTITY_MATCH_MISSING",
        grounding_terms=frozenset({"snack_brand", "beverage_brand"}),
        intended_patch_shape="entity_disambiguation",
        asi_question_intent="single",
        qids=qids,
    )


def test_detect_pattern_candidates_returns_empty_below_threshold(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATTERN_CANDIDATE_K", "3")
    from genie_space_optimizer.optimization.archetype_learning import (
        detect_pattern_candidates,
    )
    records = [
        _rec(sig="sigA", cluster_id="C1"),
        _rec(sig="sigA", cluster_id="C2"),
    ]
    assert detect_pattern_candidates(records=records) == ()


def test_detect_pattern_candidates_emits_one_per_signature_at_threshold(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATTERN_CANDIDATE_K", "3")
    from genie_space_optimizer.optimization.archetype_learning import (
        detect_pattern_candidates,
    )
    records = [
        _rec(sig="sigA", cluster_id="C1", qids=("gs_1",)),
        _rec(sig="sigA", cluster_id="C2", qids=("gs_2",)),
        _rec(sig="sigA", cluster_id="C3", qids=("gs_3",)),
        _rec(sig="sigB", cluster_id="C4", qids=("gs_4",)),
    ]
    candidates = detect_pattern_candidates(records=records)
    assert len(candidates) == 1
    assert candidates[0].signature_hash == "sigA"
    assert candidates[0].member_count == 3
    assert set(candidates[0].member_cluster_ids) == {"C1", "C2", "C3"}
    assert set(candidates[0].union_qids) == {"gs_1", "gs_2", "gs_3"}


def test_detect_pattern_candidates_orders_by_member_count_desc_then_signature(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATTERN_CANDIDATE_K", "3")
    from genie_space_optimizer.optimization.archetype_learning import (
        detect_pattern_candidates,
    )
    records = [
        _rec(sig="sigA", cluster_id="A1"),
        _rec(sig="sigA", cluster_id="A2"),
        _rec(sig="sigA", cluster_id="A3"),
        _rec(sig="sigB", cluster_id="B1"),
        _rec(sig="sigB", cluster_id="B2"),
        _rec(sig="sigB", cluster_id="B3"),
        _rec(sig="sigB", cluster_id="B4"),
    ]
    candidates = detect_pattern_candidates(records=records)
    assert [c.signature_hash for c in candidates] == ["sigB", "sigA"]


def test_detect_pattern_candidates_excludes_already_provisioned_signatures(monkeypatch) -> None:
    """Signatures already covered by a confirmed-or-trialled provisional
    archetype must not re-enter the detection pipeline."""
    monkeypatch.setenv("GSO_PATTERN_CANDIDATE_K", "3")
    from genie_space_optimizer.optimization.archetype_learning import (
        detect_pattern_candidates,
    )
    records = [
        _rec(sig="sigA", cluster_id="C1"),
        _rec(sig="sigA", cluster_id="C2"),
        _rec(sig="sigA", cluster_id="C3"),
    ]
    candidates = detect_pattern_candidates(
        records=records, exclude_signature_hashes=frozenset({"sigA"}),
    )
    assert candidates == ()
