"""Replay test for Section E Tier 2 detection on the canonical
ccf1d60d S001 pattern.

The fixture seeds three unmatched-pattern records sharing the
SYNONYM_OR_ENTITY_MATCH_MISSING + snack_brand signature; Tier 2
detection must emit exactly one PatternCandidate."""

from __future__ import annotations

import json
from pathlib import Path


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "archetype_learning"
    / "ccf1d60d_S001_pattern_candidate.json"
)


def _load() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def test_ccf1d60d_S001_pattern_detection(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ARCHETYPE_LEARNING", "1")
    monkeypatch.setenv("GSO_PATTERN_CANDIDATE_MEMBER_THRESHOLD", "3")
    from genie_space_optimizer.optimization import archetype_learning as al
    from genie_space_optimizer.optimization.archetype_learning_state import (
        reset_state,
    )
    from genie_space_optimizer.optimization.rca import RCACard, RcaKind

    fixture = _load()
    run_id = fixture["run_id"]
    reset_state(run_id)

    for c in fixture["unmatched_clusters"]:
        rc = c["rca_card"]
        card = RCACard(
            card_id=rc["card_id"],
            cluster_id=rc["cluster_id"],
            qids=tuple(rc["qids"]),
            root_cause=RcaKind[rc["root_cause"]],
            grounding_terms=frozenset(rc["grounding_terms"]),
            intended_patch_shape=rc["intended_patch_shape"],
            allowed_patch_families=frozenset(rc["allowed_patch_families"]),
            forbidden_patch_families=frozenset(rc["forbidden_patch_families"]),
            rationale=rc["rationale"],
        )
        al.emit_unmatched_pattern_record(
            run_id=run_id, card=card,
            cluster={
                "cluster_id": c["cluster_id"],
                "asi_question_intent": c["asi_question_intent"],
                "question_ids": c["question_ids"],
            },
        )

    new_provisionals, candidates = al.run_iteration_prelude_tiers_2_to_3(
        run_id=run_id, iteration=2, w=None,
    )

    expected = fixture["expected_pattern_candidate"]
    assert len(candidates) == 1
    cand = candidates[0]
    assert cand.member_count == expected["member_count"]
    assert set(cand.member_cluster_ids) == set(expected["member_cluster_ids_set"])
    assert set(cand.union_qids) == set(expected["union_qids_set"])
    assert cand.root_cause_label == expected["root_cause_label"]
    assert cand.intended_patch_shape == expected["intended_patch_shape"]
    # No LLM mocked → Tier 3 returns nothing because the LLM sub-flag is OFF.
    assert new_provisionals == ()
