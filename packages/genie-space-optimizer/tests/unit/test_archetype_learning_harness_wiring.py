"""Helper-level integration tests for Section E harness wiring.

The wiring lives in harness.py inside flag-gated try/except blocks. The
production code calls a small pure helper for each tier so the harness
body need not be patched in test."""

from __future__ import annotations

from genie_space_optimizer.optimization.archetype_learning_state import (
    get_state,
    reset_state,
)
from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def _card(cluster_id: str, root_cause: RcaKind = RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING) -> RCACard:
    return RCACard(
        card_id=f"c_{cluster_id}", cluster_id=cluster_id,
        qids=(f"gs_{cluster_id}",),
        root_cause=root_cause,
        grounding_terms=frozenset({"snack_brand"}),
        intended_patch_shape="entity_disambiguation",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="t",
    )


def test_tier1_helper_emits_one_record_per_unmatched_cluster() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        emit_unmatched_pattern_records_for_unmatched_clusters,
    )

    reset_state("run_W1")
    clusters = [
        {"cluster_id": "S001", "rca_card": _card("S001"),
         "asi_question_intent": "single", "question_ids": ["gs_001"]},
        {"cluster_id": "S002", "rca_card": _card("S002"),
         "asi_question_intent": "single", "question_ids": ["gs_002"]},
        # No card → not unmatched, just absent. Skipped.
        {"cluster_id": "X", "rca_card": None, "question_ids": ["gs_x"]},
    ]
    for c in clusters:
        c["_repair_kit"] = None

    out = emit_unmatched_pattern_records_for_unmatched_clusters(
        run_id="run_W1", clusters=clusters,
    )
    assert len(out) == 2
    assert {r.cluster_id for r in out} == {"S001", "S002"}
    assert len(get_state("run_W1").unmatched_pattern_records) == 2


def test_tier1_helper_skips_clusters_with_a_repair_kit() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        emit_unmatched_pattern_records_for_unmatched_clusters,
    )

    reset_state("run_W2")
    clusters = [
        {"cluster_id": "H001", "rca_card": _card("H001"),
         "asi_question_intent": "plural", "question_ids": ["gs_026"],
         "_repair_kit": {"repair_archetype": "plural_top_n_collapse"}},
    ]
    out = emit_unmatched_pattern_records_for_unmatched_clusters(
        run_id="run_W2", clusters=clusters,
    )
    assert out == []


def test_run_tiers_2_to_3_returns_empty_when_below_threshold(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ARCHETYPE_LEARNING", "1")
    monkeypatch.setenv("GSO_PATTERN_CANDIDATE_MEMBER_THRESHOLD", "5")  # raise above seed count
    from genie_space_optimizer.optimization.archetype_learning import (
        emit_unmatched_pattern_record,
        run_iteration_prelude_tiers_2_to_3,
    )
    from genie_space_optimizer.optimization.archetype_learning_state import reset_state

    reset_state("run_W3")
    for cid in ("C1", "C2"):
        emit_unmatched_pattern_record(
            run_id="run_W3", card=_card(cid),
            cluster={"cluster_id": cid, "asi_question_intent": "single",
                     "question_ids": [f"gs_{cid}"]},
        )
    new_provisionals, candidates = run_iteration_prelude_tiers_2_to_3(
        run_id="run_W3", iteration=2, w=None,
    )
    assert new_provisionals == ()
    assert candidates == ()


def test_run_tiers_2_to_3_emits_provisional_when_tiers_2_3_succeed(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ARCHETYPE_LEARNING", "1")
    monkeypatch.setenv("GSO_PROVISIONAL_SYNTHESIS_LLM", "1")
    monkeypatch.setenv("GSO_PATTERN_CANDIDATE_MEMBER_THRESHOLD", "3")
    from unittest.mock import patch

    from genie_space_optimizer.optimization import archetype_learning as al
    from genie_space_optimizer.optimization.archetype_learning_state import reset_state

    reset_state("run_W4")
    for cid in ("C1", "C2", "C3"):
        al.emit_unmatched_pattern_record(
            run_id="run_W4", card=_card(cid),
            cluster={"cluster_id": cid, "asi_question_intent": "single",
                     "question_ids": [f"gs_{cid}"]},
        )

    fake_payload = {
        "name": "x_provisional",
        "applicable_rca_kinds": ["SYNONYM_OR_ENTITY_MATCH_MISSING"],
        "required_grounding_tokens": ["snack_brand"],
        "evidence_predicates": [],
        "default_priority_step": "repair_kit",
        "expected_causal_effect_template": "x",
        "rationale": "x",
    }
    with patch.object(al, "_call_llm_for_provisional_archetype_synthesis",
                      return_value=fake_payload):
        new_provisionals, candidates = al.run_iteration_prelude_tiers_2_to_3(
            run_id="run_W4", iteration=2, w=None,
        )
    assert len(candidates) == 1
    assert len(new_provisionals) == 1
    assert new_provisionals[0].lifecycle_state == "provisional"
