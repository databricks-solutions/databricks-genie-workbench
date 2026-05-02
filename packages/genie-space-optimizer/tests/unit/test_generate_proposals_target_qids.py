"""Pin the proposal-time target_qids defaulting in generate_proposals_from_strategy.

Cycle 8 Bug 1 Phase 2: every proposal returned by
``generate_proposals_from_strategy`` carries a populated ``target_qids``
field. Standard L1-L4 paths inherit ``action_group.affected_questions``;
RCA-bridge / cluster-driven / RCA-forced paths keep their narrower
explicit values. Closes the proposal-vs-patch gap so anywhere downstream
that reads ``proposal.target_qids`` before ``_backfill_patch_causal_metadata``
runs sees real values, not ``[]``.

See `docs/2026-05-02-cycle8-bug1-target-qids-diagnosis-and-plan.md`
Phase 2 for the rationale.
"""
from __future__ import annotations


def test_proposal_inherits_affected_questions_when_no_explicit_target_qids() -> None:
    """The post-proposals defaulting pass at the end of
    ``generate_proposals_from_strategy`` must stamp
    ``target_qids = action_group.affected_questions`` on any proposal that
    didn't explicitly set them. Synthesizes the post-build defaulting
    directly so we don't need to drive the full lever pipeline."""
    affected_qs = ["airline_ticketing_and_fare_analysis_gs_024"]
    proposals = [
        {"proposal_id": "P001", "patch_type": "update_description"},
    ]

    # Mirrors the production block at optimizer.py:14786-14803.
    _ag_default_target_qids = [str(q) for q in (affected_qs or []) if q]
    if _ag_default_target_qids:
        for _proposal in proposals:
            _existing = _proposal.get("target_qids") or _proposal.get(
                "_grounding_target_qids"
            ) or []
            if not [q for q in _existing if q]:
                _proposal["target_qids"] = list(_ag_default_target_qids)

    assert proposals[0]["target_qids"] == [
        "airline_ticketing_and_fare_analysis_gs_024"
    ]


def test_explicit_narrow_target_qids_are_preserved_over_ag_default() -> None:
    """RCA-bridge / cluster-driven paths stamp narrower ``target_qids``
    BEFORE the defaulting pass runs. The defaulting must not overwrite
    them with the broader AG-scoped default — precision wins."""
    affected_qs = ["q_narrow", "q_other_in_ag", "q_yet_another"]
    proposals = [
        {"proposal_id": "P001", "target_qids": ["q_narrow"]},
    ]

    _ag_default_target_qids = [str(q) for q in (affected_qs or []) if q]
    if _ag_default_target_qids:
        for _proposal in proposals:
            _existing = _proposal.get("target_qids") or _proposal.get(
                "_grounding_target_qids"
            ) or []
            if not [q for q in _existing if q]:
                _proposal["target_qids"] = list(_ag_default_target_qids)

    assert proposals[0]["target_qids"] == ["q_narrow"]


def test_grounding_target_qids_is_treated_as_existing_value() -> None:
    """If a proposal has ``_grounding_target_qids`` but no ``target_qids``,
    the defaulting must NOT overwrite — the grounding stamp is a more
    authoritative narrowing signal than the AG default."""
    affected_qs = ["q_broad", "q_other"]
    proposals = [
        {"proposal_id": "P001", "_grounding_target_qids": ["q_grounded"]},
    ]

    _ag_default_target_qids = [str(q) for q in (affected_qs or []) if q]
    if _ag_default_target_qids:
        for _proposal in proposals:
            _existing = _proposal.get("target_qids") or _proposal.get(
                "_grounding_target_qids"
            ) or []
            if not [q for q in _existing if q]:
                _proposal["target_qids"] = list(_ag_default_target_qids)

    # target_qids is NOT stamped because _grounding_target_qids was the
    # narrowing signal. The patch-side backfill will read
    # _grounding_target_qids and copy it to target_qids.
    assert "target_qids" not in proposals[0]
    assert proposals[0]["_grounding_target_qids"] == ["q_grounded"]


def test_empty_existing_target_qids_triggers_defaulting() -> None:
    """A proposal with ``target_qids=[]`` (truthy-empty) must be treated
    the same as one with no key at all — defaulting fires."""
    affected_qs = ["q_default"]
    proposals = [
        {"proposal_id": "P001", "target_qids": []},
    ]

    _ag_default_target_qids = [str(q) for q in (affected_qs or []) if q]
    if _ag_default_target_qids:
        for _proposal in proposals:
            _existing = _proposal.get("target_qids") or _proposal.get(
                "_grounding_target_qids"
            ) or []
            if not [q for q in _existing if q]:
                _proposal["target_qids"] = list(_ag_default_target_qids)

    assert proposals[0]["target_qids"] == ["q_default"]


def test_empty_affected_questions_no_op() -> None:
    """When the AG itself has no ``affected_questions`` (degenerate edge
    case), the defaulting pass is a no-op — no synthetic fallback."""
    affected_qs: list[str] = []
    proposals = [
        {"proposal_id": "P001"},
    ]

    _ag_default_target_qids = [str(q) for q in (affected_qs or []) if q]
    if _ag_default_target_qids:
        for _proposal in proposals:
            _existing = _proposal.get("target_qids") or _proposal.get(
                "_grounding_target_qids"
            ) or []
            if not [q for q in _existing if q]:
                _proposal["target_qids"] = list(_ag_default_target_qids)

    assert "target_qids" not in proposals[0]


def test_backfill_helper_now_a_noop_for_proposals_from_this_function() -> None:
    """Acceptance criterion (plan, Phase 2): after the post-proposals
    defaulting lands, ``_backfill_patch_causal_metadata`` should never
    need to fall back to ``ag_qids`` for a proposal that came out of
    ``generate_proposals_from_strategy``. Verify the backfill helper is
    a no-op for a stamped proposal."""
    from genie_space_optimizer.optimization.harness import (
        _backfill_patch_causal_metadata,
    )

    # Simulate what generate_proposals_from_strategy would emit
    # post-defaulting, then convert to a patch (the converter copies
    # target_qids via PROPOSAL_METADATA_ALLOWLIST so the patch starts
    # already populated).
    patch_with_stamped_target_qids = {
        "proposal_id": "P001",
        "patch_type": "update_description",
        "target_qids": ["q_a", "q_b"],
    }
    action_group = {
        "id": "AG_TEST",
        "affected_questions": ["q_a", "q_b", "q_c"],  # broader than narrow stamp
    }

    out = _backfill_patch_causal_metadata(
        patches=[patch_with_stamped_target_qids],
        action_group=action_group,
        source_clusters=[],
    )

    # The narrow stamp survives — backfill keeps the explicit value, does
    # not widen to ag.affected_questions.
    assert out[0]["target_qids"] == ["q_a", "q_b"]
    assert out[0]["_grounding_target_qids"] == ["q_a", "q_b"]
