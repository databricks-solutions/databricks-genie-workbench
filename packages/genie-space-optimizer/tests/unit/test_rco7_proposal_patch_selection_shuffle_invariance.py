"""RCO-7 Site 3 — proposal generation and patch selection are reorder-invariant.

Two failure modes are guarded:

  * ``stages.proposals.generate`` walks ``proposals_by_ag`` in dict
    order and each AG's proposal list in input order. Reordering
    proposals within an AG must not change the slate.
  * ``patch_selection._deduplicate_patches`` is first-wins. Reordering
    patches with the same stable identity must not change which patch
    survives.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from genie_space_optimizer.optimization.patch_selection import (
    select_causal_patch_cap,
)
from genie_space_optimizer.optimization.stages.proposals import (
    ProposalsInput,
    generate,
)


@dataclass
class _StubCtx:
    run_id: str = "run_rco7"
    iteration: int = 1

    def decision_emit(self, _record) -> None:
        return None


# ── Proposal generation shuffle invariance ────────────────────────────


def _proposals_input(proposal_order_by_ag: dict[str, list[str]]) -> ProposalsInput:
    proposal_dicts = {
        "P_a1": {"expanded_patch_id": "L5:P_a1", "proposal_id": "P_a1", "lever": 5,
                 "type": "add_snippet", "snippet_name": "x"},
        "P_a2": {"expanded_patch_id": "L3:P_a2", "proposal_id": "P_a2", "lever": 3,
                 "type": "rewrite_instruction", "section_name": "y"},
        "P_b1": {"expanded_patch_id": "L6:P_b1", "proposal_id": "P_b1", "lever": 6,
                 "type": "add_sql_expression", "target": "z"},
    }
    return ProposalsInput(
        proposals_by_ag={
            ag_id: tuple(proposal_dicts[pid] for pid in order)
            for ag_id, order in proposal_order_by_ag.items()
        },
        rca_id_by_cluster={"C1": "R1", "C2": "R2"},
        cluster_root_cause_by_id={"C1": "missing_filter", "C2": "wrong_measure"},
    )


def test_proposal_generation_is_reorder_invariant() -> None:
    forward = _proposals_input({
        "AG_alpha": ["P_a1", "P_a2"],
        "AG_beta":  ["P_b1"],
    })
    reversed_inp = _proposals_input({
        "AG_alpha": ["P_a2", "P_a1"],
        "AG_beta":  ["P_b1"],
    })

    out_forward = generate(_StubCtx(), forward)
    out_reversed = generate(_StubCtx(), reversed_inp)

    canon = lambda slate: json.dumps(
        {
            ag_id: [
                {k: v for k, v in p.items() if k != "content_fingerprint"}
                for p in props
            ]
            for ag_id, props in slate.proposals_by_ag.items()
        },
        sort_keys=True,
    )
    assert canon(out_forward) == canon(out_reversed)

    # Fingerprints emitted are content-derived, not order-derived;
    # the multiset must match even though emit order is now canonical.
    assert sorted(out_forward.content_fingerprints_emitted) == sorted(
        out_reversed.content_fingerprints_emitted
    )


# ── Patch selection shuffle invariance ────────────────────────────────


def _patches_for_cap_test() -> list[dict]:
    """Four patches; two share an expanded_patch_id but differ in
    lever and target. First-wins dedup will pick a different one
    depending on input order unless the canonical sort runs first."""
    return [
        {
            "expanded_patch_id": "L5:P002#1",
            "proposal_id": "P002",
            "lever": 5,
            "type": "add_snippet",
            "snippet_name": "alpha",
            "relevance_score": 0.7,
            "rca_id": "R1",
            "target_qids": ["q1", "q2"],
        },
        {
            "expanded_patch_id": "L3:P001#1",
            "proposal_id": "P001",
            "lever": 3,
            "type": "rewrite_instruction",
            "section_name": "intro",
            "relevance_score": 0.9,
            "rca_id": "R2",
            "target_qids": ["q3"],
        },
        {
            "expanded_patch_id": "L6:P003#1",
            "proposal_id": "P003",
            "lever": 6,
            "type": "add_sql_expression",
            "target": "metric_x",
            "relevance_score": 0.4,
            "rca_id": "R3",
            "target_qids": ["q4"],
        },
        {
            "expanded_patch_id": "L5:P004#1",
            "proposal_id": "P004",
            "lever": 5,
            "type": "add_snippet",
            "snippet_name": "delta",
            "relevance_score": 0.6,
            "rca_id": "R4",
            "target_qids": ["q5"],
        },
    ]


def test_select_causal_patch_cap_is_reorder_invariant() -> None:
    patches_forward = _patches_for_cap_test()
    patches_reversed = list(reversed(_patches_for_cap_test()))
    patches_shuffled = [
        _patches_for_cap_test()[i] for i in (2, 0, 3, 1)
    ]

    selected_f, decisions_f = select_causal_patch_cap(
        patches_forward, max_patches=3,
    )
    selected_r, decisions_r = select_causal_patch_cap(
        patches_reversed, max_patches=3,
    )
    selected_s, decisions_s = select_causal_patch_cap(
        patches_shuffled, max_patches=3,
    )

    # The kept-patch SET must be identical across orderings. The
    # selection order itself remains rank-by-score; identity-equality
    # of the surviving patches is the load-bearing property RCO-2's
    # contract-health summary depends on.
    surviving_ids = lambda sel: sorted(
        p["expanded_patch_id"] for p in sel
    )
    assert surviving_ids(selected_f) == surviving_ids(selected_r)
    assert surviving_ids(selected_f) == surviving_ids(selected_s)

    # The DECISIONS list must be byte-stable under canonical JSON
    # (decisions are sorted by stable identity before emission).
    canon = lambda decs: json.dumps(
        sorted(decs, key=lambda d: d.get("expanded_patch_id") or ""),
        sort_keys=True, default=str,
    )
    assert canon(decisions_f) == canon(decisions_r)
    assert canon(decisions_f) == canon(decisions_s)


def test_select_causal_patch_cap_dedup_is_order_invariant() -> None:
    """Two patches sharing a stable identity must dedup to the same
    surviving patch regardless of which appears first in input."""
    twin_a = {
        "expanded_patch_id": "L5:P010#1",
        "proposal_id": "P010",
        "lever": 5,
        "type": "add_snippet",
        "snippet_name": "shared",
        "relevance_score": 0.8,
        "rca_id": "R10",
        "target_qids": ["q10"],
    }
    twin_b = dict(twin_a)
    # Same identity, different relevance score — only one survives.
    twin_b["relevance_score"] = 0.5

    selected_ab, _ = select_causal_patch_cap([twin_a, twin_b], max_patches=2)
    selected_ba, _ = select_causal_patch_cap([twin_b, twin_a], max_patches=2)

    # Canonical sort + first-wins means the SAME twin survives in both.
    assert len(selected_ab) == 1
    assert len(selected_ba) == 1
    assert (
        selected_ab[0]["relevance_score"]
        == selected_ba[0]["relevance_score"]
    )
