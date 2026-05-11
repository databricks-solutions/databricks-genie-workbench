"""RCO-7 Site 2 — ``stages.action_groups.select`` is reorder-invariant.

Two ``ActionGroupsInput`` instances with the same AG content in
different order must produce identical ``ActionGroupSlate`` outputs
(both ``ags`` and any populated ``admission_trace``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from genie_space_optimizer.optimization.stages.action_groups import (
    ActionGroupsInput,
    ForbiddenAG,
    ForbiddenReason,
    select,
)


@dataclass
class _StubCtx:
    """Minimal stage context. ``decision_emit`` swallows records so the
    test does not depend on harness-side decision-trace plumbing."""
    run_id: str = "run_rco7"
    iteration: int = 1

    def decision_emit(self, _record) -> None:
        return None


def _input(ag_order: list[str]) -> ActionGroupsInput:
    ag_dicts = {
        "AG_alpha": {
            "id": "AG_alpha",
            "affected_questions": ["q1"],
            "lever_directives": {"5": {"target_qids": ["q1"]}},
            "source_cluster_ids": ["C1"],
        },
        "AG_beta": {
            "id": "AG_beta",
            "affected_questions": ["q2"],
            "lever_directives": {"3": {"target_qids": ["q2"]}},
            "source_cluster_ids": ["C2"],
        },
        "AG_gamma": {
            "id": "AG_gamma",
            "affected_questions": ["q3"],
            "lever_directives": {"6": {"target_qids": ["q3"]}},
            "source_cluster_ids": ["C3"],
        },
    }
    return ActionGroupsInput(
        action_groups=tuple(ag_dicts[i] for i in ag_order),
        source_clusters_by_id={
            "C1": {"cluster_id": "C1"},
            "C2": {"cluster_id": "C2"},
            "C3": {"cluster_id": "C3"},
        },
        rca_id_by_cluster={"C1": "R1", "C2": "R2", "C3": "R3"},
    )


def test_select_action_groups_are_reorder_invariant() -> None:
    forward = _input(["AG_alpha", "AG_beta", "AG_gamma"])
    reversed_inp = _input(["AG_gamma", "AG_beta", "AG_alpha"])
    shuffled = _input(["AG_beta", "AG_gamma", "AG_alpha"])

    out_forward = select(_StubCtx(), forward)
    out_reversed = select(_StubCtx(), reversed_inp)
    out_shuffled = select(_StubCtx(), shuffled)

    # Compare canonicalized JSON of the slate's ags tuple.
    canon = lambda slate: json.dumps(
        [dict(a) for a in slate.ags], sort_keys=True, default=str,
    )
    assert canon(out_forward) == canon(out_reversed)
    assert canon(out_forward) == canon(out_shuffled)

    assert [a["id"] for a in out_forward.ags] == [
        "AG_alpha", "AG_beta", "AG_gamma",
    ]


def test_select_forbidden_ags_are_reorder_invariant() -> None:
    """The forbidden-AG input list is part of the LLM-derived ingestion
    boundary (forbidden AGs are computed from prior-iteration LLM
    rollback decisions). Shuffling the forbidden tuple must not change
    the admission-trace output when the chunk_b flag is on."""
    import os
    os.environ["GSO_STAGE_HANDLERS_CHUNK_B"] = "1"
    try:
        forbidden_forward = (
            ForbiddenAG(ag_id="AG_beta", reason=ForbiddenReason.NO_PROPOSALS),
            ForbiddenAG(ag_id="AG_gamma", reason=ForbiddenReason.AG_RETIRED),
        )
        forbidden_reversed = tuple(reversed(forbidden_forward))

        inp_forward = ActionGroupsInput(
            action_groups=_input(["AG_alpha", "AG_beta", "AG_gamma"]).action_groups,
            forbidden_ags=forbidden_forward,
        )
        inp_reversed = ActionGroupsInput(
            action_groups=_input(["AG_alpha", "AG_beta", "AG_gamma"]).action_groups,
            forbidden_ags=forbidden_reversed,
        )

        out_forward = select(_StubCtx(), inp_forward)
        out_reversed = select(_StubCtx(), inp_reversed)

        canon_trace = lambda slate: json.dumps(
            [
                {"ag_id": t.ag_id, "verdict": str(t.verdict),
                 "denial_reason": t.denial_reason}
                for t in slate.admission_trace
            ],
            sort_keys=True,
        )
        assert canon_trace(out_forward) == canon_trace(out_reversed)
    finally:
        os.environ.pop("GSO_STAGE_HANDLERS_CHUNK_B", None)
