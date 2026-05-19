"""Plan 1 Task 8 — ProposalSlate.repair_intents_by_id carrier.

When a proposal in the input slate carries a serialised
``repair_intent`` field (stamped at synthesis time, Task 7),
``generate()`` parses it back into a typed ``RepairIntent`` and adds
it to the typed carrier. Legacy proposals without the field flow
through unchanged; the carrier just omits them.
"""

from __future__ import annotations

from types import SimpleNamespace

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
    stamp_repair_intent_on_proposal,
)
from genie_space_optimizer.optimization.stages.proposals import (
    ProposalsInput,
    ProposalSlate,
    generate,
)


def _intent(intent_id: str = "i1") -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id,
        intent_name="top_n_by_metric",
        intent_description="d",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="r",
        confidence="medium",
        source="test",
        cluster_id="H001",
        target_qids=("gs_009",),
        blame_set=(),
        rca_card_id="rca",
        ag_id="AG_X",
    )


def _ctx():
    emitted: list = []
    return SimpleNamespace(
        run_id="run",
        iteration=1,
        decision_emit=lambda r: emitted.append(r),
        _emitted=emitted,
    )


def test_proposal_slate_has_repair_intents_carrier_field() -> None:
    """ProposalSlate must declare the typed carrier even when empty."""
    slate = ProposalSlate(proposals_by_ag={})
    assert hasattr(slate, "repair_intents_by_id")
    assert slate.repair_intents_by_id == {}


def test_generate_rolls_up_stamped_intents_into_carrier() -> None:
    proposal = {"proposal_id": "p1", "patch_type": "add_example_sql"}
    stamp_repair_intent_on_proposal(proposal, _intent("i1"))
    inp = ProposalsInput(proposals_by_ag={"AG_X": (proposal,)})
    out = generate(_ctx(), inp)
    assert "i1" in out.repair_intents_by_id
    assert out.repair_intents_by_id["i1"].intent_name == "top_n_by_metric"


def test_generate_skips_unstamped_proposals_silently() -> None:
    """A proposal without ``repair_intent`` is legacy / out-of-scope
    for Plan 1; it does not appear in the carrier (and does not
    raise)."""
    inp = ProposalsInput(
        proposals_by_ag={
            "AG_X": (
                {"proposal_id": "p_legacy", "patch_type": "add_example_sql"},
            ),
        }
    )
    out = generate(_ctx(), inp)
    assert out.repair_intents_by_id == {}


def test_generate_carrier_handles_mixed_stamped_and_legacy() -> None:
    p1 = {"proposal_id": "p1", "patch_type": "add_example_sql"}
    stamp_repair_intent_on_proposal(p1, _intent("i1"))
    p2 = {"proposal_id": "p2_legacy", "patch_type": "add_example_sql"}
    inp = ProposalsInput(proposals_by_ag={"AG_X": (p1, p2)})
    out = generate(_ctx(), inp)
    assert set(out.repair_intents_by_id) == {"i1"}


def test_proposal_slate_round_trip_preserves_carrier() -> None:
    intent = _intent("i1")
    slate = ProposalSlate(
        proposals_by_ag={},
        repair_intents_by_id={"i1": intent},
    )
    payload = slate.to_json()
    restored = ProposalSlate.from_json(payload)
    assert "i1" in restored.repair_intents_by_id
    assert restored.repair_intents_by_id["i1"] == intent


def test_empty_proposal_slate_to_json_includes_carrier_key() -> None:
    """Byte-stable extension: carrier key is always present (empty
    dict when no intents). Postmortem code can rely on the key
    existing."""
    slate = ProposalSlate(proposals_by_ag={})
    payload = slate.to_json()
    assert payload["repair_intents_by_id"] == {}
