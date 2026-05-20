"""Plan 9 Task 3 — RepairProposal.required_constructs field.

The LLM emits the contract its own SQL must satisfy. Replaces
archetype.output_shape["requires_constructs"]. Constructs are
case-sensitive SQL clause keywords like 'SELECT', 'GROUP_BY',
'ORDER_BY', 'LIMIT', 'JOIN', 'WHERE', 'WINDOW'.
"""
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def test_required_constructs_defaults_empty_tuple():
    proposal = RepairProposal(
        intent_id="i_001",
        intent_name="x",
        intent_description="...",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_INSTRUCTION,
        rationale="...",
        confidence="medium",
        patch_body={"instruction_text": "Do X."},
        blame_set=(),
    )
    assert proposal.required_constructs == ()


def test_required_constructs_round_trips_via_json():
    proposal = RepairProposal(
        intent_id="i_002",
        intent_name="top_n",
        intent_description="...",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="...",
        confidence="high",
        patch_body={
            "example_question": "?",
            "example_sql": "SELECT 1",
        },
        blame_set=("a",),
        required_constructs=("SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"),
    )
    payload = proposal.to_json()
    assert payload["required_constructs"] == [
        "SELECT", "GROUP_BY", "ORDER_BY", "LIMIT",
    ]
    reconstructed = RepairProposal.from_json(payload)
    assert reconstructed.required_constructs == proposal.required_constructs
