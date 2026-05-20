"""Plan 9 Task 3 — synthesis.py validator reads required_constructs
from RepairProposal instead of archetype.output_shape.

When a proposal carries non-empty required_constructs, the
output_shape gate must check the generated SQL against the
proposal's contract, not the archetype's. When the proposal carries
empty required_constructs (legacy / instruction path), the gate
falls back to the archetype's contract for backward compatibility
(deleted by T10).
"""
from types import SimpleNamespace

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.synthesis import (
    check_output_shape,
)


def _make_archetype_with_constructs(*constructs):
    """Helper — simulate a minimal Archetype carrying output_shape."""
    return SimpleNamespace(
        output_shape={"requires_constructs": list(constructs)},
    )


def _make_proposal(required_constructs=()):
    return RepairProposal(
        intent_id="i_001",
        intent_name="x",
        intent_description="...",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="...",
        confidence="high",
        patch_body={
            "example_question": "?",
            "example_sql": (
                "SELECT product, SUM(amount) FROM orders "
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
            ),
        },
        blame_set=(),
        required_constructs=required_constructs,
    )


def test_validator_uses_proposal_required_constructs_when_present():
    proposal = _make_proposal(
        required_constructs=("SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"),
    )
    archetype = _make_archetype_with_constructs("WINDOW")  # wrong shape
    sql_dict = {"example_sql": proposal.patch_body["example_sql"]}

    result = check_output_shape(
        sql_dict, archetype=archetype, proposal=proposal,
    )
    assert result.passed is True


def test_validator_rejects_when_proposal_constructs_missing_from_sql():
    proposal = _make_proposal(
        # WINDOW not in SQL — gate must reject.
        required_constructs=("SELECT", "GROUP_BY", "WINDOW"),
    )
    archetype = _make_archetype_with_constructs("SELECT")
    sql_dict = {"example_sql": "SELECT 1 FROM orders"}

    result = check_output_shape(
        sql_dict, archetype=archetype, proposal=proposal,
    )
    assert result.passed is False


def test_validator_falls_back_to_archetype_when_proposal_constructs_empty():
    """Pre-T10 backward compat. Once T10 deletes the catalog,
    archetype is None and this branch is gone."""
    proposal = _make_proposal(required_constructs=())  # empty — fallback
    archetype = _make_archetype_with_constructs("SELECT", "LIMIT")
    sql_dict = {"example_sql": "SELECT 1 FROM orders LIMIT 1"}

    result = check_output_shape(
        sql_dict, archetype=archetype, proposal=proposal,
    )
    assert result.passed is True
