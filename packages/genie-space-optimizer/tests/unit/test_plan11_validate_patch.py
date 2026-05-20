"""Plan 11 — validate_patch dispatcher: one happy-path test per PatchType arm,
plus error_kind tests and precheck tests.

Uses unittest.mock to stub out the heavyweight validators (applier,
benchmarks) so these tests run in milliseconds with no Spark/SQL deps.
"""
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.stages.validate_patch import validate_patch
from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
from genie_space_optimizer.optimization.repair_intent import PatchType, RepairShape


def _make_proposal(patch_type: str, patch_body: dict) -> RepairProposal:
    return RepairProposal(
        intent_id="intent_001",
        intent_name="test patch",
        intent_description="test",
        repair_shape=RepairShape.OTHER,
        patch_type=patch_type,
        rationale="test",
        confidence="high",
        patch_body=patch_body,
        blame_set=(),
        repair_hypothesis="test hypothesis",
        target_qids=("gs_009",),
    )


@pytest.fixture
def mock_context():
    return dict(
        config={},
        metadata_snapshot={"tables": [], "columns": [], "joins": []},
        spark=MagicMock(),
        w=MagicMock(),
        catalog="main",
        gold_schema="gso",
        warehouse_id="abc123",
    )


def test_patch_type_unknown_returns_error(mock_context):
    proposal = _make_proposal("not_a_real_patch_type", {})
    result = validate_patch(proposal, **mock_context)
    assert result.is_valid is False
    assert result.errors[0].error_kind == "patch_type_unknown"
    assert result.errors[0].failing_location == "patch_type"


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch._validate_example_sql_entry",
    return_value=(True, []),
)
@patch(
    "genie_space_optimizer.optimization.stages.validate_patch.validate_sql_snippet",
    return_value=(True, "SELECT 1"),
)
def test_add_example_sql_valid(mock_validate_sql, mock_validate_entry, mock_context):
    proposal = _make_proposal(
        PatchType.ADD_EXAMPLE_SQL,
        {"example_question": "Top 10 orders?", "example_sql": "SELECT * FROM orders LIMIT 10"},
    )
    result = validate_patch(proposal, **mock_context)
    assert result.is_valid is True
    assert result.errors == ()


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch._validate_example_sql_entry",
    return_value=(False, ["example_sql is required"]),
)
def test_add_example_sql_schema_error(mock_validate_entry, mock_context):
    proposal = _make_proposal(PatchType.ADD_EXAMPLE_SQL, {})
    result = validate_patch(proposal, **mock_context)
    assert result.is_valid is False
    assert result.errors[0].error_kind == "genie_schema"


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch.validate_instruction_text",
    return_value=(True, []),
)
def test_add_instruction_valid(mock_validate_instr, mock_context):
    proposal = _make_proposal(
        PatchType.ADD_INSTRUCTION,
        {"instruction_text": "## KPI Definitions\nRevenue = SUM(amount)\n"},
    )
    result = validate_patch(proposal, **mock_context)
    assert result.is_valid is True


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch.validate_instruction_text",
    return_value=(False, ["instruction_text failed 5-section schema"]),
)
def test_add_instruction_canonical_error(mock_validate_instr, mock_context):
    proposal = _make_proposal(
        PatchType.ADD_INSTRUCTION,
        {"instruction_text": "bad instructions"},
    )
    result = validate_patch(proposal, **mock_context)
    assert result.is_valid is False
    assert result.errors[0].error_kind == "instruction_canonical"


def test_trivial_toggle_always_valid(mock_context):
    for pt in (
        PatchType.ENABLE_EXAMPLE_VALUES,
        PatchType.DISABLE_EXAMPLE_VALUES,
        PatchType.ENABLE_VALUE_DICTIONARY,
        PatchType.DISABLE_VALUE_DICTIONARY,
    ):
        proposal = _make_proposal(pt, {})
        result = validate_patch(proposal, **mock_context)
        assert result.is_valid is True, f"Expected valid for {pt}"


def test_patch_body_missing_field_for_example_sql(mock_context):
    """patch_body with no example_sql field should trigger patch_body_missing_field."""
    proposal = _make_proposal(PatchType.ADD_EXAMPLE_SQL, {"example_question": "Q?"})
    # Don't mock _validate_example_sql_entry — let it return the real error
    with patch(
        "genie_space_optimizer.optimization.stages.validate_patch._validate_example_sql_entry",
        return_value=(False, ["example_sql is required"]),
    ):
        result = validate_patch(proposal, **mock_context)
    assert result.is_valid is False
