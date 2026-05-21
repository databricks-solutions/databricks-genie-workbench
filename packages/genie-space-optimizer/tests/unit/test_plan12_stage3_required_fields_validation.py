"""Plan 12 — every RepairProposal exiting Stage 3 must satisfy the
survival contract or be marked CONTRACT_FAILED."""
import pytest


def _make_proposal(**overrides):
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    from genie_space_optimizer.optimization.target_object_typed import (
        AssetKind,
        TargetObject,
    )

    defaults = dict(
        intent_id="intent_001",
        intent_name="Add MTD filter",
        intent_description="Filter to month-to-date",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_SQL_SNIPPET_FILTER,
        rationale="Judge identified trailing-30 misuse",
        confidence="high",
        patch_body={"name": "mtd_filter", "sql_expression": "..."},
        blame_set=("catalog.schema.orders.order_date",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="catalog.schema.orders",
                columns=("order_date",),
            ),
        ),
        target_qids=("gs_021",),
        repair_hypothesis="Replace trailing-30 with MTD",
    )
    defaults.update(overrides)
    return RepairProposal(**defaults)


def test_valid_proposal_passes_survival_contract():
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )
    p = _make_proposal()
    result = validate_survival_contract(p)
    assert result.is_valid
    assert result.missing_fields == ()


def test_missing_target_objects_fails():
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )
    p = _make_proposal(target_objects=())
    result = validate_survival_contract(p)
    assert not result.is_valid
    assert "target_objects" in result.missing_fields


def test_missing_blame_set_fails():
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )
    p = _make_proposal(blame_set=())
    result = validate_survival_contract(p)
    assert not result.is_valid
    assert "blame_set" in result.missing_fields


def test_missing_target_qids_fails():
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )
    p = _make_proposal(target_qids=())
    result = validate_survival_contract(p)
    assert not result.is_valid
    assert "target_qids" in result.missing_fields


def test_missing_intent_id_fails():
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )
    p = _make_proposal(intent_id="")
    result = validate_survival_contract(p)
    assert not result.is_valid
    assert "intent_id" in result.missing_fields
