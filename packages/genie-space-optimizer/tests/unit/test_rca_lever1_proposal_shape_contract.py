"""Pin strict producer-side shape contract for RCA Lever-1 proposals."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.proposal_shape import (
    ProposalShapeError,
    validate_column_proposal_shape,
)


def test_validate_accepts_scalar_table_column() -> None:
    validate_column_proposal_shape({
        "proposal_id": "P001",
        "patch_type": "update_column_description",
        "table": "cat.sch.mv_esr_dim_location",
        "column": "zone_vp_name",
    })


@pytest.mark.parametrize(
    "proposal, message",
    [
        (
            {
                "proposal_id": "P002",
                "patch_type": "update_column_description",
                "table": "cat.sch.mv",
                "column": [],
            },
            "column",
        ),
        (
            {
                "proposal_id": "P003",
                "patch_type": "update_column_description",
                "table": "",
                "column": "zone_vp_name",
            },
            "table",
        ),
        (
            {
                "proposal_id": "P004",
                "patch_type": "update_column_description",
                "table": "cat.sch.mv",
                "column": "[zone_vp_name, cy_sales]",
            },
            "list-shaped",
        ),
    ],
)
def test_validate_rejects_malformed_column_proposals(proposal, message) -> None:
    with pytest.raises(ProposalShapeError, match=message):
        validate_column_proposal_shape(proposal)


def test_validate_ignores_non_column_proposals() -> None:
    validate_column_proposal_shape({
        "proposal_id": "P005",
        "patch_type": "add_instruction",
        "target": "QUERY PATTERNS",
    })
