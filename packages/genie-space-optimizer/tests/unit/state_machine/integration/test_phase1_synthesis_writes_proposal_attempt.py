"""Stage 3 synthesis raises ContractFailure when RepairProposal is missing required fields.

The Phase 12 PatchOutcome contract specified these as required.
Phase 1 makes the contract enforced at Stage 3 *exit* so untyped paths
cannot leak into the L6 lane.
"""
import pytest

from genie_space_optimizer.optimization.stages.synthesize import (
    StageThreeContractError,
    validate_synthesis_output_for_state_machine,
)


def _proposal_dict(**overrides) -> dict:
    base = {
        "intent_id": "intent_xyz",
        "patch_type": "add_sql_snippet_expression",
        "target_objects": ["tkt_payment"],
        "target_qids": ["gs_009"],
        "rca_card_id": "rca_abc",
        "causal_target": "ROW_NUMBER",
        "original_patch_body": "ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)",
    }
    base.update(overrides)
    return base


def test_complete_proposal_validates():
    validate_synthesis_output_for_state_machine(_proposal_dict())  # should not raise


def test_missing_intent_id_raises():
    with pytest.raises(StageThreeContractError, match="intent_id"):
        validate_synthesis_output_for_state_machine(_proposal_dict(intent_id=""))


def test_missing_target_qids_raises():
    with pytest.raises(StageThreeContractError, match="target_qids"):
        validate_synthesis_output_for_state_machine(_proposal_dict(target_qids=[]))


def test_missing_rca_card_id_raises():
    with pytest.raises(StageThreeContractError, match="rca_card_id"):
        validate_synthesis_output_for_state_machine(_proposal_dict(rca_card_id=""))


def test_missing_original_patch_body_raises():
    with pytest.raises(StageThreeContractError, match="original_patch_body"):
        validate_synthesis_output_for_state_machine(_proposal_dict(original_patch_body=""))
