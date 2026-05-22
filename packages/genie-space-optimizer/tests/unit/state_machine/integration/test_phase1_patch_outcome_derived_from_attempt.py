"""GSO_PATCH_OUTCOME_V1 marker is derived from ProposalAttempt at terminal transitions."""
import json

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    patch_outcome_marker_from_attempt,
)
from genie_space_optimizer.optimization.state_machine.records import ProposalAttempt


def _parse(line: str) -> tuple[str, dict]:
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_applied_attempt_emits_outcome_applied():
    pa = ProposalAttempt(
        attempt_index=0,
        intent_id="intent_xyz",
        patch_type="add_sql_snippet_expression",
        deepest_stage_in_attempt=FunnelStage.APPLIED,
        outcome="applied",
        outcome_reason="applied",
        patch_outcome_id="po_1",
    )
    name, payload = _parse(patch_outcome_marker_from_attempt(
        run_id="r", iteration=1, qid="gs_009", attempt=pa,
    ))
    assert name == "GSO_PATCH_OUTCOME_V1"
    assert payload["intent_id"] == "intent_xyz"
    assert payload["outcome"] == "applied"


def test_blast_radius_rejected_attempt_emits_typed_outcome():
    pa = ProposalAttempt(
        attempt_index=0,
        intent_id="intent_xyz",
        patch_type="add_sql_snippet_filter",
        deepest_stage_in_attempt=FunnelStage.APPLYABLE,
        outcome="blast_radius_rejected",
        outcome_reason="high_collateral_risk_flagged",
    )
    name, payload = _parse(patch_outcome_marker_from_attempt(
        run_id="r", iteration=1, qid="gs_024", attempt=pa,
    ))
    assert name == "GSO_PATCH_OUTCOME_V1"
    assert payload["outcome"] == "blast_radius_rejected"
    assert payload["outcome_reason"] == "high_collateral_risk_flagged"
