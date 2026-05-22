"""Phase 2 gs_009 anchor: full state machine drives APPLIED end-to-end."""
import json
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.optimizer import (
    run_state_machine_iteration_and_persist,
)
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage


FIXTURE = Path(__file__).parent / "fixtures" / "gs_009_full_trajectory.json"


@dataclass
class _StubResponse:
    succeeded: bool = True
    parsed_output: object = None
    declined: object = None


def _load():
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def stub_all_llm_and_side_effects():
    payload = _load()

    @dataclass
    class _Diag:
        rca_kind_label: str
        evidence_summary: str
        observed_failure: str
        expected_sql_shape: str
        confidence: str
        rca_card_id: str

    @dataclass
    class _ClusterMember:
        qid: str
        cluster_id: str
        ag_id: str
        co_member_qids: tuple
        routing_evidence_kind: str

    @dataclass
    class _ClusterParsed:
        members: tuple

    @dataclass
    class _RepairProposal:
        intent_id: str
        patch_type: str
        target_objects: tuple
        target_qids: tuple
        rca_card_id: str
        causal_target: str
        original_patch_body: str

    stage1 = _StubResponse(parsed_output=_Diag(**payload["stage1_diagnosis"]))
    member = payload["stage2_cluster_member"]
    stage2 = _StubResponse(parsed_output=_ClusterParsed(members=(
        _ClusterMember(member["qid"], member["cluster_id"], member["ag_id"],
                       tuple(member["co_member_qids"]), member["routing_evidence_kind"]),
    )))
    stage3 = _RepairProposal(
        intent_id=payload["stage3_proposal"]["intent_id"],
        patch_type=payload["stage3_proposal"]["patch_type"],
        target_objects=tuple(payload["stage3_proposal"]["target_objects"]),
        target_qids=tuple(payload["stage3_proposal"]["target_qids"]),
        rca_card_id=payload["stage3_proposal"]["rca_card_id"],
        causal_target=payload["stage3_proposal"]["causal_target"],
        original_patch_body=payload["stage3_proposal"]["original_patch_body"],
    )
    applier = payload["applier_result"]

    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm._invoke_stage1_llm",
        return_value=stage1,
    ), patch(
        "genie_space_optimizer.optimization.state_machine.transformers.cluster_batch._invoke_stage2_llm",
        return_value=stage2,
    ), patch(
        "genie_space_optimizer.optimization.state_machine.transformers.synthesize_llm._invoke_stage3_llm",
        return_value=stage3,
    ), patch(
        "genie_space_optimizer.optimization.state_machine.transformers.structural_repair_gate."
        "_proposal_passes_structural_check",
        return_value=(True, ""),
    ), patch(
        "genie_space_optimizer.optimization.state_machine.transformers.blast_radius_batch._assess_blast_radius",
        return_value=("safe", None),
    ), patch(
        "genie_space_optimizer.optimization.state_machine.transformers.applier_gate._apply_via_genie_api",
        return_value=(applier["apply_call_id"], applier["succeeded"], ""),
    ), patch(
        # Phase 3 PR 3.3: evaluated_gate runs at APPLIED.
        "genie_space_optimizer.optimization.state_machine.transformers.evaluated_gate._run_post_apply_eval",
        return_value=(1.0, "SELECT_POST", "row_post_gs_009"),
    ), patch(
        # Phase 3 PR 3.3: acceptance_gate runs at EVALUATED.
        "genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate._assess_collateral",
        return_value=(),  # no collateral regressions
    ):
        yield


def test_gs_009_reaches_applied_end_to_end(tmp_path: Path, stub_all_llm_and_side_effects):
    """gs_009 reaches APPLIED (or further) via the production registry.

    With Phase 2 wiring the test asserted exactly APPLIED. After Phase 3
    PR 3.3 added evaluated_gate at APPLIED and acceptance_gate at
    EVALUATED, the registry drives the state further: APPLIED → EVALUATED
    → ACCEPTED (or TERMINATED with OPTIMIZER_TRIED_NO_GAIN on
    regression). The stubs here send the eval through to ACCEPTED, so
    the assertion is "deepest reached APPLIED or later" and the
    applied record is still present along the way.
    """
    payload = _load()
    final = run_state_machine_iteration_and_persist(
        eval_rows=[payload["eval_row"]],
        iteration=1,
        run_id="phase2_smoke",
        run_root=tmp_path,
    )
    assert len(final) == 1
    s = final[0]
    assert s.qid == "gs_009"
    # Phase 2 contract: state machine MUST reach APPLIED at minimum.
    # Phase 3 additions drive further; we accept any post-APPLIED state.
    from genie_space_optimizer.optimization.state_machine.funnel import stage_index
    assert stage_index(s.deepest_stage_reached) >= stage_index(FunnelStage.APPLIED), (
        f"deepest_reached={s.deepest_stage_reached.value}"
    )
    assert s.applied is not None
    assert s.applied.apply_call_id == "apply_gs_009_abc"


def test_gs_009_persisted_trajectory_records_full_progression(tmp_path: Path, stub_all_llm_and_side_effects):
    payload = _load()
    run_state_machine_iteration_and_persist(
        eval_rows=[payload["eval_row"]],
        iteration=1,
        run_id="phase2_smoke",
        run_root=tmp_path,
    )
    qstate_file = tmp_path / "iteration_1" / "qstate_gs_009.json"
    persisted = json.loads(qstate_file.read_text())
    stage_sequence = [t["to_stage"] for t in persisted["transitions"]]
    # Must traverse the full Phase 2 ladder.
    for required in ("diagnosed", "clustered", "proposed", "normalized", "applyable", "applied"):
        assert required in stage_sequence, (
            f"missing stage transition: {required}; sequence={stage_sequence}"
        )
