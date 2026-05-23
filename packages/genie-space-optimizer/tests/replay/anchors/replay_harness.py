"""Replay harness for anchor QID trajectories.

Drives the Phase 1 state machine deterministically using fixture-mocked
LLM responses. Builds the per-QID trajectory and returns it for
assertion by the per-anchor test files.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.trajectory import (
    QuestionTrajectory,
    build_trajectory,
)
from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
    build_initial_states_from_eval_rows,
)
# SM Cutover Phase 3 — routing_gate quarantined to ``_legacy/``. This
# replay harness is itself a legacy fixture used by the pre-cutover
# tests; the post-cutover state machine has no in-SM routing step.
from genie_space_optimizer.optimization._legacy.state_machine.transformers.routing_gate import (  # noqa: E501
    routing_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


@dataclass(frozen=True)
class AnchorReplayResult:
    trajectory: QuestionTrajectory
    proposal_attempts_count: int
    deepest_reached: FunnelStage


def run_anchor_replay(fixture_path: Path) -> AnchorReplayResult:
    payload = json.loads(fixture_path.read_text())
    qid = payload["qid"]
    eval_rows = payload["eval_rows"]
    mocked_diag = payload["mocked_diagnosis"]
    mocked_prop = payload["mocked_proposal"]

    # 1. Dispatch input gate: build initial state from raw eval row.
    states = build_initial_states_from_eval_rows(eval_rows, iteration=1)
    assert len(states) == 1, f"expected 1 hard QID for {qid}, got {len(states)}"
    s = states[0]

    # 2. Stub Stage 1 diagnosis: advance to DIAGNOSED with mocked record.
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "plan11_stage1", "llm"),
        diagnosed=DiagnosisRecord(
            source="plan11_stage1",
            rca_kind_label=mocked_diag["rca_kind_label"],
            evidence_summary=mocked_diag["evidence_summary"],
            observed_failure=mocked_diag["observed_failure"],
            expected_sql_shape=mocked_diag["expected_sql_shape"],
            confidence=mocked_diag["confidence"],
            rca_card_id=mocked_diag["rca_card_id"],
        ),
    )

    # 3. Stub Stage 2 clustering: advance to CLUSTERED with single-member cluster.
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "plan11_stage2", "batch"),
        clustered=ClusterMembershipRecord(
            cluster_id=f"H_{qid}",
            ag_id=f"AG_{qid}",
            co_member_qids=(qid,),
            effective_target_lever=0,
            routing_evidence_kind="",
        ),
    )

    # 4. Routing gate (decoration at CLUSTERED) — writes effective_target_lever.
    ctx = TransformerContext(
        iteration=1, run_id="replay_" + qid,
        validation_context=ValidationContext(1, "replay_" + qid, {}),
    )
    s = routing_gate.transform(s, ctx)
    if s.current_stage == FunnelStage.TERMINATED:
        return _wrap(qid, s)

    # 5. Stub Stage 3 synthesis: advance to PROPOSED with a typed ProposalAttempt
    # placeholder. The outcome is "applied" as a pending tag — downstream
    # gates (when wired into the orchestrator) will flip to the real
    # terminal outcome. For Phase 1 fixtures, this is the contract:
    # the proposal LANDS — that is what the merge gate asserts.
    proposal_attempt = ProposalAttempt(
        attempt_index=0,
        intent_id=mocked_prop["intent_id"],
        patch_type=mocked_prop["patch_type"],
        deepest_stage_in_attempt=FunnelStage.PROPOSED,
        outcome="applied",  # tentative; gates downstream may flip
        outcome_reason="pending_gates",
        patch_outcome_id=None,
    )
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "plan11_stage3", "llm"),
        proposals=(proposal_attempt,),
    )

    return _wrap(qid, s)


def _wrap(qid: str, state: QuestionStateInIteration) -> AnchorReplayResult:
    traj = build_trajectory(qid=qid, iterations=(state,))
    return AnchorReplayResult(
        trajectory=traj,
        proposal_attempts_count=len(state.proposals),
        deepest_reached=traj.deepest_stage_ever,
    )
