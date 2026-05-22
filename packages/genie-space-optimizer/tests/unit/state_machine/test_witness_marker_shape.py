"""GSO_QSTATE_TRANSITION_V1 witness marker shape."""
import json

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    qstate_transition_marker,
)
from genie_space_optimizer.optimization.state_machine.records import (
    StageTransition,
)


def _parse(line: str) -> tuple[str, dict]:
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_qstate_transition_marker_shape():
    transition = StageTransition(
        from_stage=FunnelStage.HARD_QID_SEEN,
        to_stage=FunnelStage.DIAGNOSED,
        at_ms=1234,
        transformer_name="plan11_stage1",
        transition_kind="llm",
    )
    line = qstate_transition_marker(
        run_id="run_x",
        iteration=2,
        qid="gs_009",
        transition=transition,
    )
    name, payload = _parse(line)
    assert name == "GSO_QSTATE_TRANSITION_V1"
    assert payload["qid"] == "gs_009"
    assert payload["iteration"] == 2
    assert payload["from_stage"] == "hard_qid_seen"
    assert payload["to_stage"] == "diagnosed"
    assert payload["transformer_name"] == "plan11_stage1"
    assert payload["transition_kind"] == "llm"


def test_marker_carries_reason_when_present():
    t = StageTransition(
        FunnelStage.APPLYABLE,
        FunnelStage.TERMINATED,
        1,
        "blast_radius_gate",
        "validation_gate",
        reason="all_escalation_paths_exhausted",
    )
    name, payload = _parse(qstate_transition_marker(run_id="r", iteration=1, qid="q", transition=t))
    assert payload["reason"] == "all_escalation_paths_exhausted"


def test_marker_carries_proposal_attempt_index_when_set():
    t = StageTransition(
        FunnelStage.PROPOSED,
        FunnelStage.NORMALIZED,
        1,
        "structural_repair_gate",
        "validation_gate",
        proposal_attempt_index=2,
    )
    name, payload = _parse(qstate_transition_marker(run_id="r", iteration=1, qid="q", transition=t))
    assert payload["proposal_attempt_index"] == 2
