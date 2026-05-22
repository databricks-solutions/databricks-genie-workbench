"""QuestionStateInIteration round-trips through JsonRoundTrip across all populated records."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
    build_initial_state,
)


def test_fully_populated_state_roundtrips():
    s = build_initial_state(
        qid="gs_009",
        iteration=1,
        seen=HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
        diagnosed=DiagnosisRecord("plan11_stage1", "x", "x", "x", "x", "high", "rca_1"),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
        clustered=ClusterMembershipRecord("H001", "AG_1", ("gs_009",), 6, "plural_top_n_collapse"),
    )
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
        proposals=(ProposalAttempt(0, "i1", "add_sql_snippet_expression",
                                   FunnelStage.PROPOSED, "escalated", "...", 1, "po_1"),),
    )
    payload = s.to_json()
    s2 = QuestionStateInIteration.from_json(payload)
    assert s2 == s
