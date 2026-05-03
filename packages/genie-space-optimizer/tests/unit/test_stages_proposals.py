from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.proposals import (
    ProposalsInput,
    ProposalSlate,
)


def _stub_ctx() -> StageContext:
    return StageContext(
        run_id="r1",
        iteration=2,
        space_id="s1",
        domain="airline",
        catalog="main",
        schema="gso",
        apply_mode="real",
        journey_emit=MagicMock(),
        decision_emit=MagicMock(),
        mlflow_anchor_run_id=None,
        feature_flags={},
    )


def test_proposals_input_required_fields() -> None:
    inp = ProposalsInput(
        proposals_by_ag={
            "AG_001": (
                {
                    "proposal_id": "P_001",
                    "patch_type": "update_instruction_section",
                    "target_qids": ("q1",),
                    "cluster_id": "c1",
                },
            ),
        },
        rca_id_by_cluster={"c1": "rca_001"},
        cluster_root_cause_by_id={"c1": "wrong_join_spec"},
    )
    assert inp.proposals_by_ag["AG_001"][0]["proposal_id"] == "P_001"


def test_proposal_slate_required_fields() -> None:
    sl = ProposalSlate(
        proposals_by_ag={
            "AG_001": (
                {"proposal_id": "P_001", "content_fingerprint": "abc"},
            ),
        },
        rejected_proposal_alternatives=({"proposal_id": "P_X", "reject_reason": "malformed"},),
        content_fingerprints_emitted=("abc",),
    )
    assert sl.proposals_by_ag["AG_001"][0]["content_fingerprint"] == "abc"
    assert "abc" in sl.content_fingerprints_emitted


def test_generate_emits_proposal_records_with_content_fingerprint() -> None:
    """generate() emits one PROPOSAL_GENERATED per proposal and stamps
    each with a content_fingerprint via patch_retry_signature."""
    from genie_space_optimizer.optimization.stages import proposals as ps
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = ps.ProposalsInput(
        proposals_by_ag={
            "AG_001": (
                {
                    "proposal_id": "P_001",
                    "patch_type": "update_instruction_section",
                    "target_qids": ("q1",),
                    "cluster_id": "c1",
                    "value": "JOIN dim_route ON ... AND year=2023",
                    "instruction_section": "QUERY PATTERNS",
                },
                {
                    "proposal_id": "P_002",
                    "patch_type": "add_example_sql",
                    "target_qids": ("q2",),
                    "cluster_id": "c1",
                    "example_sql": "SELECT TOP 5 ...",
                },
            ),
        },
        rca_id_by_cluster={"c1": "rca_001"},
        cluster_root_cause_by_id={"c1": "wrong_join_spec"},
    )

    out = ps.generate(ctx, inp)

    assert "AG_001" in out.proposals_by_ag
    # 2 proposals → 2 PROPOSAL_GENERATED records
    record_types = [r.decision_type.value for r in captured]
    assert record_types.count("proposal_generated") == 2
    # Every proposal has content_fingerprint stamped.
    for p in out.proposals_by_ag["AG_001"]:
        assert "content_fingerprint" in p
        assert isinstance(p["content_fingerprint"], str)
    # Aggregate fingerprints surfaced.
    assert len(out.content_fingerprints_emitted) == 2


def test_generate_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.stages import proposals as ps
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = ps.ProposalsInput(
        proposals_by_ag={},
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )
    out = ps.generate(ctx, inp)
    assert out.proposals_by_ag == {}
    assert captured == []
    assert out.content_fingerprints_emitted == ()


def test_generate_skips_proposals_without_target_qids() -> None:
    """A proposal with empty target_qids does not produce a
    PROPOSAL_GENERATED record (the producer drops it per Cycle-8-Bug-1)."""
    from genie_space_optimizer.optimization.stages import proposals as ps
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = ps.ProposalsInput(
        proposals_by_ag={
            "AG_001": (
                {"proposal_id": "P_no_target", "patch_type": "x",
                 "target_qids": (), "cluster_id": "c1"},
            ),
        },
        rca_id_by_cluster={"c1": "rca_001"},
        cluster_root_cause_by_id={"c1": "wrong_join_spec"},
    )
    out = ps.generate(ctx, inp)
    # Slate still includes the proposal (with fingerprint) for downstream
    # gates to inspect, but no DecisionRecord is emitted.
    assert "AG_001" in out.proposals_by_ag
    assert captured == []
