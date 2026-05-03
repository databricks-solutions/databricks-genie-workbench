from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.action_groups import (
    ActionGroupsInput,
    ActionGroupSlate,
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


def test_action_groups_input_required_fields() -> None:
    inp = ActionGroupsInput(
        action_groups=(
            {
                "id": "AG_001",
                "source_cluster_ids": ["c1"],
                "affected_questions": ["q1"],
                "lever_directives": {6: {"target_qids": ["q1"]}},
            },
        ),
        source_clusters_by_id={"c1": {"cluster_id": "c1", "root_cause": "wrong_join_spec"}},
        rca_id_by_cluster={"c1": "rca_001"},
    )
    assert inp.action_groups[0]["id"] == "AG_001"
    assert inp.rca_id_by_cluster["c1"] == "rca_001"


def test_action_group_slate_required_fields() -> None:
    sl = ActionGroupSlate(
        ags=({"id": "AG_001", "source_cluster_ids": ["c1"]},),
        rejected_ag_alternatives=(
            {"id": "AG_002", "reject_reason": "below_priority"},
        ),
    )
    assert sl.ags[0]["id"] == "AG_001"
    assert sl.rejected_ag_alternatives[0]["reject_reason"] == "below_priority"


def test_select_emits_strategist_ag_record_per_ag() -> None:
    """select() emits one STRATEGIST_AG_EMITTED record per AG."""
    from genie_space_optimizer.optimization.stages import action_groups as ags
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = ags.ActionGroupsInput(
        action_groups=(
            {
                "id": "AG_001",
                "source_cluster_ids": ["c1"],
                "affected_questions": ["q1"],
                "lever_directives": {6: {"target_qids": ["q1"]}},
            },
            {
                "id": "AG_002",
                "source_cluster_ids": ["c2"],
                "affected_questions": ["q2"],
                "lever_directives": {5: {"target_qids": ["q2"]}},
            },
        ),
        source_clusters_by_id={
            "c1": {"cluster_id": "c1", "root_cause": "wrong_join_spec"},
            "c2": {"cluster_id": "c2", "root_cause": "measure_swap"},
        },
        rca_id_by_cluster={"c1": "rca_001", "c2": "rca_002"},
    )

    out = ags.select(ctx, inp)

    assert len(out.ags) == 2
    assert {a["id"] for a in out.ags} == {"AG_001", "AG_002"}
    # Two STRATEGIST_AG_EMITTED records — one per AG.
    record_types = [r.decision_type.value for r in captured]
    assert record_types.count("strategist_ag_emitted") == 2


def test_select_handles_missing_target_qids() -> None:
    """An AG with no target_qids gets reason_code=MISSING_TARGET_QIDS
    (the Cycle-8-Bug-1 signal). The cross-checker exempts it."""
    from genie_space_optimizer.optimization.stages import action_groups as ags
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = ags.ActionGroupsInput(
        action_groups=(
            {
                "id": "AG_no_targets",
                "source_cluster_ids": [],
                "affected_questions": [],
                "lever_directives": {},
            },
        ),
        source_clusters_by_id={},
        rca_id_by_cluster={},
    )
    ags.select(ctx, inp)
    assert len(captured) == 1
    assert captured[0].reason_code == ReasonCode.MISSING_TARGET_QIDS


def test_select_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.stages import action_groups as ags
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = ags.ActionGroupsInput(
        action_groups=(),
        source_clusters_by_id={},
        rca_id_by_cluster={},
    )
    out = ags.select(ctx, inp)
    assert out.ags == ()
    assert captured == []
