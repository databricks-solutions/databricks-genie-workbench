from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.application import (
    ApplicationInput,
    AppliedPatchSet,
    AppliedPatch,
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


def test_application_input_required_fields() -> None:
    inp = ApplicationInput(
        applied_entries_by_ag={"AG_001": (
            {"patch": {"proposal_id": "P_001", "patch_type": "x",
                       "target_qids": ["q1"], "cluster_id": "c1"}},
        )},
        ags=({"ag_id": "AG_001", "target_qids": ("q1",)},),
        rca_id_by_cluster={"c1": "rca_001"},
        cluster_root_cause_by_id={"c1": "wrong_join_spec"},
    )
    assert "AG_001" in inp.applied_entries_by_ag


def test_applied_patch_set_required_fields() -> None:
    aps = AppliedPatchSet(
        applied=(
            AppliedPatch(
                proposal_id="P_001",
                ag_id="AG_001",
                patch_type="update_instruction_section",
                target_qids=("q1",),
                cluster_id="c1",
                content_fingerprint="abc",
                rolled_back_immediately=False,
                rollback_reason=None,
            ),
        ),
        applied_signature="sig_xyz",
    )
    assert aps.applied[0].proposal_id == "P_001"
    assert aps.applied_signature == "sig_xyz"


def test_apply_emits_patch_applied_record_per_entry() -> None:
    """apply() emits one PATCH_APPLIED DecisionRecord per applied
    entry (whose patch has non-empty target_qids)."""
    from genie_space_optimizer.optimization.stages import application as app
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = app.ApplicationInput(
        applied_entries_by_ag={
            "AG_001": (
                {"patch": {"proposal_id": "P_001",
                           "patch_type": "update_instruction_section",
                           "target_qids": ["q1"],
                           "cluster_id": "c1",
                           "content_fingerprint": "abc"}},
                {"patch": {"proposal_id": "P_002",
                           "patch_type": "add_example_sql",
                           "target_qids": ["q2"],
                           "cluster_id": "c1",
                           "content_fingerprint": "def"}},
            ),
        },
        ags=({"ag_id": "AG_001", "target_qids": ("q1", "q2")},),
        rca_id_by_cluster={"c1": "rca_001"},
        cluster_root_cause_by_id={"c1": "wrong_join_spec"},
    )

    out = app.apply(ctx, inp)

    # 2 patches → 2 PATCH_APPLIED records
    record_types = [r.decision_type.value for r in captured]
    assert record_types.count("patch_applied") == 2
    # Slate carries 2 AppliedPatch entries
    assert len(out.applied) == 2
    proposal_ids = {p.proposal_id for p in out.applied}
    assert proposal_ids == {"P_001", "P_002"}
    # All applied (no immediate rollback in happy path)
    assert all(not p.rolled_back_immediately for p in out.applied)


def test_apply_marks_immediate_rollback_when_entry_carries_rollback_marker() -> None:
    """An applied entry that carries rollback_reason gets stamped on
    AppliedPatch.rolled_back_immediately=True."""
    from genie_space_optimizer.optimization.stages import application as app
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = app.ApplicationInput(
        applied_entries_by_ag={
            "AG_001": (
                {
                    "patch": {"proposal_id": "P_failed",
                              "patch_type": "x",
                              "target_qids": ["q1"],
                              "cluster_id": "c1"},
                    "rolled_back_immediately": True,
                    "rollback_reason": "schema drift after apply",
                },
            ),
        },
        ags=({"ag_id": "AG_001", "target_qids": ("q1",)},),
        rca_id_by_cluster={"c1": "rca_001"},
        cluster_root_cause_by_id={"c1": "wrong_join_spec"},
    )

    out = app.apply(ctx, inp)

    assert len(out.applied) == 1
    assert out.applied[0].rolled_back_immediately is True
    assert "schema drift" in (out.applied[0].rollback_reason or "")


def test_apply_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.stages import application as app
    captured: list = []
    ctx = _stub_ctx()
    ctx.decision_emit = lambda r: captured.append(r)

    inp = app.ApplicationInput(
        applied_entries_by_ag={},
        ags=(),
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )
    out = app.apply(ctx, inp)
    assert out.applied == ()
    assert captured == []


def test_apply_computes_stable_applied_signature() -> None:
    """applied_signature is deterministic — same inputs give same hash."""
    from genie_space_optimizer.optimization.stages import application as app
    ctx = _stub_ctx()

    def _make_inp():
        return app.ApplicationInput(
            applied_entries_by_ag={
                "AG_001": (
                    {"patch": {"proposal_id": "P_001",
                               "patch_id": "patch_001",
                               "patch_type": "x",
                               "target_qids": ["q1"],
                               "cluster_id": "c1"}},
                ),
            },
            ags=({"ag_id": "AG_001"},),
            rca_id_by_cluster={"c1": "rca_001"},
            cluster_root_cause_by_id={"c1": "wrong_join_spec"},
        )

    sig1 = app.apply(ctx, _make_inp()).applied_signature
    sig2 = app.apply(ctx, _make_inp()).applied_signature
    assert sig1 == sig2
    assert sig1  # non-empty
