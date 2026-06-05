"""Step §C of the production-seam wire-in plan.

``_invoke_stage2_llm`` now adapts ``stages.cluster_plan11.cluster_diagnoses``
into the response shape ``transform_batch`` consumes. Tests monkeypatch
``cluster_diagnoses`` at the adapter callsite.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    cluster_batch as cluster_module,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _diagnosed(qid: str):
    s = build_initial_state(
        qid=qid, iteration=1,
        seen=HardQidSeenRecord(
            eval_row_id="r",
            predicate="row_is_hard_failure",
            score=0.0,
            baseline_sql="S",
            expected_shape="x",
            iteration_first_seen=1,
        ),
    )
    return s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm",
        ),
        diagnosed=DiagnosisRecord(
            source="plan11_stage1",
            rca_kind_label="plural_top_n_collapse",
            evidence_summary="s",
            observed_failure="f",
            expected_sql_shape="e",
            confidence="high",
            rca_card_id="rca_001",
        ),
    )


def _ctx(**kw) -> TransformerContext:
    base = dict(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        schema_columns=("orders.id",),
        w=None,
    )
    base.update(kw)
    return TransformerContext(**base)


def test_happy_path_two_members_one_cluster(monkeypatch):
    """cluster_diagnoses returns one cluster of two QIDs → both states
    land at CLUSTERED with shared cluster_id."""
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    captured = {}

    def fake_cluster(
        *, diagnoses, schema_columns, optimization_run_id,
        iteration, namespace, w, forbidden_signatures=(),
        insufficient_repair_signatures=(),
    ):
        captured["diagnoses_count"] = len(diagnoses)
        captured["namespace"] = namespace
        return [
            FailureCluster(
                cluster_id="H001",
                semantic_theme="top-N collapse",
                member_qids=("gs_009", "gs_026"),
                unifying_evidence="judge said collapsed plural",
                repair_hypothesis="plural_top_n_repair",
                primary_blame_set=("orders.status",),
                confidence="high",
            ),
        ]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.cluster_plan11.cluster_diagnoses",
        fake_cluster,
    )

    batch = (_diagnosed("gs_009"), _diagnosed("gs_026"))
    out = cluster_module.plan11_stage2_clustering.transform_batch(batch, _ctx())

    assert len(out) == 2
    for s in out:
        assert s.current_stage == FunnelStage.CLUSTERED
        assert s.clustered is not None
        assert s.clustered.cluster_id == "H001"
        assert s.clustered.ag_id == "AG_H001"
        assert set(s.clustered.co_member_qids) == {"gs_009", "gs_026"}
        # routing_evidence_kind is non-empty (mandatory per ClusterMembershipRecord).
        assert s.clustered.routing_evidence_kind != ""
    assert captured["namespace"] == "hard"
    assert captured["diagnoses_count"] == 2


def test_empty_clusters_terminates_all(monkeypatch):
    """cluster_diagnoses returns [] (LLM declined) → every state in the
    batch terminates with OPTIMIZER_NO_CANDIDATES."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.cluster_plan11.cluster_diagnoses",
        lambda **kw: [],
    )

    batch = (_diagnosed("gs_009"),)
    out = cluster_module.plan11_stage2_clustering.transform_batch(batch, _ctx())
    assert out[0].current_stage == FunnelStage.TERMINATED
    assert out[0].terminal.kind == "OPTIMIZER_NO_CANDIDATES"


def test_qid_dropped_from_cluster_terminates_only_that_state(monkeypatch):
    """When the LLM returns a cluster that omits one of the input QIDs,
    that QID's state terminates while the other advances."""
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    def fake_cluster(**kw):
        return [
            FailureCluster(
                cluster_id="H001",
                semantic_theme="theme",
                member_qids=("gs_009",),  # gs_026 dropped
                unifying_evidence="",
                repair_hypothesis="hypothesis_h001",
                primary_blame_set=(),
                confidence="medium",
            ),
        ]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.cluster_plan11.cluster_diagnoses",
        fake_cluster,
    )

    batch = (_diagnosed("gs_009"), _diagnosed("gs_026"))
    out = cluster_module.plan11_stage2_clustering.transform_batch(batch, _ctx())
    by_qid = {s.qid: s for s in out}
    assert by_qid["gs_009"].current_stage == FunnelStage.CLUSTERED
    assert by_qid["gs_026"].current_stage == FunnelStage.TERMINATED


def test_single_state_path_via_transform(monkeypatch):
    """The orchestrator's per-state ``step()`` calls ``transform(state, ctx)``,
    which wraps a single state into a 1-tuple and dispatches to
    transform_batch."""
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.cluster_plan11.cluster_diagnoses",
        lambda **kw: [
            FailureCluster(
                cluster_id="H001",
                semantic_theme="theme",
                member_qids=("gs_009",),
                unifying_evidence="",
                repair_hypothesis="hypothesis",
                primary_blame_set=(),
                confidence="high",
            ),
        ],
    )

    state = _diagnosed("gs_009")
    out = cluster_module.plan11_stage2_clustering.transform(state, _ctx())
    assert out.current_stage == FunnelStage.CLUSTERED
    assert out.clustered.cluster_id == "H001"
