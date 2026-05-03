from unittest.mock import MagicMock

from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.clustering import (
    ClusteringInput,
    ClusterFindings,
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


def test_clustering_input_required_fields() -> None:
    inp = ClusteringInput(
        eval_result_for_clustering={"rows": [{"question_id": "q1"}]},
        metadata_snapshot={"id": "s1"},
        soft_eval_result=None,
    )
    assert inp.eval_result_for_clustering["rows"][0]["question_id"] == "q1"
    assert inp.soft_eval_result is None


def test_cluster_findings_required_fields() -> None:
    cf = ClusterFindings(
        clusters=({"cluster_id": "H001", "question_ids": ("q1",), "root_cause": "wrong_join_spec"},),
        soft_clusters=(),
        rejected_cluster_alternatives=({"cluster_id": "H002", "demoted_reason": "below_hard_threshold"},),
    )
    assert cf.clusters[0]["cluster_id"] == "H001"
    assert cf.rejected_cluster_alternatives[0]["demoted_reason"] == "below_hard_threshold"


def test_form_invokes_cluster_failures_and_returns_findings(monkeypatch) -> None:
    """form() calls optimizer.cluster_failures with the real signature
    (positional eval_result + metadata_snapshot, keyword args for spark/run_id/etc.)
    and returns a ClusterFindings."""
    from genie_space_optimizer.optimization.stages import clustering as clust

    fake_clusters = [
        {"cluster_id": "H001", "question_ids": ["q1", "q2"], "root_cause": "wrong_join_spec"},
        {"cluster_id": "H002", "question_ids": [], "demoted_reason": "below_hard_threshold"},
    ]
    captured_calls: list = []

    def _stub_cluster_failures(eval_result, metadata, **kw):
        captured_calls.append((eval_result, metadata, kw))
        return list(fake_clusters)

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.clustering.cluster_failures",
        _stub_cluster_failures,
    )

    ctx = _stub_ctx()
    inp = clust.ClusteringInput(
        eval_result_for_clustering={"rows": [{"question_id": "q1"}, {"question_id": "q2"}]},
        metadata_snapshot={"id": "s1"},
        soft_eval_result=None,
    )

    out = clust.form(ctx, inp)

    # cluster_failures was called once (no soft).
    assert len(captured_calls) == 1
    eval_result_arg, metadata_arg, kw = captured_calls[0]
    assert eval_result_arg == inp.eval_result_for_clustering
    assert metadata_arg == inp.metadata_snapshot
    assert kw["signal_type"] == "hard"
    assert kw["namespace"] == "H"

    # Promoted vs rejected split by demoted_reason.
    assert any(c["cluster_id"] == "H001" for c in out.clusters)
    assert any(
        c["cluster_id"] == "H002" for c in out.rejected_cluster_alternatives
    )


def test_form_invokes_soft_clustering_when_soft_rows_present(monkeypatch) -> None:
    from genie_space_optimizer.optimization.stages import clustering as clust

    captured_signals: list[str] = []

    def _stub_cluster_failures(eval_result, metadata, **kw):
        captured_signals.append(kw.get("signal_type", ""))
        if kw.get("signal_type") == "soft":
            return [{"cluster_id": "S001", "question_ids": ["q3"], "signal_type": "soft"}]
        return [{"cluster_id": "H001", "question_ids": ["q1"]}]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.clustering.cluster_failures",
        _stub_cluster_failures,
    )

    ctx = _stub_ctx()
    inp = clust.ClusteringInput(
        eval_result_for_clustering={"rows": [{"question_id": "q1"}]},
        metadata_snapshot={"id": "s1"},
        soft_eval_result={"rows": [{"question_id": "q3"}]},
    )

    out = clust.form(ctx, inp)
    assert "hard" in captured_signals
    assert "soft" in captured_signals
    assert any(c["cluster_id"] == "S001" for c in out.soft_clusters)


def test_form_handles_empty_eval_result(monkeypatch) -> None:
    """Empty eval rows → cluster_failures returns []; ClusterFindings is empty."""
    from genie_space_optimizer.optimization.stages import clustering as clust
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.clustering.cluster_failures",
        lambda *a, **k: [],
    )
    ctx = _stub_ctx()
    inp = clust.ClusteringInput(
        eval_result_for_clustering={"rows": []},
        metadata_snapshot={},
        soft_eval_result=None,
    )
    out = clust.form(ctx, inp)
    assert out.clusters == ()
    assert out.soft_clusters == ()
    assert out.rejected_cluster_alternatives == ()
