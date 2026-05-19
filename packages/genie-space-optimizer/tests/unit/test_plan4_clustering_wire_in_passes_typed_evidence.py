"""Plan 8 Task 1 — confirm stages/clustering.form passes
rca_evidence_typed + w into optimizer.cluster_failures so Plan 4's
LLM short-circuit activates."""
from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.clustering import (
    ClusteringInput, form,
)


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid,
        observed_failure="wrong_column",
        generated_sql_issue="selected store_id",
        expected_sql_shape="select location_id",
        blame_set=("catalog.schema.dim_store.location_id",),
        suggested_repair_family="column_description",
        repair_hint_patch_type=PatchType.ADD_COLUMN_DESCRIPTION,
        confidence="high",
        quoted_evidence=("expected: location_id",),
    )


def _ctx() -> StageContext:
    return StageContext(
        run_id="r1", iteration=1, space_id="s", domain="d",
        catalog="c", schema="sc", apply_mode="apply",
        journey_emit=lambda **kw: None,
        decision_emit=lambda rec: None,
        feature_flags={},
    )


def test_form_threads_rca_evidence_typed_and_w_into_cluster_failures():
    inp = ClusteringInput(
        eval_result_for_clustering={"rows": []},
        metadata_snapshot={"_rca_evidence_typed": {"q1": _evidence("q1"),
                                                    "q2": _evidence("q2")}},
        soft_eval_result=None, held_out_qids=(), qid_state={},
    )
    with patch(
        "genie_space_optimizer.optimization.stages.clustering."
        "cluster_failures",
        return_value=[],
    ) as mock_cf:
        form(_ctx(), inp)
    assert mock_cf.call_count >= 1
    kwargs = mock_cf.call_args_list[0].kwargs
    assert "rca_evidence_typed" in kwargs, (
        "ClusteringInput.metadata_snapshot[_rca_evidence_typed] "
        "must be threaded into cluster_failures(rca_evidence_typed=...)"
    )
    assert set(kwargs["rca_evidence_typed"].keys()) == {"q1", "q2"}
    assert "w" in kwargs
