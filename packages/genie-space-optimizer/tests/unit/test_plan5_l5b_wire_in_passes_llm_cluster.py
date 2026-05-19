"""Plan 8 Task 2 — confirm _stage_2_l5b passes rca_evidence_typed,
llm_cluster, ag_id, iteration to _dispatch_lever_5b_for_cluster so
Plan 5's LLM intent short-circuit activates."""
from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.activation_bundle import (
    ActivationBundle,
)
from genie_space_optimizer.optimization.three_stage_pipeline import (
    _stage_2_l5b,
)


_SENTINEL_EVIDENCE = {"q1": object()}
_SENTINEL_LLM_CLUSTER = object()


def _bundle() -> ActivationBundle:
    return ActivationBundle(
        skill_id="lever-5b-example-sql",
        ag_id="AG_X",
        target_objects=(),
        cluster_afs=({"cluster_id": "H001",
                       "question_ids": ["q1"],
                       "root_cause": "wrong_column"},),
        metadata_snapshot={"iteration": 3, "schema_columns": ["a", "b"]},
        identifier_allowlist="",
        evidence_refs=(),
        expected_impact_qids=(),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="",
        priority=1,
        rca_evidence_typed=_SENTINEL_EVIDENCE,
        llm_cluster_by_cluster_id={"H001": _SENTINEL_LLM_CLUSTER},
        iteration=3,
    )


def test_stage_2_l5b_threads_intent_aware_kwargs():
    with patch(
        "genie_space_optimizer.optimization.optimizer."
        "_dispatch_lever_5b_for_cluster",
        return_value=[],
    ) as mock_disp:
        _stage_2_l5b(_bundle(), w=None)
    assert mock_disp.call_count == 1
    kw = mock_disp.call_args.kwargs
    assert kw["ag_id"] == "AG_X"
    assert kw["iteration"] == 3
    assert kw["rca_evidence_typed"] is _SENTINEL_EVIDENCE
    assert kw["llm_cluster"] is _SENTINEL_LLM_CLUSTER


def test_stage_2_l5b_passes_none_llm_cluster_when_cluster_id_absent():
    """When the bundle's llm_cluster_by_cluster_id has no entry for the
    AFS's cluster_id, the per-cluster dispatcher gets llm_cluster=None
    (which makes Plan 5's short-circuit fall back to deterministic)."""
    b = ActivationBundle(
        skill_id="lever-5b-example-sql",
        ag_id="AG_X",
        target_objects=(),
        cluster_afs=({"cluster_id": "H_UNKNOWN", "question_ids": []},),
        metadata_snapshot={},
        identifier_allowlist="",
        evidence_refs=(),
        expected_impact_qids=(),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="",
        priority=1,
        rca_evidence_typed={},
        llm_cluster_by_cluster_id={"H001": _SENTINEL_LLM_CLUSTER},
        iteration=0,
    )
    with patch(
        "genie_space_optimizer.optimization.optimizer."
        "_dispatch_lever_5b_for_cluster",
        return_value=[],
    ) as mock_disp:
        _stage_2_l5b(b, w=None)
    assert mock_disp.call_args.kwargs["llm_cluster"] is None
