"""Plan 11 Stage 2 — cluster_diagnoses() unit tests."""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
    PerQidDiagnosis,
)


def _make_diagnosis(qid: str) -> PerQidDiagnosis:
    return PerQidDiagnosis(
        qid=qid,
        rca_kind_label="top-N collapsed",
        observed_failure="Wrong rows",
        generated_sql_issue="MAX instead of ORDER BY LIMIT",
        expected_sql_shape="ORDER BY revenue DESC LIMIT 10",
        blame_set=(),
        evidence_summary="summary",
        confidence="high",
    )


def _make_success_response(theme: str, qids: list[str]) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_stage2_cluster.iter_1.hard",
        skill_id="plan11_cluster",
        succeeded=True,
        parsed_output={
            "clusters": [
                {
                    "semantic_theme": theme,
                    "member_qids": qids,
                    "unifying_evidence": "shared evidence",
                    "repair_hypothesis": "Replace RANK() with ROW_NUMBER() + LIMIT",
                    "primary_blame_set": [],
                    "confidence": "high",
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=80,
        tokens_output=40,
        duration_ms=700,
        error=None,
    )


def _make_decline_response() -> LlmReasoningResponse:
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    verdict = AbstainVerdict(
        reason=AbstainReason.AMBIGUOUS_FAILURE,
        explanation="no coherent grouping",
        needed_evidence=(),
        suggested_next_step="re-diagnose",
    )
    return LlmReasoningResponse(
        call_id="plan11_stage2_cluster.iter_1.hard",
        skill_id="plan11_cluster",
        succeeded=False,
        parsed_output=None,
        declined=verdict,
        raw_text="",
        tokens_input=60,
        tokens_output=10,
        duration_ms=500,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.cluster_plan11.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.cluster_plan11.plan11_stage2_clustering_marker"
)
def test_cluster_happy_path(mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE2_CLUSTERING_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_success_response("top-N failures", ["gs_009"])
    )

    clusters = cluster_diagnoses(
        diagnoses=[_make_diagnosis("gs_009")],
        schema_columns=[],
        optimization_run_id="run_x",
        iteration=1,
        namespace="hard",
        w=MagicMock(),
    )

    assert len(clusters) == 1
    assert isinstance(clusters[0], FailureCluster)
    assert clusters[0].cluster_id == "H001"
    assert clusters[0].repair_hypothesis == "Replace RANK() with ROW_NUMBER() + LIMIT"
    assert clusters[0].member_qids == ("gs_009",)
    mock_marker.assert_called_once()
    assert mock_marker.call_args[1]["outcome"] == "clustered"
    assert mock_marker.call_args[1]["clusters_count"] == 1


@patch("genie_space_optimizer.optimization.stages.cluster_plan11.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.cluster_plan11.plan11_stage2_clustering_marker"
)
def test_cluster_decline_returns_empty(mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE2_CLUSTERING_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_decline_response())

    clusters = cluster_diagnoses(
        diagnoses=[_make_diagnosis("gs_009")],
        schema_columns=[],
        optimization_run_id="run_x",
        iteration=1,
        namespace="hard",
        w=MagicMock(),
    )

    assert clusters == []
    assert mock_marker.call_args[1]["outcome"] == "declined"


@patch("genie_space_optimizer.optimization.stages.cluster_plan11.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.cluster_plan11.plan11_stage2_clustering_marker"
)
def test_cluster_drops_hallucinated_member_qids(mock_marker, MockLlmCall):
    """LLM returning member_qids outside the input set are dropped; we
    fall back to ALL input qids rather than silently emit an empty cluster.
    """
    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE2_CLUSTERING_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_success_response("theme", ["gs_999_fake"])
    )

    clusters = cluster_diagnoses(
        diagnoses=[_make_diagnosis("gs_009")],
        schema_columns=[],
        optimization_run_id="run_x",
        iteration=1,
        namespace="hard",
        w=MagicMock(),
    )

    assert len(clusters) == 1
    assert clusters[0].member_qids == ("gs_009",)


def test_cluster_empty_input_returns_empty():
    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )
    clusters = cluster_diagnoses(
        diagnoses=[],
        schema_columns=[],
        optimization_run_id="run_x",
        iteration=1,
        namespace="hard",
        w=MagicMock(),
    )
    assert clusters == []
