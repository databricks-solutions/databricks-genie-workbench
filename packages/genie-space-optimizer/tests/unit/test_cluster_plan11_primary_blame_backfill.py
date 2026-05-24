"""Trial 13g — Stage 2 clustering backfills empty primary_blame_set.

When the Stage 2 LLM omits ``primary_blame_set`` in its output, the
clustering handler now unions the member QIDs' Stage 1 diagnosis
blame_sets so the resulting :class:`FailureCluster` has a non-empty
blame seed for Stage 3. The 98ec production replay run exposed this
gap (empty cluster blame → empty proposal blame → survival contract
rejected). The Stage 2 marker emits
``primary_blame_set_backfilled`` so postmortems can see how often
the backfill fires.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    PerQidDiagnosis,
)


def _diagnosis(qid: str, blame_set: tuple[str, ...]) -> PerQidDiagnosis:
    return PerQidDiagnosis(
        qid=qid,
        rca_kind_label="row_count_mismatch",
        observed_failure="row count too high",
        generated_sql_issue="RANK() didn't bound rows",
        expected_sql_shape="LIMIT N",
        blame_set=blame_set,
        evidence_summary="judge said top-N is off-by-many",
        confidence="high",
    )


def _llm_response(
    *,
    raw_clusters: list[dict],
) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_stage2_cluster.iter_1.hard",
        skill_id="plan11_cluster",
        succeeded=True,
        parsed_output={"clusters": raw_clusters},
        declined=None,
        raw_text="{...}",
        tokens_input=300,
        tokens_output=120,
        duration_ms=2200,
        error=None,
    )


def _run(*, diagnoses, raw_clusters):
    from genie_space_optimizer.optimization.stages.cluster_plan11 import (
        cluster_diagnoses,
    )
    captured_lines: list[str] = []

    def _capture(line):
        captured_lines.append(line)

    with patch(
        "genie_space_optimizer.optimization.stages.cluster_plan11.LlmReasoningCall"
    ) as MockLlmCall, patch(
        "builtins.print", side_effect=_capture,
    ):
        MockLlmCall.return_value.invoke = MagicMock(
            return_value=_llm_response(raw_clusters=raw_clusters),
        )
        clusters = cluster_diagnoses(
            diagnoses=diagnoses,
            schema_columns=[],
            optimization_run_id="run_t13g",
            iteration=1,
            namespace="hard",
            w=MagicMock(),
        )
    marker_payload = None
    for line in captured_lines:
        if line.startswith("GSO_PLAN11_STAGE2_CLUSTERING_V1 "):
            marker_payload = json.loads(line.split(" ", 1)[1])
            break
    return clusters, marker_payload


def test_backfill_unions_member_blame_sets_when_llm_omits() -> None:
    """LLM returns empty primary_blame_set → handler unions member
    diagnoses' blame_sets and the resulting cluster carries them."""
    diagnoses = [
        _diagnosis("q1", ("main.sales.orders.order_id",)),
        _diagnosis("q2", ("main.sales.orders.amount",)),
    ]
    raw_clusters = [
        {
            "semantic_theme": "top-N",
            "member_qids": ["q1", "q2"],
            "unifying_evidence": "RANK() unbounded",
            "repair_hypothesis": "ROW_NUMBER+LIMIT",
            "primary_blame_set": [],  # LLM omitted
            "confidence": "high",
        },
    ]
    clusters, marker = _run(diagnoses=diagnoses, raw_clusters=raw_clusters)
    assert len(clusters) == 1
    assert clusters[0].primary_blame_set == (
        "main.sales.orders.order_id",
        "main.sales.orders.amount",
    )
    assert marker is not None
    assert marker["primary_blame_set_backfilled"] == 1


def test_backfill_skipped_when_llm_emits_primary_blame_set() -> None:
    """LLM-emitted primary_blame_set wins; backfill does not fire."""
    diagnoses = [_diagnosis("q1", ("main.sales.orders.order_id",))]
    raw_clusters = [
        {
            "semantic_theme": "top-N",
            "member_qids": ["q1"],
            "unifying_evidence": "RANK() unbounded",
            "repair_hypothesis": "ROW_NUMBER+LIMIT",
            "primary_blame_set": ["main.sales.orders.LLM_picked"],
            "confidence": "high",
        },
    ]
    clusters, marker = _run(diagnoses=diagnoses, raw_clusters=raw_clusters)
    assert clusters[0].primary_blame_set == (
        "main.sales.orders.LLM_picked",
    )
    assert marker["primary_blame_set_backfilled"] == 0


def test_backfill_dedupes_and_preserves_arrival_order() -> None:
    """Members share blame entries → union dedupes in arrival order."""
    diagnoses = [
        _diagnosis("q1", ("a.b", "c.d")),
        _diagnosis("q2", ("c.d", "e.f")),
    ]
    raw_clusters = [
        {
            "semantic_theme": "x",
            "member_qids": ["q1", "q2"],
            "unifying_evidence": "y",
            "repair_hypothesis": "z",
            "primary_blame_set": [],
            "confidence": "high",
        },
    ]
    clusters, _ = _run(diagnoses=diagnoses, raw_clusters=raw_clusters)
    assert clusters[0].primary_blame_set == ("a.b", "c.d", "e.f")


def test_backfill_empty_when_no_member_diagnoses_carry_blame() -> None:
    """If member diagnoses ALSO have empty blame_sets, backfill yields
    empty and the counter stays at 0 (counter increments only on a
    *successful* backfill)."""
    diagnoses = [_diagnosis("q1", ())]
    raw_clusters = [
        {
            "semantic_theme": "x",
            "member_qids": ["q1"],
            "unifying_evidence": "y",
            "repair_hypothesis": "z",
            "primary_blame_set": [],
            "confidence": "high",
        },
    ]
    clusters, marker = _run(diagnoses=diagnoses, raw_clusters=raw_clusters)
    assert clusters[0].primary_blame_set == ()
    # Counter increments only when the unioned result is non-empty.
    assert marker["primary_blame_set_backfilled"] == 0
