"""Trial 13g — Stage 3 effective_blame_set 3-step fallback chain.

The chain (in priority order):

  1. LLM-emitted ``blame_set`` per proposal.
  2. ``cluster.primary_blame_set``.
  3. Union of ``member_qid_evidence[*].blame_set`` (Stage 1 seeds).
  4. ``empty`` (proposal will be rejected by survival contract).

The 98ec production replay run revealed the chain was only 2 steps
deep, so when Stage 2's clustering LLM AND the Stage 3 LLM both
omitted blame_set, the proposal ended up with ``blame_set=()`` even
though Stage 1 typed evidence carried plenty of blame data. This
test pins the third step plus the closed-vocabulary
``proposals_blame_set_source`` map on the Stage 3 marker.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _make_cluster(
    *,
    primary_blame_set: tuple[str, ...] = (),
) -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="top-N failures",
        member_qids=("gs_009",),
        unifying_evidence="RANK() doesn't bound row count",
        repair_hypothesis="Use ROW_NUMBER() and LIMIT 10",
        primary_blame_set=primary_blame_set,
        confidence="high",
    )


def _response(
    *,
    proposal_blame_set: list[str],
    patch_type: str = "add_example_sql",
) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_1",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Use ROW_NUMBER pattern",
                    "intent_description": "Bound result rows to N",
                    "repair_hypothesis": "ROW_NUMBER + LIMIT",
                    "patch_type": patch_type,
                    "rationale": "Demonstrates pattern",
                    "confidence": "high",
                    "patch_body": {
                        "example_question": "Top 10 orders?",
                        "example_sql": "SELECT * FROM orders LIMIT 10",
                    },
                    "blame_set": list(proposal_blame_set),
                    "target_qids": ["gs_009"],
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=200,
        tokens_output=100,
        duration_ms=2100,
        error=None,
    )


def _run(
    *,
    response: LlmReasoningResponse,
    cluster: FailureCluster,
    member_qid_evidence: list[dict] | None,
):
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockLlmCall, patch(
        "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
    ) as mock_marker:
        mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"
        MockLlmCall.return_value.invoke = MagicMock(return_value=response)
        result = run_plan11_synthesis_for_single_cluster(
            cluster=cluster,
            schema_slice={},
            history=[],
            member_qid_evidence=member_qid_evidence,
            optimization_run_id="run_t13g",
            iteration=1,
            ag_id="AG_H001",
            w=MagicMock(),
        )
        return result, mock_marker


def test_llm_emitted_blame_set_attributed_to_llm() -> None:
    """Step 1: LLM emits a blame_set — source map records 'llm'."""
    result, mock_marker = _run(
        response=_response(
            proposal_blame_set=["main.sales.orders.order_id"],
        ),
        cluster=_make_cluster(primary_blame_set=()),
        member_qid_evidence=None,
    )
    assert result.proposal is not None
    assert result.proposal["blame_set"] == ["main.sales.orders.order_id"]
    source = mock_marker.call_args[1]["proposals_blame_set_source"]
    assert source == {"llm": 1}


def test_cluster_primary_blame_set_fallback_attributed_to_cluster() -> None:
    """Step 2: LLM empty → use cluster.primary_blame_set; source = 'cluster'."""
    result, mock_marker = _run(
        response=_response(proposal_blame_set=[]),
        cluster=_make_cluster(
            primary_blame_set=("main.sales.orders.order_id",),
        ),
        member_qid_evidence=None,
    )
    assert result.proposal is not None
    assert result.proposal["blame_set"] == ["main.sales.orders.order_id"]
    source = mock_marker.call_args[1]["proposals_blame_set_source"]
    assert source == {"cluster": 1}


def test_member_union_fallback_attributed_to_member_union() -> None:
    """Step 3: LLM empty AND cluster empty → union member_qid_evidence;
    source = 'member_union'. This is the Trial 13g safety net."""
    member_qid_evidence = [
        {
            "qid": "gs_009",
            "blame_set": ["main.sales.orders.order_id"],
            "observed_failure": "row count mismatch",
            "expected_sql_shape": "LIMIT N",
            "confidence": "high",
        },
    ]
    result, mock_marker = _run(
        response=_response(proposal_blame_set=[]),
        cluster=_make_cluster(primary_blame_set=()),
        member_qid_evidence=member_qid_evidence,
    )
    assert result.proposal is not None
    assert result.proposal["blame_set"] == ["main.sales.orders.order_id"]
    source = mock_marker.call_args[1]["proposals_blame_set_source"]
    assert source == {"member_union": 1}


def test_member_union_reads_nested_diagnosis_key() -> None:
    """The SM transformer nests blame_set under ``diagnosis`` as well —
    the union helper accepts either shape."""
    member_qid_evidence = [
        {
            "qid": "gs_009",
            "diagnosis": {
                "blame_set": ["main.sales.orders.order_id"],
            },
        },
    ]
    result, mock_marker = _run(
        response=_response(proposal_blame_set=[]),
        cluster=_make_cluster(primary_blame_set=()),
        member_qid_evidence=member_qid_evidence,
    )
    assert result.proposal is not None
    assert result.proposal["blame_set"] == ["main.sales.orders.order_id"]


def test_all_sources_empty_attributed_to_empty_and_drops_proposal() -> None:
    """Step 4: every source is empty → source map records 'empty' and
    the proposal is then dropped by the Plan 12 survival contract
    (no target_objects derivable). Note: the marker is emitted with
    the 'empty' source BEFORE survival validation, so postmortems
    still get observability on the cause."""
    result, mock_marker = _run(
        response=_response(proposal_blame_set=[]),
        cluster=_make_cluster(primary_blame_set=()),
        member_qid_evidence=[],
    )
    # Survival contract drops the empty-target proposal.
    assert result.proposal is None
    source = mock_marker.call_args[1]["proposals_blame_set_source"]
    assert source == {"empty": 1}


def test_member_union_dedup_and_arrival_order() -> None:
    """Multiple member entries — union dedupes by string and preserves
    arrival order (the helper is deterministic)."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        _union_member_blame_sets,
    )
    out = _union_member_blame_sets([
        {"blame_set": ["a.b", "c.d"]},
        {"blame_set": ["c.d", "e.f"]},
        {"diagnosis": {"blame_set": ["a.b", "g.h"]}},
    ])
    assert out == ["a.b", "c.d", "e.f", "g.h"]


def test_member_union_empty_input_returns_empty() -> None:
    """No member_qid_evidence → empty list (caller treats as 'empty')."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        _union_member_blame_sets,
    )
    assert _union_member_blame_sets(None) == []
    assert _union_member_blame_sets([]) == []
    assert _union_member_blame_sets([{"qid": "x"}, {"blame_set": []}]) == []
