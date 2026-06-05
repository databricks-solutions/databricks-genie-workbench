"""Phase 1 P1.1 — batched Stage 3 synthesis unit tests.

Covers:
  * ``should_batch_stage3_synthesis`` predicate honours both the
    cluster-count floor and the token-budget ceiling.
  * ``_split_batched_proposals_by_cluster`` routes proposals by
    explicit ``cluster_id``, falls back to ``target_qids`` overlap,
    and drops proposals that match neither.
  * ``run_plan11_synthesis_for_all_clusters`` makes exactly ONE LLM
    call and returns one ``ClusterSynthesisResult`` per input cluster.
  * The batched path reuses the per-cluster post-processing — survival
    contract / blame-set derivation / marker emission behave the same
    as if every cluster had been called individually.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    BATCH_STAGE3_MAX_INPUT_TOKENS,
    BATCH_STAGE3_MIN_CLUSTERS,
    _split_batched_proposals_by_cluster,
    estimate_batched_stage3_input_tokens,
    run_plan11_synthesis_for_all_clusters,
    should_batch_stage3_synthesis,
)


def _make_cluster(cid: str, qid: str = "gs_009") -> FailureCluster:
    return FailureCluster(
        cluster_id=cid,
        semantic_theme=f"theme-{cid}",
        member_qids=(qid,),
        unifying_evidence="evidence",
        repair_hypothesis="hyp",
        primary_blame_set=(f"catalog.schema.t{cid}.col",),
        confidence="high",
    )


def _make_cluster_input(cid: str, qid: str = "gs_009") -> dict:
    cluster = _make_cluster(cid, qid)
    return {
        "cluster": cluster,
        "schema_slice": {},
        "member_qid_evidence": [],
        "ag_id": f"AG{cid}",
    }


# ── should_batch_stage3_synthesis ────────────────────────────────────────


def test_should_batch_returns_false_below_cluster_floor():
    inputs = [_make_cluster_input(f"H00{i}") for i in range(
        BATCH_STAGE3_MIN_CLUSTERS - 1
    )]
    assert not should_batch_stage3_synthesis(
        [
            {
                "cluster_id": ci["cluster"].cluster_id,
                "ag_id": ci["ag_id"],
                "cluster_json": ci["cluster"].to_json(),
                "schema_slice": ci["schema_slice"],
                "member_qid_evidence": ci["member_qid_evidence"],
            }
            for ci in inputs
        ],
        history=[],
    )


def test_should_batch_returns_true_at_cluster_floor_under_budget():
    inputs = [_make_cluster_input(f"H00{i}") for i in range(
        BATCH_STAGE3_MIN_CLUSTERS
    )]
    serializable = [
        {
            "cluster_id": ci["cluster"].cluster_id,
            "ag_id": ci["ag_id"],
            "cluster_json": ci["cluster"].to_json(),
            "schema_slice": ci["schema_slice"],
            "member_qid_evidence": ci["member_qid_evidence"],
        }
        for ci in inputs
    ]
    assert should_batch_stage3_synthesis(serializable, history=[])


def test_should_batch_returns_false_over_budget():
    # Build a single cluster whose member_qid_evidence is large enough
    # to single-handedly exceed the batch budget. Even three of these
    # blow past the cap.
    big_evidence = [{"big_blob": "x" * 80_000}]
    inputs = [
        {
            "cluster_id": f"H00{i}",
            "ag_id": f"AG{i}",
            "cluster_json": _make_cluster(f"H00{i}").to_json(),
            "schema_slice": {},
            "member_qid_evidence": big_evidence,
        }
        for i in range(BATCH_STAGE3_MIN_CLUSTERS)
    ]
    estimate = estimate_batched_stage3_input_tokens(inputs, history=[])
    assert estimate > BATCH_STAGE3_MAX_INPUT_TOKENS
    assert not should_batch_stage3_synthesis(inputs, history=[])


# ── _split_batched_proposals_by_cluster ──────────────────────────────────


def test_split_routes_by_explicit_cluster_id_tag():
    parsed = {
        "proposals": [
            {"cluster_id": "H001", "intent_name": "a"},
            {"cluster_id": "H002", "intent_name": "b"},
            {"cluster_id": "H001", "intent_name": "c"},
        ]
    }
    out = _split_batched_proposals_by_cluster(
        parsed,
        ["H001", "H002"],
        cluster_member_qids={"H001": set(), "H002": set()},
    )
    assert [p["intent_name"] for p in out["H001"]] == ["a", "c"]
    assert [p["intent_name"] for p in out["H002"]] == ["b"]


def test_split_falls_back_to_target_qids_overlap_when_tag_missing():
    parsed = {
        "proposals": [
            {"cluster_id": "", "target_qids": ["gs_001"], "intent_name": "a"},
            {"cluster_id": "BAD", "target_qids": ["gs_002"], "intent_name": "b"},
        ]
    }
    out = _split_batched_proposals_by_cluster(
        parsed,
        ["H001", "H002"],
        cluster_member_qids={
            "H001": {"gs_001"},
            "H002": {"gs_002"},
        },
    )
    assert [p["intent_name"] for p in out["H001"]] == ["a"]
    assert [p["intent_name"] for p in out["H002"]] == ["b"]


def test_split_drops_proposals_with_ambiguous_or_no_match():
    parsed = {
        "proposals": [
            # No tag, no target_qids → dropped
            {"intent_name": "dropped"},
            # target_qids match TWO clusters → ambiguous, dropped
            {
                "target_qids": ["gs_shared"],
                "intent_name": "ambiguous",
            },
        ]
    }
    out = _split_batched_proposals_by_cluster(
        parsed,
        ["H001", "H002"],
        cluster_member_qids={
            "H001": {"gs_shared"},
            "H002": {"gs_shared"},
        },
    )
    assert out == {"H001": [], "H002": []}


def test_split_handles_empty_parsed_output():
    out = _split_batched_proposals_by_cluster(
        None,
        ["H001"],
        cluster_member_qids={"H001": set()},
    )
    assert out == {"H001": []}


# ── run_plan11_synthesis_for_all_clusters ────────────────────────────────


def _make_batched_response(cluster_ids: list[str]) -> LlmReasoningResponse:
    """One proposal per cluster, all tagged with explicit cluster_id."""
    proposals = []
    for idx, cid in enumerate(cluster_ids):
        proposals.append(
            {
                "intent_name": f"intent_{cid}",
                "intent_description": "desc",
                "repair_hypothesis": "hyp",
                "patch_type": "add_example_sql",
                "rationale": "r",
                "confidence": "high",
                "patch_body": {
                    "example_question": f"q for {cid}",
                    "example_sql": f"SELECT * FROM t{cid} LIMIT 10",
                },
                "blame_set": [f"catalog.schema.t{cid}.col"],
                "target_qids": [f"gs_00{idx}"],
                "cluster_id": cid,
            }
        )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.BATCHED.n3.iter_1",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={"proposals": proposals},
        declined=None,
        raw_text="{...}",
        tokens_input=900,
        tokens_output=300,
        duration_ms=3000,
        error=None,
    )


@patch(
    "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
)
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_batched_synthesis_makes_one_llm_call_for_n_clusters(
    mock_marker, MockLlmCall,
):
    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"
    cluster_ids = ["H001", "H002", "H003"]
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_batched_response(cluster_ids)
    )
    inputs = [
        _make_cluster_input(cid, f"gs_00{idx}")
        for idx, cid in enumerate(cluster_ids)
    ]

    out = run_plan11_synthesis_for_all_clusters(
        inputs,
        history=[],
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    # Exactly one LLM call, three results.
    assert MockLlmCall.return_value.invoke.call_count == 1
    assert set(out.keys()) == set(cluster_ids)
    for cid in cluster_ids:
        assert out[cid].proposal is not None
        # The post-processor preserves the LLM-emitted intent_name
        # verbatim in the typed proposal that survives.
        assert out[cid].proposal["intent_name"] == f"intent_{cid}"


@patch(
    "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
)
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_batched_synthesis_declined_response_returns_envelope_per_cluster(
    mock_marker, MockLlmCall,
):
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"
    declined = LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.BATCHED.n3.iter_1",
        skill_id="plan11_synthesize",
        succeeded=False,
        parsed_output=None,
        declined=AbstainVerdict(
            reason=AbstainReason.NO_APPLICABLE_PATCH_TYPE,
            explanation="cluster unclear",
            needed_evidence=(),
            suggested_next_step="re-cluster",
        ),
        raw_text="",
        tokens_input=50,
        tokens_output=0,
        duration_ms=200,
        error=None,
    )
    MockLlmCall.return_value.invoke = MagicMock(return_value=declined)
    cluster_ids = ["H001", "H002", "H003"]
    inputs = [_make_cluster_input(cid) for cid in cluster_ids]

    out = run_plan11_synthesis_for_all_clusters(
        inputs,
        history=[],
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert set(out.keys()) == set(cluster_ids)
    for cid in cluster_ids:
        assert out[cid].proposal is None
        # The skipped_reason matches the per-cluster declined shape so
        # postmortem tooling joining on this prefix sees both paths.
        assert out[cid].skipped_reason.startswith("exception:plan11_stage3_")


def test_batched_synthesis_empty_inputs_returns_empty_map():
    out = run_plan11_synthesis_for_all_clusters(
        [],
        history=[],
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )
    assert out == {}
