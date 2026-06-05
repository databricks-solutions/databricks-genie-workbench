"""Trial 23 W6 — real partitioned re-dispatch unit tests.

Trial 22 W7 computed a token-budget partition for oversized
RCA-subcluster Stage 3 requests, emitted a marker, then issued ONE
oversized call that the LLM declined as ``prompt_too_large`` (16
declines/run in the d139 postmortem). W6 actually makes N smaller calls
(one per QID partition) and merges the proposals so the corrective
mechanism family is synthesized instead of declined.
"""
from __future__ import annotations

from genie_space_optimizer.optimization import subcluster_redispatch as sr
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)


def _resp(succeeded, proposals=None, declined=None, t_in=10, t_out=5):
    return LlmReasoningResponse(
        call_id="c",
        skill_id="plan11_synthesize",
        succeeded=succeeded,
        parsed_output=({"proposals": proposals or []} if succeeded else None),
        declined=declined,
        raw_text="{}",
        tokens_input=t_in,
        tokens_output=t_out,
        duration_ms=3,
        error=None,
    )


# ---- slice_member_evidence -----------------------------------------

def test_slice_member_evidence_filters_by_partition():
    ev = [
        {"qid": "gs_001"},
        {"qid": "gs_002"},
        {"qid": "gs_003"},
    ]
    got = sr.slice_member_evidence(ev, ("gs_001", "gs_003"))
    assert [e["qid"] for e in got] == ["gs_001", "gs_003"]


def test_slice_member_evidence_empty_inputs():
    assert sr.slice_member_evidence(None, ("gs_001",)) == []
    assert sr.slice_member_evidence([{"qid": "x"}], ()) == []


# ---- merge_subcluster_responses ------------------------------------

def test_merge_unions_proposals_from_succeeded_responses():
    r1 = _resp(True, [{"patch_type": "add_instruction"}])
    r2 = _resp(True, [{"patch_type": "add_sql_snippet_filter"}])
    merged = sr.merge_subcluster_responses(
        [r1, r2], call_id="cid", skill_id="plan11_synthesize"
    )
    assert merged.succeeded is True
    types = [p["patch_type"] for p in merged.parsed_output["proposals"]]
    assert types == ["add_instruction", "add_sql_snippet_filter"]
    assert merged.tokens_input == 20
    assert merged.tokens_output == 10
    assert merged.call_id == "cid"


def test_merge_succeeds_when_at_least_one_succeeds():
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    decline = AbstainVerdict(
        reason=AbstainReason.NO_APPLICABLE_PATCH_TYPE,
        explanation="x",
        needed_evidence=(),
        suggested_next_step="y",
    )
    r1 = _resp(False, declined=decline)
    r2 = _resp(True, [{"patch_type": "add_instruction"}])
    merged = sr.merge_subcluster_responses(
        [r1, r2], call_id="cid", skill_id="plan11_synthesize"
    )
    assert merged.succeeded is True
    assert len(merged.parsed_output["proposals"]) == 1


def test_merge_returns_decline_when_all_decline():
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    decline = AbstainVerdict(
        reason=AbstainReason.NO_APPLICABLE_PATCH_TYPE,
        explanation="x",
        needed_evidence=(),
        suggested_next_step="y",
    )
    merged = sr.merge_subcluster_responses(
        [_resp(False, declined=decline), _resp(False, declined=decline)],
        call_id="cid",
        skill_id="plan11_synthesize",
    )
    assert merged.succeeded is False
    assert merged.parsed_output is None
    assert merged.declined is not None


def test_merge_empty_responses_is_error_state():
    merged = sr.merge_subcluster_responses(
        [], call_id="cid", skill_id="plan11_synthesize"
    )
    assert merged.succeeded is False
    assert merged.error


# ---- marker ---------------------------------------------------------

# ---- synthesis wiring (forced split) -------------------------------

def _ok_response():
    return _resp(
        True,
        [
            {
                "intent_name": "x",
                "intent_description": "y",
                "repair_hypothesis": "z",
                "patch_type": "add_example_sql",
                "rationale": "r",
                "confidence": "high",
                "patch_body": {
                    "example_question": "q?",
                    "example_sql": "SELECT 1",
                },
                "blame_set": [],
                "target_qids": ["gs_001"],
            }
        ],
    )


def _subcluster_cluster():
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id="H001_subcluster_a",
        semantic_theme="theme",
        member_qids=("gs_001", "gs_002"),
        unifying_evidence="evidence",
        repair_hypothesis="hyp",
        primary_blame_set=("cat.sch.orders.amount",),
        confidence="high",
        root_cause="",
    )


def test_synthesis_makes_n_real_calls_when_split_needed(capsys, monkeypatch):
    from unittest.mock import MagicMock, patch

    # Force the size verdict to require a split and the partition to
    # yield 2 batches (both imported function-locally from the source
    # module, so patching the source attribute is picked up at call
    # time).
    import genie_space_optimizer.optimization.stage3_prompt_sizer as sizer

    monkeypatch.setattr(
        sizer,
        "slice_segments",
        lambda **kw: {
            "system_msg_tokens": 1,
            "cacheable_block_tokens": 1,
            "user_prompt_tokens": 1,
            "total_tokens": 3,
            "cap": 40000,
            "over_cap": False,
            "observe_only": False,
            "sub_cluster_split_needed": True,
        },
    )
    monkeypatch.setattr(
        sizer,
        "partition_rca_subcluster_by_token_budget",
        lambda **kw: (("gs_001",), ("gs_002",)),
    )

    invoke = MagicMock(side_effect=[_ok_response(), _ok_response()])
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_subcluster_cluster(),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    assert invoke.call_count == 2, "W6 must issue one LLM call per partition"
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1" in out
    assert '"batch_count": 2' in out


def test_synthesis_single_call_when_flag_off(capsys, monkeypatch):
    from unittest.mock import MagicMock, patch

    monkeypatch.setenv("GSO_TRIAL23_SUBCLUSTER_REAL_SLICE", "0")
    import genie_space_optimizer.optimization.stage3_prompt_sizer as sizer

    monkeypatch.setattr(
        sizer,
        "slice_segments",
        lambda **kw: {
            "system_msg_tokens": 1,
            "cacheable_block_tokens": 1,
            "user_prompt_tokens": 1,
            "total_tokens": 3,
            "cap": 40000,
            "over_cap": False,
            "observe_only": False,
            "sub_cluster_split_needed": True,
        },
    )
    invoke = MagicMock(return_value=_ok_response())
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_subcluster_cluster(),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    assert invoke.call_count == 1, "rollback: one oversized call, no split"
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1" not in out


def test_real_slice_marker_payload():
    line = sr.subcluster_real_slice_marker(
        optimization_run_id="run_x",
        iteration=2,
        cluster_id="H001_subcluster_a",
        batch_count=3,
        batch_sizes=[2, 2, 1],
        proposals_merged=4,
    )
    assert line.startswith("GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1 ")
    import json

    payload = json.loads(line.split(" ", 1)[1])
    assert payload["batch_count"] == 3
    assert payload["batch_sizes"] == [2, 2, 1]
    assert payload["proposals_merged"] == 4
    assert payload["cluster_id"] == "H001_subcluster_a"
