"""Plan 12 deferred — gs_009 L6 applyability deploy gate.

Drives the Plan 11 Stage 1 → 2 → 3 chain with a deterministic mock
LLM that emits the expected ROW_NUMBER-top-N proposal, then asserts
that the post-synthesize outcome stream contains exactly one
``GSO_PATCH_OUTCOME_V1`` with ``outcome_kind=applied`` for the
proposal's ``intent_id``.

The applier APPLIED emission is provided by the scaffold's
``emit_applied_outcome`` helper (the stand-in for PR 3's deferred
production wire-in at the L6 applier callsite).
"""
import json
from pathlib import Path

from tests.replay.conftest import (
    ReplayEmitContext,
    emit_applied_outcome,
    parse_patch_outcome_markers,
    run_single_replay_iteration,
)


ANCHOR_FIXTURE = Path(__file__).parent / "active/fixtures/anchor_qids/gs_009.json"


def _fixture() -> dict:
    return json.loads(ANCHOR_FIXTURE.read_text())


def _diagnose_payload(qid: str, fx: dict) -> dict:
    return {
        "diagnoses": [
            {
                "qid": qid,
                "rca_kind_label": "top-N collapsed (RANK without LIMIT)",
                "observed_failure": fx["judge_rationale"],
                "generated_sql_issue": "RANK() returns all rows tied at rank 1+",
                "expected_sql_shape": "ROW_NUMBER() with LIMIT 10",
                "blame_set": list(fx["blame_set_expected"]),
                "evidence_summary": fx["judge_rationale"],
                "confidence": "high",
            },
        ],
    }


def _cluster_payload(qid: str, fx: dict) -> dict:
    return {
        "clusters": [
            {
                "semantic_theme": "top-N row limit failures",
                "member_qids": [qid],
                "unifying_evidence": fx["judge_rationale"],
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "primary_blame_set": list(fx["blame_set_expected"]),
                "confidence": "high",
            },
        ],
    }


def _synthesize_payload(qid: str, fx: dict) -> dict:
    return {
        "proposals": [
            {
                "intent_name": "row_number_top_n",
                "intent_description": "Replace RANK with ROW_NUMBER and LIMIT 10",
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "patch_type": fx["expected_patch_type"],
                "rationale": "Addresses the diagnosed root cause",
                "confidence": "high",
                "patch_body": {
                    "name": "top_10_customers_by_order_count",
                    "sql_expression": (
                        "ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn"
                    ),
                    "usage_guidance": "use with LIMIT 10",
                },
                "blame_set": list(fx["blame_set_expected"]),
                "target_qids": [qid],
            },
        ],
    }


def test_gs_009_proposal_survives_to_applied(monkeypatch):
    fx = _fixture()
    qid = fx["qid"]
    eval_rows = [
        {
            "question_id": qid,
            "question": fx["question"],
            "ground_truth_sql": fx["ground_truth_sql"],
            "generated_sql": fx["generated_sql"],
            "judge_rationale": fx["judge_rationale"],
            "score": fx["score"],
        },
    ]

    def _emit(proposals, ctx: ReplayEmitContext) -> None:
        assert proposals, (
            "Stage 3 must produce at least one proposal for gs_009; "
            "the survival contract validator dropped everything"
        )
        for p in proposals:
            emit_applied_outcome(p, ctx)

    stdout = run_single_replay_iteration(
        failing_qids=[qid],
        eval_rows=eval_rows,
        diagnose_payload=_diagnose_payload(qid, fx),
        cluster_payload=_cluster_payload(qid, fx),
        synthesize_payload=_synthesize_payload(qid, fx),
        post_synthesize_outcome_emitter=_emit,
        monkeypatch=monkeypatch,
    )

    outcomes = parse_patch_outcome_markers(stdout)
    assert len(outcomes) == 1, (
        f"expected exactly one GSO_PATCH_OUTCOME_V1; got {outcomes!r}"
    )
    o = outcomes[0]
    assert o["outcome_kind"] == fx["expected_outcome_kind"]
    assert o["intent_id"].startswith("H")  # Stage 2 stamps H001 cluster_id
    assert o["applied_patch_id"]
