"""Plan 12 deferred — gs_021 L6 applyability deploy gate.

Scaffold drives Stage 1 → 2 → 3 with mocked LLMs that emit the
MTD-filter proposal; assert APPLIED outcome for the resulting
intent.
"""
import json
from pathlib import Path

from tests.replay.conftest import (
    ReplayEmitContext,
    emit_applied_outcome,
    parse_patch_outcome_markers,
    run_single_replay_iteration,
)


ANCHOR_FIXTURE = Path(__file__).parent / "active/fixtures/anchor_qids/gs_021.json"


def _fixture() -> dict:
    return json.loads(ANCHOR_FIXTURE.read_text())


def test_gs_021_mtd_filter_proposal_applies(monkeypatch):
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

    diagnose_payload = {
        "diagnoses": [
            {
                "qid": qid,
                "rca_kind_label": "trailing-30 instead of MTD",
                "observed_failure": fx["judge_rationale"],
                "generated_sql_issue": "CURRENT_DATE - INTERVAL 30 DAY",
                "expected_sql_shape": "DATE_TRUNC('month', CURRENT_DATE)",
                "blame_set": list(fx["blame_set_expected"]),
                "evidence_summary": fx["judge_rationale"],
                "confidence": "high",
            },
        ],
    }
    cluster_payload = {
        "clusters": [
            {
                "semantic_theme": "month-to-date filter",
                "member_qids": [qid],
                "unifying_evidence": fx["judge_rationale"],
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "primary_blame_set": list(fx["blame_set_expected"]),
                "confidence": "high",
            },
        ],
    }
    synthesize_payload = {
        "proposals": [
            {
                "intent_name": "mtd_revenue_filter",
                "intent_description": "Filter to month-to-date using DATE_TRUNC",
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "patch_type": fx["expected_patch_type"],
                "rationale": "Addresses the trailing-30 misuse",
                "confidence": "high",
                "patch_body": {
                    "name": "mtd_filter",
                    "sql_expression": (
                        "order_date >= DATE_TRUNC('month', CURRENT_DATE)"
                    ),
                    "usage_guidance": "use for month-to-date aggregations",
                },
                "blame_set": list(fx["blame_set_expected"]),
                "target_qids": [qid],
            },
        ],
    }

    def _emit(proposals, ctx: ReplayEmitContext) -> None:
        assert proposals, "Stage 3 must produce a proposal for gs_021"
        for p in proposals:
            emit_applied_outcome(p, ctx)

    stdout = run_single_replay_iteration(
        failing_qids=[qid],
        eval_rows=eval_rows,
        diagnose_payload=diagnose_payload,
        cluster_payload=cluster_payload,
        synthesize_payload=synthesize_payload,
        post_synthesize_outcome_emitter=_emit,
        monkeypatch=monkeypatch,
    )

    outcomes = parse_patch_outcome_markers(stdout)
    assert len(outcomes) == 1
    assert outcomes[0]["outcome_kind"] == fx["expected_outcome_kind"]
    assert outcomes[0]["applied_patch_id"]
