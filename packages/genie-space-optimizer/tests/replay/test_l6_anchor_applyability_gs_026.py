"""Plan 12 deferred — gs_026 L6 applyability deploy gate.

Same shape as gs_009 (RANK→ROW_NUMBER pattern, slightly different
fixture metadata); asserts APPLIED outcome via the scaffold.
"""
import json
from pathlib import Path

from tests.replay.conftest import (
    ReplayEmitContext,
    emit_applied_outcome,
    parse_patch_outcome_markers,
    run_single_replay_iteration,
)


ANCHOR_FIXTURE = Path(__file__).parent / "active/fixtures/anchor_qids/gs_026.json"


def test_gs_026_proposal_survives_to_applied(monkeypatch):
    fx = json.loads(ANCHOR_FIXTURE.read_text())
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
                "rca_kind_label": "top-N collapsed (RANK without LIMIT)",
                "observed_failure": fx["judge_rationale"],
                "generated_sql_issue": "RANK returns all rows ranked",
                "expected_sql_shape": "ROW_NUMBER() with LIMIT 5",
                "blame_set": list(fx["blame_set_expected"]),
                "evidence_summary": fx["judge_rationale"],
                "confidence": "high",
            },
        ],
    }
    cluster_payload = {
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
    synthesize_payload = {
        "proposals": [
            {
                "intent_name": "row_number_top_n_categories",
                "intent_description": "Replace RANK with ROW_NUMBER + LIMIT 5",
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "patch_type": fx["expected_patch_type"],
                "rationale": "Addresses the diagnosed top-N collapse",
                "confidence": "high",
                "patch_body": {
                    "name": "top_5_categories_by_revenue",
                    "sql_expression": (
                        "ROW_NUMBER() OVER (ORDER BY SUM(revenue) DESC) AS rn"
                    ),
                    "usage_guidance": "use with LIMIT 5",
                },
                "blame_set": list(fx["blame_set_expected"]),
                "target_qids": [qid],
            },
        ],
    }

    def _emit(proposals, ctx: ReplayEmitContext) -> None:
        assert proposals
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
