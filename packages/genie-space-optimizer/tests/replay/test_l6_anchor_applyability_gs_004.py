"""Plan 12 deferred — gs_004 L6 applyability deploy gate.

gs_004 (wrong-aggregation evidence) was routed to Lever 1
(non-generating add_column_description) in the original postmortem
dc89d1a9-... The scaffold bypasses the production AG router and
drives Stage 1 → 2 → 3 directly with Stage 3 producing the
``add_example_sql`` patch the new evidence-kind policy (PR 6)
prefers. This test verifies the contract at the per-intent level;
the separate gs_004-routes-to-l5b unit test in
``test_plan12_gs_004_routes_to_l5b_not_l1.py`` verifies the
``_apply_evidence_to_lever_policy`` helper that PR 6's deferred
harness wire-in will call.

When the harness wire-in lands and this scaffold is upgraded to
drive the production iteration loop, the assertions here remain
correct (the proposal still ends in APPLIED) — only the path from
evidence → Stage 3 will be exercised through the production AG
router instead of synthetic.
"""
import json
from pathlib import Path

from tests.replay.conftest import (
    ReplayEmitContext,
    emit_applied_outcome,
    parse_patch_outcome_markers,
    run_single_replay_iteration,
)


ANCHOR_FIXTURE = Path(__file__).parent / "active/fixtures/anchor_qids/gs_004.json"


def test_gs_004_routes_to_l5b_and_applies(monkeypatch):
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
                "rca_kind_label": "wrong_aggregation (quantity vs revenue, COUNT vs COUNT DISTINCT)",
                "observed_failure": fx["judge_rationale"],
                "generated_sql_issue": "SUM(quantity)/COUNT(orders) does not compute AOV",
                "expected_sql_shape": "SUM(revenue)/COUNT(DISTINCT order_id)",
                "blame_set": list(fx["blame_set_expected"]),
                "evidence_summary": fx["judge_rationale"],
                "confidence": "high",
            },
        ],
    }
    cluster_payload = {
        "clusters": [
            {
                "semantic_theme": "wrong_aggregation",
                "member_qids": [qid],
                "unifying_evidence": fx["judge_rationale"],
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "primary_blame_set": list(fx["blame_set_expected"]),
                "confidence": "high",
            },
        ],
    }
    # The PR 6 policy refuses Lever 1 for wrong_aggregation; Stage 3
    # should produce an add_example_sql proposal teaching the correct
    # AOV measure.
    synthesize_payload = {
        "proposals": [
            {
                "intent_name": "aov_correct_measure_example",
                "intent_description": "Teach Genie the correct AOV measure",
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "patch_type": fx["expected_patch_type"],
                "rationale": "wrong_aggregation requires a generating lane",
                "confidence": "high",
                "patch_body": {
                    "example_question": "Top 5 categories by average order value last quarter?",
                    "example_sql": (
                        "SELECT category, "
                        "SUM(revenue) / COUNT(DISTINCT order_id) AS aov "
                        "FROM catalog.schema.orders "
                        "GROUP BY category ORDER BY aov DESC LIMIT 5"
                    ),
                },
                "blame_set": list(fx["blame_set_expected"]),
                "target_qids": [qid],
            },
        ],
    }

    def _emit(proposals, ctx: ReplayEmitContext) -> None:
        assert proposals, (
            "Stage 3 must produce a generating-lane proposal for "
            "wrong_aggregation evidence — the survival contract "
            "would have dropped it otherwise"
        )
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
