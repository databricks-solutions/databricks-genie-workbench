"""Plan 12 — gs_004 evidence-routing replay anchor.

This test is the replay-layer counterpart to the unit test
``tests/unit/test_plan12_gs_004_evidence_routing_wired.py``:

  * The unit test exercises ``_resolve_effective_lever_with_evidence_policy``
    in isolation against a synthesized AG dict.
  * THIS replay test drives the Plan 11 Stage 1→2→3 scaffold against
    the gs_004 fixture AND then exercises the harness wire-in directly
    against a gs_004-shaped AG, proving both halves of the contract:
      1. Stage 3 produces the add_example_sql proposal expected for
         wrong_aggregation evidence.
      2. The harness's evidence→lever policy reroutes ``target_lever=1``
         away from add_column_description for gs_004's
         wrong_aggregation evidence.

The combination defends against any future regression that splits the
two layers — a Stage-3 fix that doesn't reach the dispatcher, or a
dispatcher fix without the Stage-3 proposal shape. Either breakage
fires here.
"""
import json
from pathlib import Path

from tests.replay.conftest import (
    ReplayEmitContext,
    emit_applied_outcome,
    parse_patch_outcome_markers,
    run_single_replay_iteration,
)


ANCHOR_FIXTURE = (
    Path(__file__).parent / "active" / "fixtures" / "anchor_qids"
    / "gs_004.json"
)


def _parse_evidence_routing_marker(out: str) -> dict | None:
    for line in out.splitlines():
        if line.startswith("GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1 "):
            return json.loads(line.partition(" ")[2])
    return None


def test_gs_004_replay_stage3_to_applied_and_policy_reroutes(monkeypatch):
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
                "rca_kind_label": fx["expected_evidence_kind"],
                "observed_failure": fx["judge_rationale"],
                "generated_sql_issue": "wrong measure: SUM(quantity)/COUNT(orders)",
                "expected_sql_shape": fx["expected_repair_hypothesis"],
                "blame_set": list(fx["blame_set_expected"]),
                "evidence_summary": fx["judge_rationale"],
                "confidence": "high",
            },
        ],
    }
    cluster_payload = {
        "clusters": [
            {
                "semantic_theme": fx["expected_evidence_kind"],
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
                "intent_name": "aov_correct_measure_example",
                "intent_description": "Teach Genie the correct AOV measure",
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "patch_type": fx["expected_patch_type"],
                "rationale": (
                    "wrong_aggregation requires a generating lane "
                    "(add_example_sql); add_column_description "
                    "cannot address the failure mode"
                ),
                "confidence": "high",
                "patch_body": {
                    "example_question": (
                        "Top 5 categories by average order value last quarter?"
                    ),
                    "example_sql": fx["ground_truth_sql"],
                },
                "blame_set": list(fx["blame_set_expected"]),
                "target_qids": [qid],
            },
        ],
    }

    def _emit(proposals, ctx: ReplayEmitContext) -> None:
        assert proposals, (
            "Stage 3 must produce a generating-lane proposal for "
            "wrong_aggregation evidence"
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

    # === Half 1: Stage 3 reaches APPLIED ===========================
    outcomes = parse_patch_outcome_markers(stdout)
    assert len(outcomes) == 1
    assert outcomes[0]["outcome_kind"] == fx["expected_outcome_kind"]
    assert outcomes[0]["applied_patch_id"]

    # === Half 2: Harness wire-in reroutes Lever 1 → Lever 5 ========
    # Now exercise the production AG router's evidence-routing helper
    # against the same fixture. With the flag ON, target_lever=1 must
    # be rerouted to 5 because gs_004's evidence is wrong_aggregation.
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "1")
    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    import io
    import contextlib

    ag = {
        "id": "AG_gs_004",
        "source_cluster_ids": ["H001"],
        "asi_failure_type": fx["expected_evidence_kind"],
        "root_cause": fx["expected_evidence_kind"],
        "lever_directives": {"1": {"column_descriptions": []}},
    }

    capture = io.StringIO()
    with contextlib.redirect_stdout(capture):
        effective = _resolve_effective_lever_with_evidence_policy(
            target_lever=1,
            action_group=ag,
            optimization_run_id="run_gs_004_replay",
            iteration=1,
            ag_id="AG_gs_004",
            cluster_id="H001",
        )

    assert effective == 5, (
        f"gs_004 wrong_aggregation MUST route away from Lever 1; "
        f"got effective={effective}"
    )
    marker = _parse_evidence_routing_marker(capture.getvalue())
    assert marker is not None
    assert marker["evidence_kind"] == fx["expected_evidence_kind"]
    assert marker["target_lever_before"] == 1
    assert marker["target_lever_after"] == 5
    assert marker["reroute_applied"] is True
