"""Plan 12 deferred — gs_024 blast-radius narrow-replacement gate.

The Stage 3 proposal (broad customer_segment filter) collides with a
protected collateral QID (gs_003). The scaffold exercises the PR 4
``narrow_replacement_from_drop_record`` wrapper end-to-end:

  1. Build a :class:`BlastRadiusDropRecord` from the proposal.
  2. Dispatch to :func:`narrow_replacement_with_llm` (patched to return
     a scoped variant).
  3. Emit ``GSO_PATCH_OUTCOME_V1`` with
     ``outcome_kind=blast_radius_rejected``,
     ``narrow_replacement_attempted=True``,
     ``narrow_outcome="narrowed"``.

Asserts the marker shape matches the contract — the narrow-replacement
wrapper has been called and produced a usable patch.
"""
import json
from pathlib import Path

from tests.replay.conftest import (
    ReplayEmitContext,
    emit_blast_radius_with_narrow_replacement,
    parse_patch_outcome_markers,
    run_single_replay_iteration,
)


ANCHOR_FIXTURE = Path(__file__).parent / "active/fixtures/anchor_qids/gs_024.json"


def _fixture() -> dict:
    return json.loads(ANCHOR_FIXTURE.read_text())


def test_gs_024_narrow_replacement_succeeds(monkeypatch):
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
                "rca_kind_label": "missing customer_segment filter",
                "observed_failure": fx["judge_rationale"],
                "generated_sql_issue": "no customer_segment predicate",
                "expected_sql_shape": "customer_segment = 'returning'",
                "blame_set": list(fx["blame_set_expected"]),
                "evidence_summary": fx["judge_rationale"],
                "confidence": "high",
            },
        ],
    }
    cluster_payload = {
        "clusters": [
            {
                "semantic_theme": "segment filter missing",
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
                "intent_name": "returning_customer_filter",
                "intent_description": "Filter to returning customer segment",
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "patch_type": fx["expected_patch_type"],
                "rationale": "Addresses missing segment predicate",
                "confidence": "high",
                "patch_body": {
                    "name": "returning_customer_filter",
                    "sql_expression": "customer_segment = 'returning'",
                    "usage_guidance": "use for returning-customer questions",
                },
                "blame_set": list(fx["blame_set_expected"]),
                "target_qids": [qid],
            },
        ],
    }

    # Patch the narrow-replacement loop to return a scoped variant —
    # simulates the LLM producing a narrower patch_body that no
    # longer collides with the protected collateral SQL.
    def _fake_narrow_loop(patch, **kwargs):
        from dataclasses import replace
        return replace(
            patch,
            patch_body={
                **patch.patch_body,
                "name": patch.patch_body.get("name", "x") + "_scoped",
                "sql_expression": (
                    patch.patch_body.get("sql_expression", "")
                    + " AND order_date >= DATEADD(quarter, -1, CURRENT_DATE)"
                ),
                "usage_guidance": (
                    patch.patch_body.get("usage_guidance", "")
                    + " (scoped to last quarter)"
                ),
            },
        )

    from genie_space_optimizer.optimization.stages import (
        narrow_replacement as _narrow_mod,
    )
    monkeypatch.setattr(
        _narrow_mod, "narrow_replacement_with_llm", _fake_narrow_loop,
    )

    def _emit(proposals, ctx: ReplayEmitContext) -> None:
        assert proposals, "Stage 3 must produce a proposal for gs_024"
        from genie_space_optimizer.optimization.stages.plan11_types import (
            FailureCluster,
        )
        cluster = FailureCluster(
            cluster_id=ctx.cluster_id,
            semantic_theme="segment filter missing",
            member_qids=(qid,),
            unifying_evidence=fx["judge_rationale"],
            repair_hypothesis=fx["expected_repair_hypothesis"],
            primary_blame_set=tuple(fx["blame_set_expected"]),
            confidence="high",
        )
        for p in proposals:
            emit_blast_radius_with_narrow_replacement(
                p,
                ctx,
                collateral_qids=tuple(fx["collateral_qids_protected"]),
                protected_sql_by_qid={
                    cid: "SELECT * FROM orders LIMIT 5"
                    for cid in fx["collateral_qids_protected"]
                },
                narrowed_patch_body=None,  # the helper uses the loop's result
                cluster=cluster,
            )

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
    o = outcomes[0]
    assert o["outcome_kind"] == fx["expected_outcome_kind"]
    assert o["narrow_replacement_attempted"] is True
    assert o["narrow_outcome"] == fx["expected_narrow_outcome"]
    assert list(o["collateral_qids"]) == fx["collateral_qids_protected"]
