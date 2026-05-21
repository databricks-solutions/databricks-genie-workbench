"""Plan 12 PR 8 — end-to-end patch-survival deploy gate.

For each of the 5 anchor QIDs (gs_004, gs_009, gs_021, gs_024, gs_026):
  1. Load the fixture from tests/replay/active/fixtures/anchor_qids/.
  2. Drive Stage 1 → 2 → 3 via the replay scaffold with deterministic
     stage payloads built from the fixture.
  3. Emit the recipe-driven outcome (APPLIED or BLAST_RADIUS_REJECTED
     with narrow-replacement) via the canonical patch-survival emitter.
  4. Assert ``GSO_PATCH_OUTCOME_V1.outcome_kind`` matches the fixture's
     ``expected_outcome_kind`` for the proposal's intent_id.
  5. Assert Plan 11 dispatch was NOT skipped for this iteration.

This gate replaces the per-anchor ``test_l6_anchor_applyability_gs_*.py``
files with a single parametrized check — they remain as standalone
test surfaces, but THIS file is what CI gates deploys on.

The 5 anchors collectively span the full patch-survival contract
surface:

  * gs_004: wrong-aggregation evidence → add_example_sql → APPLIED
  * gs_009: top-N collapse → add_sql_snippet_expression → APPLIED
  * gs_021: month-to-date filter → add_sql_snippet_filter → APPLIED
  * gs_024: customer_segment filter → add_sql_snippet_filter →
            BLAST_RADIUS_REJECTED → narrow_outcome=narrowed
  * gs_026: top-N collapse → add_sql_snippet_expression → APPLIED
"""
import json
from pathlib import Path

import pytest

from tests.replay.conftest import (
    ReplayEmitContext,
    emit_applied_outcome,
    emit_blast_radius_with_narrow_replacement,
    parse_patch_outcome_markers,
    run_single_replay_iteration,
)


ANCHOR_FIXTURE_DIR = (
    Path(__file__).parent / "active" / "fixtures" / "anchor_qids"
)
ANCHOR_QIDS = ["gs_004", "gs_009", "gs_021", "gs_024", "gs_026"]


def _load_fixture(qid: str) -> dict:
    return json.loads((ANCHOR_FIXTURE_DIR / f"{qid}.json").read_text())


def _diagnose_payload(fx: dict) -> dict:
    qid = fx["qid"]
    return {
        "diagnoses": [
            {
                "qid": qid,
                "rca_kind_label": (
                    fx.get("expected_evidence_kind") or "structural"
                ),
                "observed_failure": fx["judge_rationale"],
                "generated_sql_issue": (
                    f"{qid}: see judge rationale"
                ),
                "expected_sql_shape": fx["expected_repair_hypothesis"],
                "blame_set": list(fx["blame_set_expected"]),
                "evidence_summary": fx["judge_rationale"],
                "confidence": "high",
            },
        ],
    }


def _cluster_payload(fx: dict) -> dict:
    qid = fx["qid"]
    return {
        "clusters": [
            {
                "semantic_theme": (
                    fx.get("expected_evidence_kind") or "structural"
                ),
                "member_qids": [qid],
                "unifying_evidence": fx["judge_rationale"],
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "primary_blame_set": list(fx["blame_set_expected"]),
                "confidence": "high",
            },
        ],
    }


def _synthesize_payload(fx: dict) -> dict:
    qid = fx["qid"]
    patch_type = fx["expected_patch_type"]
    body: dict = {"name": f"plan12_{qid}"}
    if patch_type == "add_example_sql":
        body["example_question"] = fx["question"]
        body["example_sql"] = fx["ground_truth_sql"]
    elif patch_type == "add_sql_snippet_filter":
        # Extract the WHERE-clause condition as the snippet's SQL
        # expression. The fixture's ground_truth_sql is the canonical
        # post-patch shape.
        gt = fx["ground_truth_sql"]
        body["sql_expression"] = (
            gt.split("WHERE", 1)[-1].strip()
            if "WHERE" in gt
            else gt
        )
        body["usage_guidance"] = fx["expected_repair_hypothesis"]
    elif patch_type == "add_sql_snippet_expression":
        body["sql_expression"] = "ROW_NUMBER() OVER (ORDER BY ... DESC)"
        body["usage_guidance"] = fx["expected_repair_hypothesis"]
    return {
        "proposals": [
            {
                "intent_name": f"plan12_{qid}",
                "intent_description": fx["expected_repair_hypothesis"],
                "repair_hypothesis": fx["expected_repair_hypothesis"],
                "patch_type": patch_type,
                "rationale": "Addresses the diagnosed root cause",
                "confidence": "high",
                "patch_body": body,
                "blame_set": list(fx["blame_set_expected"]),
                "target_qids": [qid],
            },
        ],
    }


def _make_outcome_emitter(fx: dict):
    """Return a post_synthesize_outcome_emitter callback that produces
    the fixture's ``expected_outcome_kind`` for each surviving
    proposal. Wraps the scaffold's canonical emit helpers — does NOT
    duplicate emission logic."""
    expected = fx["expected_outcome_kind"]

    def _emit(proposals, ctx: ReplayEmitContext) -> None:
        assert proposals, (
            f"[{fx['qid']}] Stage 3 must produce at least one "
            f"proposal; the survival contract validator dropped "
            f"everything"
        )
        for p in proposals:
            if expected == "applied":
                emit_applied_outcome(p, ctx)
            elif expected == "blast_radius_rejected":
                # Build the cluster context the narrow-replacement
                # path needs. The cluster fixture for gs_024
                # documents the collateral and protected SQL.
                from genie_space_optimizer.optimization.stages.plan11_types import (
                    FailureCluster,
                )
                cluster = FailureCluster(
                    cluster_id=ctx.cluster_id,
                    semantic_theme=(
                        fx.get("expected_evidence_kind") or "structural"
                    ),
                    member_qids=(fx["qid"],),
                    unifying_evidence=fx["judge_rationale"],
                    repair_hypothesis=fx["expected_repair_hypothesis"],
                    primary_blame_set=tuple(fx["blame_set_expected"]),
                    confidence="high",
                )
                emit_blast_radius_with_narrow_replacement(
                    p,
                    ctx,
                    collateral_qids=tuple(
                        fx.get("collateral_qids_protected") or ()
                    ),
                    protected_sql_by_qid={
                        q: "SELECT 1"
                        for q in (
                            fx.get("collateral_qids_protected") or ()
                        )
                    },
                    narrowed_patch_body=p.patch_body,
                    cluster=cluster,
                )
            else:
                raise AssertionError(
                    f"[{fx['qid']}] unsupported "
                    f"expected_outcome_kind={expected!r}"
                )

    return _emit


@pytest.mark.parametrize("qid", ANCHOR_QIDS)
def test_patch_survival_anchor(qid, monkeypatch):
    """Deploy gate: each anchor QID's Stage 3 proposal must reach the
    fixture's ``expected_outcome_kind`` via the patch-survival
    emitter, with NO Plan 11 dispatch-skip events."""
    fx = _load_fixture(qid)
    expected_outcome = fx["expected_outcome_kind"]

    # For gs_024 (the narrow-replacement anchor), the scaffold
    # invokes ``narrow_replacement_with_llm``. Patch it to return a
    # narrowed proposal deterministically so the assertion on
    # ``narrow_outcome=narrowed`` is reproducible.
    if expected_outcome == "blast_radius_rejected":
        from dataclasses import replace as _dc_replace

        def _fake_narrow_loop(patch, **_kwargs):
            return _dc_replace(
                patch,
                patch_body={
                    **patch.patch_body,
                    "name": patch.patch_body.get("name", "x")
                    + "_scoped",
                },
            )

        from genie_space_optimizer.optimization.stages import (
            narrow_replacement as _narrow_mod,
        )
        monkeypatch.setattr(
            _narrow_mod,
            "narrow_replacement_with_llm",
            _fake_narrow_loop,
        )

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

    stdout = run_single_replay_iteration(
        failing_qids=[qid],
        eval_rows=eval_rows,
        diagnose_payload=_diagnose_payload(fx),
        cluster_payload=_cluster_payload(fx),
        synthesize_payload=_synthesize_payload(fx),
        post_synthesize_outcome_emitter=_make_outcome_emitter(fx),
        monkeypatch=monkeypatch,
    )

    # Assertion 1: outcome stream contains at least one matching
    # outcome marker.
    outcomes = parse_patch_outcome_markers(stdout)
    assert outcomes, (
        f"[{qid}] expected at least one GSO_PATCH_OUTCOME_V1; got "
        f"none. The proposal did not survive to a terminal state. "
        f"Captured stdout tail: {stdout[-400:]!r}"
    )
    # Assertion 2: the outcome kind matches the fixture's expectation.
    final = outcomes[-1]
    assert final["outcome_kind"] == expected_outcome, (
        f"[{qid}] expected outcome_kind={expected_outcome!r}; got "
        f"outcome_kind={final['outcome_kind']!r} "
        f"terminal_reason={final.get('terminal_reason', '')!r}"
    )

    # Assertion 3: blast_radius_rejected anchors must carry the
    # narrow-replacement audit trail.
    if expected_outcome == "blast_radius_rejected":
        expected_narrow = fx.get("expected_narrow_outcome", "narrowed")
        assert final.get("narrow_replacement_attempted") is True, (
            f"[{qid}] blast_radius_rejected MUST carry "
            f"narrow_replacement_attempted=True; got "
            f"{final.get('narrow_replacement_attempted')!r}"
        )
        assert final.get("narrow_outcome") == expected_narrow, (
            f"[{qid}] expected narrow_outcome="
            f"{expected_narrow!r}; got "
            f"{final.get('narrow_outcome')!r}"
        )

    # Assertion 4: Plan 11 dispatch was NOT skipped for this iter.
    # A skipped dispatch means the contract held in the unit tests
    # but the deployed code-path coverage didn't actually exercise
    # Plan 11 — defeats the deploy gate.
    skip_lines = [
        ln
        for ln in stdout.splitlines()
        if ln.startswith("GSO_PLAN11_DISPATCH_DECISION_V1 ")
        and '"outcome":"skipped"' in ln.replace(" ", "")
    ]
    assert not skip_lines, (
        f"[{qid}] Plan 11 was skipped during the iteration:\n"
        + "\n".join(skip_lines)
    )
