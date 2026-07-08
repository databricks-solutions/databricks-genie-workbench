"""GSO v2 Phase 9 — ``publish_and_audit`` real body (champion publish + LLM audit).

``optimization/publish.py`` replaces the Phase-7 shell. Phase 9:
  * READS the STAMPED ``terminal_reason`` off the champion iteration row — never
    re-derives it from accuracy (the Phase-7 shell's collapse bug);
  * GATES publish on the reason: ``{TARGET_REACHED, MAX_ATTEMPTS}`` publish via
    the idempotent Delta-only ``promote_best_model``; everything else does NOT
    publish but still writes a ``publish_record`` carrying concerns;
  * stamps a REUSED terminal status (CONVERGED / MAX_ITERATIONS / FAILED /
    STALLED) — NOT a new PUBLISHED_AUDITED status (arch §13.3, deferred);
  * writes a best-effort LLM audit summary over a LEAK-FREE structural context
    (no benchmark Q/A or ground-truth SQL — §3.6); a summary failure is non-fatal.

Mirrors the MagicMock-Spark / dependency-patch harness style of
``test_phase8_loop_state_persistence.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from genie_space_optimizer.optimization import publish as P


# ── fixtures / builders ─────────────────────────────────────────────────────


def _run_row(**over) -> dict:
    base = {
        "run_id": "run1",
        "space_id": "space-abc",
        "best_iteration": None,
        "best_accuracy": None,
    }
    base.update(over)
    return base


def _full_iters(*, champion_reason: str | None, champion_accuracy: float = 91.0) -> list[dict]:
    """baseline (0) → coverage (1) → surgical champion (2) full rows."""
    return [
        {
            "iteration": 0, "eval_scope": "full", "rolled_back": False,
            "overall_accuracy": 80.0, "attempt_no": None, "attempt_mode": None,
            "decision": None, "is_champion": False, "terminal_reason": None,
            "remaining_failures": "[]",
        },
        {
            "iteration": 1, "eval_scope": "full", "rolled_back": False,
            "overall_accuracy": 84.0, "attempt_no": 1, "attempt_mode": "coverage",
            "decision": "accept", "is_champion": False, "terminal_reason": None,
            "best_accuracy": 84.0, "remaining_failures": "[]",
        },
        {
            "iteration": 2, "eval_scope": "full", "rolled_back": False,
            "overall_accuracy": champion_accuracy, "attempt_no": 2,
            "attempt_mode": "surgical", "decision": "accept", "is_champion": True,
            "terminal_reason": champion_reason, "best_accuracy": champion_accuracy,
            "best_config_version_id": "cfg-v2", "surgical_attempts_used": 1,
            "target_accuracy": 90.0, "max_attempts": 3,
            "remaining_failures": '["q5", "q9"]',
        },
    ]


def _patches_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"iteration": 2, "lever": 1, "patch_type": "update_column", "rolled_back": False},
        {"iteration": 2, "lever": 6, "patch_type": "add_sql_expression", "rolled_back": False},
        {"iteration": 2, "lever": 5, "patch_type": "add_instruction", "rolled_back": True},
    ])


def _provenance_df() -> pd.DataFrame:
    """Provenance with the leakage-sensitive columns POPULATED, to prove the
    serializer never forwards them into the prompt context."""
    return pd.DataFrame([
        {
            "iteration": 2, "lever": 1, "cluster_id": "H001",
            "resolved_root_cause": "WRONG_COLUMN",
            "expected_sql": "SELECT secret_ground_truth FROM t",
            "generated_sql": "SELECT wrong FROM t",
            "rationale_snippet": "the model picked the wrong column for revenue",
            "counterfactual_fix": "use revenue_usd",
            "wrong_clause": "SELECT wrong",
        },
        {
            "iteration": 2, "lever": 6, "cluster_id": "H002",
            "resolved_root_cause": "WRONG_COLUMN",
            "expected_sql": "SELECT another_secret FROM u",
            "generated_sql": "SELECT bad FROM u",
            "rationale_snippet": "wrong aggregation",
            "counterfactual_fix": "sum not avg",
            "wrong_clause": "AVG",
        },
    ])


def _run_publish_and_audit(
    *,
    scored_iters: list[dict],
    run_row: dict | None = None,
    llm_text: str | None = "Baseline 80.0% climbed to 91.0%; champion iter 2 published.",
    llm_raises: bool = False,
    promoted_iteration: int = 2,
    refreshed_best_accuracy: float | None = None,
):
    """Invoke ``publish_and_audit`` with all Delta/LLM deps patched.

    ``scored_iters`` is the ``full`` + ``enrichment`` set the patched
    ``load_all_scored_iterations`` returns. ``promoted_iteration`` /
    ``refreshed_best_accuracy`` model what ``promote_best_model`` + the post-promote
    run-row reread return on the publish path.

    Returns ``(result, artifact_calls, update_calls, promote_mock, llm_mock)``.
    """
    run_row = run_row if run_row is not None else _run_row()

    artifact_calls: list[dict] = []
    update_calls: list[dict] = []

    def _fake_write_artifact(spark, run_id, kind, payload, **kwargs):  # noqa: ANN001
        artifact_calls.append({"kind": kind, "payload": payload, "kwargs": kwargs})
        return "artifact-1"

    def _fake_update_run_status(spark, run_id, catalog, schema, **kwargs):  # noqa: ANN001
        update_calls.append(kwargs)

    promote_mock = MagicMock(return_value=promoted_iteration)

    if llm_raises:
        llm_mock = MagicMock(side_effect=RuntimeError("endpoint 500"))
    else:
        llm_mock = MagicMock(return_value=(llm_text, MagicMock()))

    # The publish path only rereads the run row after promote; model a refreshed
    # best_accuracy on that call when asked.
    if refreshed_best_accuracy is not None:
        refreshed = {**run_row, "best_accuracy": refreshed_best_accuracy}
        load_run_mock = MagicMock(return_value=refreshed)
    else:
        load_run_mock = MagicMock(return_value=run_row)

    with (
        patch.object(P, "load_run", load_run_mock),
        patch.object(P, "load_all_scored_iterations", return_value=scored_iters),
        patch.object(P, "load_patches", return_value=_patches_df()),
        patch.object(P, "load_provenance", return_value=_provenance_df()),
        patch.object(P, "promote_best_model", promote_mock),
        patch.object(P, "write_artifact", _fake_write_artifact),
        patch.object(P, "update_run_status", _fake_update_run_status),
        patch.object(P, "call_llm", llm_mock),
    ):
        result = P.publish_and_audit(
            MagicMock(), MagicMock(), "run1",
            space_id="space-abc", catalog="c", schema="s",
            target_accuracy=90.0, max_attempts=3,
        )
    return result, artifact_calls, update_calls, promote_mock, llm_mock


# ── (i) gating-reason publish path ──────────────────────────────────────────


def test_target_reached_publishes_and_writes_full_record():
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="TARGET_REACHED"),
    )
    # promote_best_model called (idempotent Delta-only publish).
    promote.assert_called_once()
    assert result["published"] is True
    assert result["publish_outcome"] == "published"
    assert result["terminal_reason"] == "TARGET_REACHED"
    assert result["final_status"] == "CONVERGED"

    # publish_record artifact written with the full payload shape.
    assert len(artifacts) == 1
    assert artifacts[0]["kind"] == "publish_record"
    p = artifacts[0]["payload"]
    for field in (
        "run_id", "space_id", "final_status", "terminal_reason", "published",
        "publish_outcome", "champion_iteration", "champion_accuracy",
        "champion_config_version_id", "target_accuracy", "max_attempts",
        "audit_summary", "improvement_trajectory", "concerns",
    ):
        assert field in p, f"{field} missing from publish_record"
    assert p["published"] is True
    assert p["final_status"] == "CONVERGED"
    assert p["terminal_reason"] == "TARGET_REACHED"
    assert p["champion_iteration"] == 2
    assert p["champion_config_version_id"] == "cfg-v2"
    assert isinstance(p["improvement_trajectory"], list) and p["improvement_trajectory"]
    assert p["audit_summary"]  # LLM text present
    # No Phase-7 shell placeholder note.
    assert "note" not in p

    # run status stamped CONVERGED with convergence_reason == the ACTUAL reason.
    assert len(updates) == 1
    assert updates[0]["status"] == "CONVERGED"
    assert updates[0]["convergence_reason"] == "TARGET_REACHED"


def test_max_attempts_publishes_with_max_iterations_status():
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="MAX_ATTEMPTS", champion_accuracy=88.0),
    )
    promote.assert_called_once()
    assert result["published"] is True
    assert result["final_status"] == "MAX_ITERATIONS"
    assert artifacts[0]["payload"]["final_status"] == "MAX_ITERATIONS"
    # convergence_reason is the ACTUAL terminal reason, not collapsed.
    assert updates[0]["convergence_reason"] == "MAX_ATTEMPTS"
    assert updates[0]["status"] == "MAX_ITERATIONS"


# ── (ii) non-gating reasons: no publish, correct status, record still written ─


def test_eval_invalid_does_not_publish_status_failed():
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="EVAL_INVALID"),
    )
    promote.assert_not_called()
    assert result["published"] is False
    assert result["publish_outcome"] == "not_published:EVAL_INVALID"
    assert result["final_status"] == "FAILED"
    # publish_record STILL written, with concerns.
    p = artifacts[0]["payload"]
    assert p["published"] is False
    assert p["final_status"] == "FAILED"
    assert p["concerns"], "concerns must carry the stop reason"
    assert any("EVAL_INVALID" in c for c in p["concerns"])
    assert updates[0]["status"] == "FAILED"
    assert updates[0]["convergence_reason"] == "EVAL_INVALID"


def test_unknown_terminal_reason_does_not_publish_status_stalled():
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="UNKNOWN_STOP"),
    )
    promote.assert_not_called()
    assert result["published"] is False
    assert result["final_status"] == "STALLED"
    assert artifacts[0]["payload"]["concerns"]
    assert updates[0]["convergence_reason"] == "UNKNOWN_STOP"


def test_no_new_hypothesis_does_not_publish_status_stalled():
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="NO_NEW_HYPOTHESIS"),
    )
    promote.assert_not_called()
    assert result["published"] is False
    assert result["publish_outcome"] == "not_published:NO_NEW_HYPOTHESIS"
    assert result["final_status"] == "STALLED"
    p = artifacts[0]["payload"]
    assert p["published"] is False
    assert any("NO_NEW_HYPOTHESIS" in c for c in p["concerns"])
    assert updates[0]["status"] == "STALLED"


def test_eval_budget_exhausted_does_not_publish_status_stalled():
    result, _artifacts, updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="EVAL_BUDGET_EXHAUSTED"),
    )
    promote.assert_not_called()
    assert result["published"] is False
    assert result["final_status"] == "STALLED"
    assert updates[0]["convergence_reason"] == "EVAL_BUDGET_EXHAUSTED"


# ── (iii) terminal_reason is READ from the champion row, NOT re-derived ──────


def test_high_accuracy_but_eval_invalid_still_not_published():
    """A 95% champion would re-derive to TARGET_REACHED under the old shell.
    Reading the stamped EVAL_INVALID instead means it is NOT published."""
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="EVAL_INVALID", champion_accuracy=95.0),
    )
    promote.assert_not_called()
    assert result["published"] is False
    assert result["terminal_reason"] == "EVAL_INVALID"
    assert result["final_status"] == "FAILED"
    # Despite 95% accuracy >= 90% target, the stamped reason wins.
    assert artifacts[0]["payload"]["champion_accuracy"] == 95.0
    assert artifacts[0]["payload"]["terminal_reason"] == "EVAL_INVALID"


def test_unstamped_champion_does_not_publish_from_nonchampion_reason():
    """B1: a reason stamped on a NON-champion row must NEVER gate the publish.
    Champion (iter 2) is unstamped; a non-champion row carries MAX_ATTEMPTS — the
    publish is fail-closed (no publish, STALLED), and the non-champion reason is
    surfaced ONLY as a diagnostic concern."""
    iters = _full_iters(champion_reason=None, champion_accuracy=91.0)
    # champion (iter 2) unstamped; a later rolled-back surgical attempt carries it.
    iters.append({
        "iteration": 3, "eval_scope": "full", "rolled_back": True,
        "overall_accuracy": 70.0, "attempt_no": 3, "attempt_mode": "surgical",
        "decision": "reject", "is_champion": False, "terminal_reason": "MAX_ATTEMPTS",
        "remaining_failures": "[]",
    })
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(scored_iters=iters)
    # Fail-closed: NOT published, champion stays iter 2, status STALLED.
    promote.assert_not_called()
    assert result["published"] is False
    assert result["terminal_reason"] is None
    assert result["publish_outcome"] == "not_published:UNKNOWN"
    assert result["final_status"] == "STALLED"
    assert result["champion_iteration"] == 2
    # MAX_ATTEMPTS is recorded ONLY as a diagnostic concern, never used to gate.
    concerns = artifacts[0]["payload"]["concerns"]
    assert any("MAX_ATTEMPTS" in c and "NOT used for gating" in c for c in concerns)
    assert updates[0]["status"] == "STALLED"
    assert updates[0]["convergence_reason"] is None


def test_absent_terminal_reason_is_fail_closed_no_publish():
    """No stamped reason anywhere ⇒ fail-closed: do NOT publish, STALLED, concern.
    Never fabricate TARGET_REACHED from accuracy."""
    iters = _full_iters(champion_reason=None, champion_accuracy=99.0)
    result, artifacts, updates, promote, _llm = _run_publish_and_audit(scored_iters=iters)
    promote.assert_not_called()
    assert result["published"] is False
    assert result["terminal_reason"] is None
    assert result["publish_outcome"] == "not_published:UNKNOWN"
    assert result["final_status"] == "STALLED"
    assert any("terminal_reason" in c for c in artifacts[0]["payload"]["concerns"])
    # convergence_reason None ⇒ update_run_status leaves it (not collapsed).
    assert updates[0]["convergence_reason"] is None


# ── (iv) LLM audit-summary failure is non-fatal ──────────────────────────────


def test_audit_summary_failure_is_non_fatal_publish_still_succeeds():
    result, artifacts, updates, promote, llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="TARGET_REACHED"),
        llm_raises=True,
    )
    llm.assert_called_once()
    # Publish still succeeded despite the summary failure.
    promote.assert_called_once()
    assert result["published"] is True
    assert result["final_status"] == "CONVERGED"
    assert result["audit_summary_generated"] is False
    p = artifacts[0]["payload"]
    assert p["audit_summary"] is None
    assert any("Audit summary generation failed" in c for c in p["concerns"])
    # Run status was still stamped.
    assert updates[0]["status"] == "CONVERGED"


def test_audit_summary_empty_output_adds_concern_but_publishes():
    result, artifacts, _updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_full_iters(champion_reason="TARGET_REACHED"),
        llm_text="   ",
    )
    promote.assert_called_once()
    assert result["published"] is True
    p = artifacts[0]["payload"]
    assert p["audit_summary"] is None
    assert any("empty output" in c for c in p["concerns"])


# ── (v) NO benchmark Q/A reaches the audit prompt context ────────────────────


def test_audit_context_excludes_benchmark_qa_fields():
    iters = _full_iters(champion_reason="TARGET_REACHED")
    champion = P.resolve_champion_row(iters)
    ctx = P.as_audit_context(
        "run1", "space-abc", iters, _patches_df(), _provenance_df(),
        terminal_reason="TARGET_REACHED", champion_row=champion,
        target_accuracy=90.0, max_attempts=3,
    )
    # The serializer output must not carry any answer-key field as a key...
    for leaky in (
        "question", "question_text", "expected_sql", "generated_sql",
        "expected_response", "actual_response", "counterfactual_fix",
        "rationale_snippet", "wrong_clause",
    ):
        assert leaky not in ctx, f"audit context leaked key {leaky!r}"

    # ...and the serialized payload must not contain the ground-truth SQL / text
    # that lived in the provenance rows.
    import json
    blob = json.dumps(ctx, default=str)
    assert "secret_ground_truth" not in blob
    assert "another_secret" not in blob
    assert "the model picked the wrong column" not in blob
    assert "revenue_usd" not in blob

    # But the structural facts ARE present.
    assert ctx["baseline_accuracy"] == 80.0
    assert ctx["champion_iteration"] == 2
    assert ctx["champion_accuracy"] == 91.0
    assert ctx["total_patches_applied"] == 3
    assert ctx["patches_rolled_back"] == 1
    assert ctx["patch_families"]  # lever-family counts
    assert ctx["root_cause_distribution"] == {"WRONG_COLUMN": 2}
    assert ctx["residual_failure_count"] == 2  # ["q5","q9"]
    assert "H001" in ctx["residual_failing_clusters"]


def test_audit_context_includes_safe_failure_and_patch_attempt_summaries():
    import json

    iters = _full_iters(champion_reason="TARGET_REACHED")
    iters[0]["rows_json"] = json.dumps([
        {
            "question_id": "q-secret-1",
            "assessment": "BAD",
            "assessment_reasons": ["LLM_JUDGE_MISSING_JOIN"],
            "question": "benchmark question text must not leak",
            "expected_sql": "SELECT secret_ground_truth FROM t",
            "generated_sql": "SELECT wrong_generated_sql FROM t",
        },
        {
            "question_id": "q-good",
            "assessment": "GOOD",
            "assessment_reasons": [],
        },
    ])
    iters[1]["rows_json"] = json.dumps([
        {
            "question_id": "q-secret-2",
            "assessment": "BAD",
            "genie_equivalent_eval": {
                "assessment_reasons": ["LLM_JUDGE_MISSING_OR_INCORRECT_FILTER"]
            },
            "question": "another benchmark question text",
            "expected_sql": "SELECT another_secret FROM u",
        }
    ])
    iters[1]["current_hypothesis"] = json.dumps({
        "lever": 6,
        "rationale": "must not leak rationale with SELECT secret_ground_truth",
        "raw_response_preview": "must not leak raw response",
        "proposed_patch_count": 3,
        "patch_count": 1,
        "patch_types": ["update_instruction_section"],
        "patch_family": "mixed_or_structured",
        "structured_intent_lost": True,
        "preapply_dropped_count": 2,
        "preapply_dropped_summary": [
            {
                "type": "add_sql_snippet_measure",
                "drop_reason": "snippet_validation_failed",
                "drop_detail": "Execution failed: SELECT secret_ground_truth FROM t",
            },
            {
                "type": "add_example_sql",
                "drop_reason": "benchmark_example_sql_leak",
                "drop_detail": "Copied benchmark question text",
            },
        ],
    })
    iters[2]["rows_json"] = json.dumps([
        {
            "question_id": "q-secret-3",
            "assessment": "BAD",
            "assessment_reasons": ["LLM_JUDGE_WRONG_COLUMNS"],
            "expected_sql": "SELECT champion_secret FROM v",
        },
        {
            "question_id": "q-good",
            "assessment": "GOOD",
        },
    ])

    champion = P.resolve_champion_row(iters)
    ctx = P.as_audit_context(
        "run1", "space-abc", iters, _patches_df(), _provenance_df(),
        terminal_reason="TARGET_REACHED", champion_row=champion,
        target_accuracy=90.0, max_attempts=3,
    )

    blob = json.dumps(ctx, default=str)
    for forbidden in (
        "benchmark question text",
        "secret_ground_truth",
        "wrong_generated_sql",
        "another_secret",
        "champion_secret",
        "must not leak rationale",
        "must not leak raw response",
        "drop_detail",
        "raw_response_preview",
    ):
        assert forbidden not in blob

    assert ctx["baseline_failure_summary"]["failure_reason_counts"] == {
        "MISSING_JOIN": 1
    }
    assert ctx["champion_failure_summary"]["failure_reason_counts"] == {
        "WRONG_COLUMNS": 1
    }
    attempts = ctx["patch_attempt_summaries"]
    assert attempts == [
        {
            "iteration": 1,
            "attempt_no": 1,
            "accuracy": 84.0,
            "decision": "accept",
            "rolled_back": False,
            "is_champion": False,
            "lever": 6,
            "proposed_patch_count": 3,
            "surviving_patch_count": 1,
            "surviving_patch_types": ["update_instruction_section"],
            "patch_family": "mixed_or_structured",
            "preapply_dropped_count": 2,
            "preapply_dropped_patch_type_counts": {
                "add_sql_snippet_measure": 1,
                "add_example_sql": 1,
            },
            "preapply_dropped_reason_counts": {
                "snippet_validation_failed": 1,
                "benchmark_example_sql_leak": 1,
            },
            "structured_intent_lost": True,
        }
    ]


def test_build_audit_summary_passes_only_context_to_llm():
    """The user message handed to the LLM is exactly the serialized leak-free
    context — no raw benchmark Q/A is appended."""
    captured = {}

    def _fake_call_llm(w, *, messages, **kwargs):  # noqa: ANN001
        captured["messages"] = messages
        return "summary text", MagicMock()

    ctx = {"terminal_reason": "TARGET_REACHED", "champion_accuracy": 91.0}
    with patch.object(P, "call_llm", _fake_call_llm):
        summary, concern = P.build_audit_summary(MagicMock(), ctx)
    assert summary == "summary text"
    assert concern is None
    msgs = captured["messages"]
    assert msgs[0]["role"] == "system"
    assert msgs[1]["role"] == "user"
    # The user payload is exactly the JSON context (no extra Q/A smuggled in).
    import json
    assert json.loads(msgs[1]["content"]) == ctx


# ── (B3) firewall: free-text decision_reason excluded + recursive guard ──────


def test_decision_reason_free_text_is_excluded_from_audit_context():
    """B3(a): the free-text ``decision_reason`` is NEVER in the LLM context, even
    when it embeds benchmark answer-key material. Only the bounded ``decision``
    value survives into the trajectory."""
    iters = _full_iters(champion_reason="TARGET_REACHED")
    # Inject leaky free text into decision_reason values on multiple rungs.
    iters[2]["decision_reason"] = "rolled back; expected_sql was SELECT secret_q FROM t"
    iters[1]["decision_reason"] = "coverage note quoting the benchmark question text"
    champion = P.resolve_champion_row(iters)
    ctx = P.as_audit_context(
        "run1", "space-abc", iters, _patches_df(), _provenance_df(),
        terminal_reason="TARGET_REACHED", champion_row=champion,
        target_accuracy=90.0, max_attempts=3,
    )
    import json
    blob = json.dumps(ctx, default=str)
    assert "decision_reason" not in blob
    assert "expected_sql" not in blob
    assert "secret_q" not in blob
    assert "benchmark question text" not in blob
    # The bounded decision value IS retained on the trajectory rungs.
    assert all("decision" in rung for rung in ctx["improvement_trajectory"])
    assert all("decision_reason" not in rung for rung in ctx["improvement_trajectory"])


def test_assert_leak_free_is_recursive_over_nested_structures():
    """B3(b): the guard walks nested dicts/lists, not just top-level keys."""
    import pytest
    with pytest.raises(ValueError):
        P._assert_leak_free({"a": {"b": [{"expected_sql": "SELECT secret"}]}})
    with pytest.raises(ValueError):
        P._assert_leak_free({"trajectory": [{"nested": {"question": "what is x?"}}]})
    # A clean nested structure passes.
    P._assert_leak_free({"a": {"b": [{"accuracy": 91.0, "decision": "accept"}]}})


# ── (B2) champion selection is full-eval only ────────────────────────────────


def _retired_enrichment_champion_iters() -> list[dict]:
    """Baseline plus a retired enrichment rung. The unified loop no longer writes
    coverage/enrichment eval scopes, so promotion ignores this historical row and
    keeps the full-eval baseline as the champion."""
    return [
        {
            "iteration": 0, "eval_scope": "full", "rolled_back": False,
            "overall_accuracy": 80.0, "attempt_no": 0, "attempt_mode": "baseline",
            "decision": "accept", "is_champion": False, "terminal_reason": None,
            "remaining_failures": "[]",
        },
        {
            "iteration": 1, "eval_scope": "enrichment", "rolled_back": False,
            "overall_accuracy": 92.0, "attempt_no": 1, "attempt_mode": "coverage",
            "decision": "accept", "is_champion": True, "terminal_reason": "TARGET_REACHED",
            "best_accuracy": 92.0, "best_config_version_id": "cfg-cov",
            "surgical_attempts_used": 0, "target_accuracy": 90.0, "max_attempts": 3,
            "remaining_failures": "[]",
        },
    ]


def test_historical_enrichment_row_is_not_a_champion():
    iters = _retired_enrichment_champion_iters()
    champ = P.resolve_champion_row(iters)
    assert champ is not None
    assert champ["eval_scope"] == "full"
    assert champ["iteration"] == 0
    assert champ["overall_accuracy"] == 80.0
    assert P.resolve_terminal_reason(champ) is None

    # Historical rows can still be displayed in the trajectory for old runs.
    traj = P.build_improvement_trajectory(iters)
    assert any(
        t["eval_scope"] == "enrichment" and t["attempt_mode"] == "coverage"
        for t in traj
    )
    cov = next(t for t in traj if t["eval_scope"] == "enrichment")
    assert cov["delta_vs_baseline"] == 12.0


def test_champion_selection_treats_nan_flags_as_missing():
    iters = [
        {
            "iteration": 0, "eval_scope": "full", "rolled_back": False,
            "overall_accuracy": 80.0, "is_champion": False,
        },
        {
            "iteration": 1, "eval_scope": "full", "rolled_back": float("nan"),
            "overall_accuracy": 92.0, "is_champion": float("nan"),
        },
    ]

    champ = P.resolve_champion_row(iters)

    assert champ is not None
    assert champ["iteration"] == 1


def test_publish_ignores_historical_enrichment_champion_end_to_end():
    result, artifacts, _updates, promote, _llm = _run_publish_and_audit(
        scored_iters=_retired_enrichment_champion_iters(),
        promoted_iteration=0, refreshed_best_accuracy=80.0,
    )
    promote.assert_not_called()
    assert result["published"] is False
    assert result["champion_iteration"] == 0
    assert result["champion_accuracy"] == 80.0
    traj = artifacts[0]["payload"]["improvement_trajectory"]
    assert any(
        t["eval_scope"] == "enrichment" and t["attempt_mode"] == "coverage"
        for t in traj
    )


# ── (NB1) champion config pointer falls back to a stable config_json hash ─────


def test_champion_config_version_id_falls_back_to_config_json_hash():
    # No best_config_version_id, but config_json present ⇒ stable derived pointer.
    row = {"iteration": 2, "config_json": '{"tables": ["t"], "k": "v"}'}
    vid = P._champion_config_version_id(row)
    assert vid and vid.startswith("cfgsha:")
    # Deterministic for the same config_json.
    assert vid == P._champion_config_version_id(
        {"iteration": 2, "config_json": '{"tables": ["t"], "k": "v"}'}
    )
    # An explicit best_config_version_id is preferred when present.
    assert P._champion_config_version_id(
        {"best_config_version_id": "cfg-x", "config_json": "{}"}
    ) == "cfg-x"
    # None only when neither is available.
    assert P._champion_config_version_id({"iteration": 2}) is None


def test_publish_record_pointer_uses_config_json_hash_when_no_version_id():
    """NB1 through the orchestrator: a real-shaped champion row (no
    best_config_version_id, but config_json present) still yields a complete,
    non-empty champion_config_version_id in the publish_record."""
    iters = _full_iters(champion_reason="TARGET_REACHED")
    champ = iters[2]
    champ.pop("best_config_version_id", None)
    champ["config_json"] = '{"tables": ["sales"], "instructions": "x"}'
    result, artifacts, _updates, _promote, _llm = _run_publish_and_audit(scored_iters=iters)
    pointer = artifacts[0]["payload"]["champion_config_version_id"]
    assert pointer and pointer.startswith("cfgsha:")


# ── improvement trajectory shape ─────────────────────────────────────────────


def test_improvement_trajectory_is_baseline_coverage_surgical_staircase():
    iters = _full_iters(champion_reason="TARGET_REACHED")
    traj = P.build_improvement_trajectory(iters)
    assert [t["iteration"] for t in traj] == [0, 1, 2]
    assert traj[0]["attempt_mode"] == "baseline"
    assert traj[1]["attempt_mode"] == "coverage"
    assert traj[2]["attempt_mode"] == "surgical"
    # delta vs baseline is computed off iteration 0.
    assert traj[0]["delta_vs_baseline"] == 0.0
    assert traj[1]["delta_vs_baseline"] == 4.0
    assert traj[2]["delta_vs_baseline"] == 11.0
    assert traj[2]["is_champion"] is True
    # B3: free-text decision_reason is not present on trajectory rungs.
    assert all("decision_reason" not in t for t in traj)
