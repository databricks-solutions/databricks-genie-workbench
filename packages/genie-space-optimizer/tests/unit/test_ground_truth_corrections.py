"""Tests for Task 1: ground-truth correction row classification.

The arbiter exists because benchmark ground truth is partially synthetic
and Genie is sometimes more correct than the GT. A row with
``result_correctness=no`` AND ``arbiter=genie_correct`` is a corpus-quality
signal, not a Genie failure: it must be persisted to a corpus-review
queue and **must not** drive clustering, feature mining, or patch
generation.

These tests pin that contract so a future predicate edit cannot
re-introduce corpus defects into the lever loop.
"""

from __future__ import annotations

import json
import logging

import pytest

from genie_space_optimizer.optimization.ground_truth_corrections import (
    build_gt_correction_candidate,
    is_gt_correction_candidate,
    should_cluster_as_soft_signal,
)


# Helper to build a representative eval row.
def _retail_q011_row(
    *,
    rc: str = "no",
    arbiter: str = "genie_correct",
    schema_judge: str = "no",
    response_judge: str | None = None,
) -> dict:
    row = {
        "inputs.question_id": "retail_store_sales_analytics_011",
        "inputs.question": "What is average exchange rate by month?",
        "inputs.expected_sql": (
            "SELECT YEAR(date_key_2), MONTH(date_key_2), AVG(exchange_rate) "
            "FROM mv_esr_fact_sales GROUP BY YEAR(date_key_2), MONTH(date_key_2)"
        ),
        "outputs.predictions.sql": (
            "SELECT MONTH(date_key_2), AVG(exchange_rate) FROM mv_esr_fact_sales "
            "GROUP BY MONTH(date_key_2)"
        ),
        "feedback/result_correctness/value": rc,
        "feedback/arbiter/value": arbiter,
        "feedback/arbiter/rationale": (
            "Genie answer is defensible; GT is underspecified for multi-year data."
        ),
        "feedback/schema_accuracy/value": schema_judge,
    }
    if response_judge is not None:
        row["feedback/response_quality/value"] = response_judge
    return row


# ── is_gt_correction_candidate ─────────────────────────────────────────


def test_genie_correct_rc_no_is_gt_correction_candidate():
    row = _retail_q011_row(rc="no", arbiter="genie_correct")

    assert is_gt_correction_candidate(row) is True


def test_ground_truth_correct_rc_no_is_not_gt_correction_candidate():
    # Genuine Genie failure — must reach hard clustering.
    row = _retail_q011_row(rc="no", arbiter="ground_truth_correct")

    assert is_gt_correction_candidate(row) is False


def test_both_correct_rc_yes_is_not_gt_correction_candidate():
    # Fully passing row — no signal at all.
    row = _retail_q011_row(rc="yes", arbiter="both_correct")

    assert is_gt_correction_candidate(row) is False


def test_neither_correct_is_not_gt_correction_candidate():
    # Both Genie and GT wrong — hard failure with corpus-review flag,
    # but NOT a GT-correction candidate (Genie is also wrong).
    row = _retail_q011_row(rc="no", arbiter="neither_correct")

    assert is_gt_correction_candidate(row) is False


def test_legacy_unsuffixed_rc_and_arbiter_keys_work():
    row = {
        "inputs.question_id": "q",
        "inputs.expected_sql": "SELECT 1",
        "outputs.predictions.sql": "SELECT 1",
        "result_correctness/value": "no",
        "arbiter/value": "genie_correct",
    }

    assert is_gt_correction_candidate(row) is True


def test_case_insensitive_rc_and_arbiter():
    row = _retail_q011_row(rc="No", arbiter="Genie_Correct")

    assert is_gt_correction_candidate(row) is True


def test_handles_falsey_rc_strings():
    row = _retail_q011_row(rc="false", arbiter="genie_correct")

    assert is_gt_correction_candidate(row) is True


# ── should_cluster_as_soft_signal ─────────────────────────────────────


def test_genie_correct_row_is_not_soft_signal_even_with_judge_failures():
    # Q011-shape row: rc=no, arbiter=genie_correct, schema judge said no.
    # Today's predicate would see the schema-judge failure and route the
    # row to soft clustering. Task 1 prevents this so corpus defects do
    # not generate patches.
    row = _retail_q011_row(rc="no", arbiter="genie_correct", schema_judge="no")

    assert should_cluster_as_soft_signal(row) is False


def test_arbiter_rescued_row_with_judge_failures_is_not_soft_signal():
    # Task 9 broadens the soft-signal exclusion to *any* arbiter-rescued row
    # (``arbiter ∈ {both_correct, genie_correct}``), not just GT-correction
    # candidates. The arbiter has overridden judge dissent, so the row must
    # not feed clustering — Q009 H001 ghost-cluster pattern.
    row = _retail_q011_row(rc="yes", arbiter="both_correct", schema_judge="no")

    assert should_cluster_as_soft_signal(row) is False


def test_fully_correct_row_is_not_soft_signal():
    row = _retail_q011_row(rc="yes", arbiter="both_correct", schema_judge="yes")

    assert should_cluster_as_soft_signal(row) is False


def test_genuine_hard_failure_is_not_soft_signal():
    # Hard failure goes to hard clustering, not soft; should_cluster_as_soft_signal
    # returns True only when ≥1 judge failed AND row isn't a hard failure
    # AND row isn't a GT correction candidate. Hard failure rows do have
    # judge failures, but the harness branches to hard before checking
    # soft. The predicate itself is a guard, not the branch decision.
    row = _retail_q011_row(rc="no", arbiter="ground_truth_correct", schema_judge="no")

    # The helper returns True because it isn't a GT-correction candidate
    # AND it has individual judge failures. The harness's hard-failure
    # branch fires first (verified separately in test_unified_hard_failure_predicate).
    assert should_cluster_as_soft_signal(row) is True


# ── build_gt_correction_candidate ─────────────────────────────────────


def test_build_candidate_carries_required_fields():
    row = _retail_q011_row()

    candidate = build_gt_correction_candidate(row, run_id="run-1", iteration=2)

    assert candidate["run_id"] == "run-1"
    assert candidate["iteration"] == 2
    assert candidate["question_id"] == "retail_store_sales_analytics_011"
    assert candidate["arbiter_verdict"] == "genie_correct"
    assert candidate["status"] == "pending_review"
    assert "expected_sql" in candidate
    assert "genie_sql" in candidate
    assert candidate["expected_sql"].startswith("SELECT YEAR")
    assert candidate["genie_sql"].startswith("SELECT MONTH")
    assert "underspecified" in candidate["arbiter_rationale"]


def test_build_candidate_question_id_falls_back_through_legacy_keys():
    row = {
        "question_id": "q-legacy",
        "question": "Show daily sales.",
        "expected_sql": "SELECT 1",
        "genie_sql": "SELECT 1",
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "genie_correct",
    }

    candidate = build_gt_correction_candidate(row, run_id="run-2", iteration=0)

    assert candidate["question_id"] == "q-legacy"
    assert candidate["status"] == "pending_review"


def test_build_candidate_uses_alternate_sql_keys_when_outputs_predictions_missing():
    row = {
        "inputs.question_id": "q",
        "inputs.question": "Show monthly sales.",
        "inputs.expected_sql": "SELECT 1",
        "generated_sql": "SELECT 2",  # alternate key
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "genie_correct",
    }

    candidate = build_gt_correction_candidate(row, run_id="run-3", iteration=1)

    assert candidate["genie_sql"] == "SELECT 2"


def test_status_state_machine_initial_value_is_pending_review():
    # The reviewer flow must start every entry at pending_review so the
    # GT correction queue can transition to accepted_corpus_fix /
    # rejected_keep_gt / superseded downstream.
    row = _retail_q011_row()

    candidate = build_gt_correction_candidate(row, run_id="r", iteration=0)

    assert candidate["status"] == "pending_review"


# ── Cycle 8 Bug 2: broaden _extract_question_id to mirror Track D ────
#
# Cycle 8 stderr emitted ``Skipping unidentifiable GT correction
# candidate`` warnings even after Track D fixed the carrier-side
# extractor (``harness._baseline_row_qid``). Root cause: the GT-
# correction extractor was a separate, narrower implementation that
# did not learn the cycle 5/7 row shapes. The fix consolidates both
# extractors onto a shared helper and broadens the GT-correction path
# to cover ``request.kwargs.question_id`` and ``client_request_id``
# (last-resort) — exactly the shapes Track D already handles.
#
# These tests pin the new contract on the GT-correction call site so
# the same extractor divergence cannot recur.


def _arbiter_genie_correct_row(extra: dict) -> dict:
    """Build a minimal arbiter=genie_correct row with caller-supplied
    qid keys grafted on. Strips out any default ``inputs.question_id``
    so the test can isolate which qid key the extractor finds."""
    base = {
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "genie_correct",
    }
    base.update(extra)
    return base


def test_build_candidate_extracts_question_id_from_request_kwargs_dict():
    # Cycle 5 row shape: canonical qid lives nested in
    # request.kwargs.question_id. The narrow pre-fix extractor missed
    # this and the row was silently skipped.
    row = _arbiter_genie_correct_row({
        "request": {"kwargs": {"question_id": "airline_q_canonical"}},
    })

    candidate = build_gt_correction_candidate(row, run_id="run-x", iteration=3)

    assert candidate["question_id"] == "airline_q_canonical"


def test_build_candidate_extracts_question_id_from_request_kwargs_json_string():
    # Some MLflow eval-table shapes persist ``request`` as a JSON-
    # encoded string. The shared extractor parses it and finds the
    # canonical qid; the narrow pre-fix extractor returned "".
    row = _arbiter_genie_correct_row({
        "request": json.dumps({"kwargs": {"question_id": "from_json_kwargs"}}),
    })

    candidate = build_gt_correction_candidate(row, run_id="run-y", iteration=4)

    assert candidate["question_id"] == "from_json_kwargs"


def test_build_candidate_uses_client_request_id_as_last_resort_and_warns(caplog):
    # When no canonical-qid key exists, the extractor falls through to
    # ``client_request_id`` so the candidate isn't silently dropped.
    # The candidate is built but a structured WARNING fires so the
    # reviewer can canonicalize the qid via MLflow.
    row = _arbiter_genie_correct_row({"client_request_id": "tr-only"})

    with caplog.at_level(
        logging.WARNING,
        logger="genie_space_optimizer.optimization.ground_truth_corrections",
    ):
        candidate = build_gt_correction_candidate(row, run_id="r", iteration=0)

    assert candidate["question_id"] == "tr-only"
    warning_records = [
        r for r in caplog.records
        if r.levelno >= logging.WARNING
        and "trace" in r.getMessage().lower()
    ]
    assert warning_records, (
        "Expected a structured warning when GT-correction extractor "
        "falls back to a trace-id key"
    )


def test_build_candidate_prefers_canonical_over_client_request_id():
    # Co-existence of canonical and trace-id keys: canonical wins.
    # Mirrors Track D's contract that canonical sources are always
    # checked before client_request_id.
    row = _arbiter_genie_correct_row({
        "client_request_id": "tr-aaa",
        "inputs": {"question_id": "airline_canonical_024"},
    })

    candidate = build_gt_correction_candidate(row, run_id="r", iteration=0)

    assert candidate["question_id"] == "airline_canonical_024"


def test_build_candidate_logs_debug_payload_before_raising_on_unidentifiable_row(caplog):
    # When the row truly carries no qid keys at all, build_gt_correction
    # _candidate must (a) raise ValueError so the harness skip-and-warn
    # path fires, AND (b) emit a DEBUG log capturing the row's key set
    # and a payload hash so cycle 9+ can identify what shape is failing
    # without re-running the lever loop. The debug log is the diagnosis
    # hook the cycle-8 side-bugs plan called for.
    row = {
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "genie_correct",
        "unrelated_data": "no qid here",
    }

    with caplog.at_level(
        logging.DEBUG,
        logger="genie_space_optimizer.optimization.ground_truth_corrections",
    ):
        with pytest.raises(ValueError):
            build_gt_correction_candidate(row, run_id="r", iteration=0)

    debug_records = [
        r for r in caplog.records
        if r.levelno == logging.DEBUG
        and "row keys" in r.getMessage().lower()
    ]
    assert debug_records, (
        "Expected a DEBUG log capturing row.keys() before the "
        "ValueError is raised, so cycle 9+ can identify the failing shape"
    )
    msg = debug_records[0].getMessage()
    assert "feedback/result_correctness/value" in msg
    assert "unrelated_data" in msg
    # A short payload hash makes the same shape correlatable across
    # iterations / runs without leaking the full row to logs.
    assert "payload_hash" in msg.lower()
