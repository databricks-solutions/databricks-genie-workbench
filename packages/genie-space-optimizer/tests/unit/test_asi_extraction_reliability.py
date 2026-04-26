"""Tests for Task 0 Step 2: ASI source telemetry helpers.

Covers ``compute_asi_source_summary`` and ``build_asi_extraction_audit_row``.
The decoded retail run logged ``0`` recovered trace IDs across every eval
pass — i.e. the ASI/root-cause stack ran in row/UC fallback the entire
run — without any structured signal that this had happened. These tests
pin the contract that:

* a run with no traces but full row-payload coverage is classified
  ``asi_source_no_traces`` (degraded but covered),
* a run with any row missing ASI is classified ``asi_source_partial``,
* a healthy run is classified ``asi_source_complete``.
"""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.evaluation import (
    AsiSourceCounts,
    build_asi_extraction_audit_row,
    compute_asi_source_summary,
)


def _row(asi_source: str | None = None, *, with_metadata: bool = False) -> dict:
    row: dict = {}
    if asi_source is not None:
        row["_asi_source"] = asi_source
    if with_metadata:
        row["feedback/schema_accuracy/metadata"] = '{"failure_type": "wrong_column"}'
    return row


# ── compute_asi_source_summary ───────────────────────────────────────


def test_summary_classifies_known_trace_sources_as_trace():
    rows = [_row("trace"), _row("recovered_trace")]

    summary = compute_asi_source_summary(rows)

    assert summary.trace == 2
    assert summary.row_payload == 0
    assert summary.uc_metadata == 0
    assert summary.none == 0


def test_summary_classifies_cache_and_row_payload_as_row_payload():
    # ``cache`` is the legacy stamp from the run-scoped scorer cache and
    # represents row-resident metadata, so it maps to ``row_payload`` in
    # the typed summary.
    rows = [_row("cache"), _row("row_payload")]

    summary = compute_asi_source_summary(rows)

    assert summary.row_payload == 2
    assert summary.trace == 0


def test_summary_classifies_uc_sources():
    rows = [_row("uc_metadata"), _row("uc_cache")]

    summary = compute_asi_source_summary(rows)

    assert summary.uc_metadata == 2


def test_summary_infers_row_payload_when_metadata_present_without_stamp():
    rows = [_row(with_metadata=True)]

    summary = compute_asi_source_summary(rows)

    assert summary.row_payload == 1
    assert summary.none == 0


def test_summary_classifies_completely_empty_rows_as_none():
    rows = [_row(), _row()]

    summary = compute_asi_source_summary(rows)

    assert summary.none == 2
    assert summary.trace == summary.row_payload == summary.uc_metadata == 0


def test_summary_total_and_coverage_ratio():
    rows = [_row("trace"), _row("cache"), _row(with_metadata=True), _row()]

    summary = compute_asi_source_summary(rows)

    assert summary.total == 4
    # 3 of 4 rows have evidence (trace + 2× row_payload)
    assert summary.coverage_ratio == 0.75


def test_summary_unknown_stamp_falls_back_to_row_payload_not_dropped():
    rows = [_row("totally_unknown_source")]

    summary = compute_asi_source_summary(rows)

    # Unknown stamps must not silently disappear from the audit.
    assert summary.total == 1
    assert summary.row_payload == 1


# ── build_asi_extraction_audit_row ──────────────────────────────────


def test_zero_trace_ids_does_not_silently_succeed():
    """When trace recovery returns nothing but row/UC fallback covers
    every row, the audit row must call out the degraded state."""
    summary = compute_asi_source_summary([_row("cache"), _row("row_payload"), _row("uc_metadata")])

    audit = build_asi_extraction_audit_row(
        run_id="run-1",
        iteration=1,
        summary=summary,
        trace_id_count=0,
        expected_trace_count=3,
    )

    assert audit["reason_code"] == "asi_source_no_traces"
    assert audit["decision"] == "degraded"
    metrics = json.loads(audit["metrics_json"])
    assert metrics["trace"] == 0
    assert metrics["row_payload"] + metrics["uc_metadata"] == 3
    assert metrics["trace_id_count"] == 0


def test_high_none_share_emits_observability_signal():
    summary = compute_asi_source_summary([_row("trace"), _row(), _row()])

    audit = build_asi_extraction_audit_row(
        run_id="run-1", iteration=1, summary=summary,
    )

    assert audit["reason_code"] == "asi_source_partial"
    assert audit["decision"] == "degraded"
    metrics = json.loads(audit["metrics_json"])
    assert metrics["none"] == 2


def test_complete_coverage_with_traces_is_marked_complete():
    summary = compute_asi_source_summary([_row("trace"), _row("trace")])

    audit = build_asi_extraction_audit_row(
        run_id="run-1", iteration=2, summary=summary, trace_id_count=2,
    )

    assert audit["reason_code"] == "asi_source_complete"
    assert audit["decision"] == "ok"


def test_audit_row_carries_required_decision_audit_columns():
    summary = compute_asi_source_summary([_row("trace")])

    audit = build_asi_extraction_audit_row(
        run_id="run-xyz", iteration=3, summary=summary,
    )

    # Task 3's decision-audit table contract requires these keys.
    assert audit["run_id"] == "run-xyz"
    assert audit["iteration"] == 3
    assert audit["stage_letter"] == "C"
    assert audit["gate_name"] == "asi_extraction"
    assert "metrics_json" in audit


def test_summary_handles_empty_rows_input():
    summary = compute_asi_source_summary([])

    assert isinstance(summary, AsiSourceCounts)
    assert summary.total == 0
    assert summary.coverage_ratio == 0.0


def test_summary_ignores_none_in_rows_iterable():
    # Defensive: upstream may pass a list containing ``None`` rows;
    # the helper should not crash.
    summary = compute_asi_source_summary([_row("trace"), {}, _row(with_metadata=True)])

    assert summary.total == 3
    assert summary.trace == 1
    assert summary.row_payload == 1
    assert summary.none == 1
