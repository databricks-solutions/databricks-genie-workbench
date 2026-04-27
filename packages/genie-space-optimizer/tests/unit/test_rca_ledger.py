from __future__ import annotations

from genie_space_optimizer.optimization.rca import (
    RcaEvidence,
    RcaFinding,
    RcaKind,
    extract_rca_findings_from_row,
)


def test_rca_finding_carries_patchable_evidence():
    finding = RcaFinding(
        rca_id="rca_retail_010_metric_view_routing_confusion",
        question_id="retail_010",
        rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
        confidence=0.9,
        expected_objects=("mv_esr_store_sales", "avg_txn_day", "avg_txn_mtd"),
        actual_objects=("mv_7now_store_sales", "7now_avg_txn_cy_day"),
        evidence=(
            RcaEvidence(source="sql_diff", detail="expected ESR avg txn measures"),
            RcaEvidence(source="arbiter", detail="ground_truth_correct"),
        ),
        recommended_levers=(1, 5),
        patch_family="contrastive_metric_routing",
        target_qids=("retail_010",),
    )

    assert finding.rca_kind.value == "metric_view_routing_confusion"
    assert finding.patch_family == "contrastive_metric_routing"
    assert finding.recommended_levers == (1, 5)


def test_extracts_metric_view_routing_confusion_from_avg_txn_failure():
    row = {
        "inputs.question_id": "retail_010",
        "inputs.expected_sql": "SELECT MEASURE(avg_txn_day) FROM mv_esr_store_sales",
        "outputs.predictions.sql": (
            "SELECT MEASURE(`7now_avg_txn_cy_day`) FROM mv_7now_store_sales"
        ),
        "feedback/arbiter/value": "ground_truth_correct",
    }

    findings = extract_rca_findings_from_row(row, metadata_snapshot={})

    assert any(
        f.rca_kind is RcaKind.METRIC_VIEW_ROUTING_CONFUSION for f in findings
    )
    assert any("mv_esr_store_sales" in f.expected_objects for f in findings)
    assert any("mv_7now_store_sales" in f.actual_objects for f in findings)


def test_extracts_canonical_dimension_missed_for_calendar_month():
    row = {
        "inputs.question_id": "retail_003",
        "inputs.expected_sql": (
            "SELECT d.calendar_month FROM mv_esr_dim_date d "
            "GROUP BY d.calendar_month"
        ),
        "outputs.predictions.sql": (
            "SELECT MONTH(d.full_date) AS calendar_month FROM mv_esr_dim_date d "
            "GROUP BY MONTH(d.full_date)"
        ),
        "feedback/arbiter/value": "ground_truth_correct",
    }

    findings = extract_rca_findings_from_row(row, metadata_snapshot={})

    assert any(f.rca_kind is RcaKind.CANONICAL_DIMENSION_MISSED for f in findings)


def test_extracts_missing_required_dimension_for_time_window():
    row = {
        "inputs.question_id": "retail_012",
        "inputs.expected_sql": (
            "SELECT time_window, MEASURE(`7now_store_count_day`) "
            "FROM mv_7now_store_sales GROUP BY time_window"
        ),
        "outputs.predictions.sql": (
            "SELECT MEASURE(`7now_store_count_day`) "
            "FROM mv_7now_store_sales GROUP BY ALL"
        ),
        "feedback/arbiter/value": "neither_correct",
    }

    findings = extract_rca_findings_from_row(row, metadata_snapshot={})

    assert any(f.rca_kind is RcaKind.MISSING_REQUIRED_DIMENSION for f in findings)
    assert any("time_window" in f.expected_objects for f in findings)


def test_extracts_extra_defensive_filter_for_not_null_guards():
    row = {
        "inputs.question_id": "retail_021",
        "inputs.expected_sql": (
            "SELECT same_store_7now, SUM(cy_cust_count) "
            "FROM mv_7now_fact_sales GROUP BY same_store_7now"
        ),
        "outputs.predictions.sql": (
            "SELECT same_store_7now, SUM(cy_cust_count) "
            "FROM mv_7now_fact_sales "
            "WHERE cy_cust_count IS NOT NULL GROUP BY same_store_7now"
        ),
        "feedback/arbiter/value": "ground_truth_correct",
    }

    findings = extract_rca_findings_from_row(row, metadata_snapshot={})

    assert any(f.rca_kind is RcaKind.EXTRA_DEFENSIVE_FILTER for f in findings)
