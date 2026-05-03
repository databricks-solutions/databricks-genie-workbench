"""TDD: classifier overrides ``wrong_join_spec`` to TOP_N_CARDINALITY_COLLAPSE
when SQL evidence + question intent agree, and leaves unrelated failures
unchanged.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, _safe_rca_kind


_QID_9_METADATA = {
    "question_text": "What are the top 5 destination cities by passenger volume in 2023?",
    "genie_sql": (
        "SELECT city, RANK() OVER (ORDER BY passengers DESC) AS r "
        "FROM trips WHERE year = 2023 AND r >= 1"
    ),
}


def test_wrong_join_spec_with_top_n_evidence_routes_to_top_n_collapse():
    kind = _safe_rca_kind(
        value=None,
        failure_type="wrong_join_spec",
        metadata=_QID_9_METADATA,
    )
    assert kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE


def test_wrong_aggregation_with_top_n_evidence_routes_to_top_n_collapse():
    kind = _safe_rca_kind(
        value=None,
        failure_type="wrong_aggregation",
        metadata=_QID_9_METADATA,
    )
    assert kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE


def test_different_grain_with_top_n_evidence_routes_to_top_n_collapse():
    kind = _safe_rca_kind(
        value=None,
        failure_type="different_grain",
        metadata=_QID_9_METADATA,
    )
    assert kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE


def test_wrong_join_spec_without_top_n_evidence_stays_join_spec():
    """The override only fires when the SQL+intent signals agree."""
    kind = _safe_rca_kind(
        value=None,
        failure_type="wrong_join_spec",
        metadata={
            "question_text": "Show flights per city",
            "genie_sql": "SELECT city FROM trips JOIN airports ON wrong_key",
        },
    )
    assert kind == RcaKind.JOIN_SPEC_MISSING_OR_WRONG


def test_explicit_rca_kind_value_still_takes_precedence():
    """When the producer explicitly stamps ``rca_kind``, never override."""
    kind = _safe_rca_kind(
        value="join_spec_missing_or_wrong",  # explicit
        failure_type="wrong_join_spec",
        metadata=_QID_9_METADATA,  # would otherwise trigger override
    )
    assert kind == RcaKind.JOIN_SPEC_MISSING_OR_WRONG


def test_existing_plural_top_n_collapse_failure_type_still_routes_correctly():
    """Pre-override path remains intact (regression guard for line 234)."""
    kind = _safe_rca_kind(
        value=None,
        failure_type="plural_top_n_collapse",
        metadata={},
    )
    assert kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE
