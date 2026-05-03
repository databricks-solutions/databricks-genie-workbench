"""Regression: QID_009 from optimization_run_id 407772af-... must classify
as TOP_N_CARDINALITY_COLLAPSE.

The judge labeled the failure ``wrong_join_spec``; the actual root
cause is `plural_top_n_collapse`. Without the metadata override, this
qid spent 3 wasted iterations on lever-4/5 join-spec patches that
all rolled back. This test pins the override so accidental revert
breaks CI.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, _safe_rca_kind


_QID_009_LIVE_METADATA = {
    "question_text": (
        "What are the top 5 destination cities by passenger volume in 2023?"
    ),
    "genie_sql": (
        "SELECT city, "
        "RANK() OVER (ORDER BY SUM(passengers) DESC) AS r "
        "FROM trips t "
        "JOIN dim_route d ON t.route_id = d.route_id "
        "WHERE EXTRACT(year FROM t.flight_date) = 2023 "
        "GROUP BY city "
        "HAVING r >= 1"
    ),
    "wrong_clause": "JOIN dim_route ON ...",
    "counterfactual_fix": "Restrict join to 2023 routes only.",
}


def test_qid009_classifies_as_top_n_cardinality_collapse():
    kind = _safe_rca_kind(
        value=None,
        failure_type="wrong_join_spec",
        metadata=_QID_009_LIVE_METADATA,
    )
    assert kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE, (
        "QID_009 regression: judge-labeled wrong_join_spec must override "
        "to TOP_N_CARDINALITY_COLLAPSE when SQL has RANK() without LIMIT N "
        "and question text contains 'top 5'."
    )
