"""Tests for Task 7: NL_TEXT-only cluster routing + ranking tiebreakers.

The retail run had soft clusters whose only failing judge was
``response_quality`` (NL_TEXT, weight 0.1). They were ranked on the
same priority list as SQL-shape clusters but cannot be fixed by any
SQL-shape lever. After Task 6 stamps typed SQL diffs on confirmed
failures, NL_TEXT-only clusters need their own narrative-only track
so they don't consume the SQL-shape patch budget.

Also covers Task 7's deterministic ranking tiebreakers — without
them, a 5-way impact-score tie sorts by Python list insertion order
(ultimately qid lexicographic) instead of by real signal.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import (
    NL_TEXT_ONLY_JUDGES,
    _signal_class_rank,
    is_nl_text_only_cluster,
    rank_clusters,
    route_nl_text_only_cluster,
)


# ── is_nl_text_only_cluster + route_nl_text_only_cluster ───────────


def test_response_quality_only_cluster_is_nl_text_only():
    cluster = {"dominant_failed_judges": ["response_quality"]}

    assert is_nl_text_only_cluster(cluster) is True
    assert route_nl_text_only_cluster(cluster) == "narrative_only"


def test_table_routing_quality_text_only_cluster_is_nl_text_only():
    cluster = {"dominant_failed_judges": ["table_routing_quality_text"]}

    assert is_nl_text_only_cluster(cluster) is True


def test_mixed_nl_text_and_sql_shape_is_not_nl_text_only():
    cluster = {
        "dominant_failed_judges": ["response_quality", "schema_accuracy"],
    }

    assert is_nl_text_only_cluster(cluster) is False
    assert route_nl_text_only_cluster(cluster) == "default"


def test_pure_sql_shape_cluster_is_not_nl_text_only():
    cluster = {
        "dominant_failed_judges": [
            "schema_accuracy", "result_correctness", "logical_accuracy",
        ],
    }

    assert is_nl_text_only_cluster(cluster) is False


def test_falls_back_to_affected_judges_field():
    """Legacy clusters carry ``affected_judges`` instead of
    ``dominant_failed_judges`` — the predicate must still classify
    them correctly."""
    cluster = {"affected_judges": ["response_quality"]}

    assert is_nl_text_only_cluster(cluster) is True


def test_empty_judges_list_is_not_nl_text_only():
    cluster = {"dominant_failed_judges": []}

    assert is_nl_text_only_cluster(cluster) is False


def test_nl_text_only_judges_set_contains_response_quality():
    # Pin the canonical set so future contributors can't silently widen
    # it (the routing depends on this contract).
    assert "response_quality" in NL_TEXT_ONLY_JUDGES


# ── _signal_class_rank ────────────────────────────────────────────


def test_signal_class_rank_orders_sql_shape_above_nl_text():
    sql_cluster = {"signal_class": "sql_shape"}
    nl_cluster = {"signal_class": "nl_text"}

    assert _signal_class_rank(sql_cluster) > _signal_class_rank(nl_cluster)


def test_signal_class_rank_falls_back_to_judge_aggregation():
    cluster = {"affected_judges": ["schema_accuracy", "result_correctness"]}

    # No precomputed signal_class — derive from judges.
    rank = _signal_class_rank(cluster)
    assert rank == _signal_class_rank({"signal_class": "sql_shape"})


# ── rank_clusters tiebreakers ─────────────────────────────────


def test_tiebreaker_prefers_hard_over_soft_at_equal_impact():
    """When two clusters tie on impact_score, the hard one ranks first."""
    clusters = [
        {
            "cluster_id": "S001", "signal_type": "soft",
            "raw_impact_score": 1.0,
            "affected_judges": ["schema_accuracy"],
            "question_ids": ["q1"],
        },
        {
            "cluster_id": "H001", "signal_type": "hard",
            "raw_impact_score": 1.0,
            "affected_judges": ["schema_accuracy"],
            "question_ids": ["q2"],
        },
    ]

    ranked = rank_clusters(clusters)

    assert ranked[0]["cluster_id"] == "H001"
    assert ranked[1]["cluster_id"] == "S001"


def test_tiebreaker_prefers_sql_shape_over_nl_text_at_equal_impact():
    clusters = [
        {
            "cluster_id": "C_NL", "signal_type": "hard",
            "raw_impact_score": 1.0,
            "affected_judges": ["response_quality"],
            "question_ids": ["q1"],
        },
        {
            "cluster_id": "C_SQL", "signal_type": "hard",
            "raw_impact_score": 1.0,
            "affected_judges": ["schema_accuracy"],
            "question_ids": ["q2"],
        },
    ]

    ranked = rank_clusters(clusters)

    assert ranked[0]["cluster_id"] == "C_SQL"
    assert ranked[1]["cluster_id"] == "C_NL"


def test_tiebreaker_prefers_higher_signal_quality():
    """SQL-shape vs SQL-shape, same impact: higher signal_quality.combined wins."""
    clusters = [
        {
            "cluster_id": "C_low", "signal_type": "hard",
            "raw_impact_score": 1.0,
            "affected_judges": ["schema_accuracy"],
            "signal_quality": {"combined": 0.4},
            "question_ids": ["q1"],
        },
        {
            "cluster_id": "C_high", "signal_type": "hard",
            "raw_impact_score": 1.0,
            "affected_judges": ["schema_accuracy"],
            "signal_quality": {"combined": 0.95},
            "question_ids": ["q2"],
        },
    ]

    ranked = rank_clusters(clusters)

    assert ranked[0]["cluster_id"] == "C_high"
    assert ranked[1]["cluster_id"] == "C_low"


def test_tiebreaker_uses_judge_failure_ratio_as_final_signal():
    """Same impact, same signal class, same signal quality → break by
    mean_judge_failure_ratio (higher = more judges agreeing)."""
    clusters = [
        {
            "cluster_id": "C_lo", "signal_type": "hard",
            "raw_impact_score": 1.0,
            "affected_judges": ["schema_accuracy"],
            "signal_quality": {"combined": 0.8},
            "mean_judge_failure_ratio": 0.3,
            "question_ids": ["q1"],
        },
        {
            "cluster_id": "C_hi", "signal_type": "hard",
            "raw_impact_score": 1.0,
            "affected_judges": ["schema_accuracy"],
            "signal_quality": {"combined": 0.8},
            "mean_judge_failure_ratio": 0.85,
            "question_ids": ["q2"],
        },
    ]

    ranked = rank_clusters(clusters)

    assert ranked[0]["cluster_id"] == "C_hi"


def test_higher_impact_still_wins_over_tiebreakers():
    """Tiebreakers only fire when impact ties — a clear impact lead
    must always rank first regardless of signal class."""
    clusters = [
        # NL_TEXT cluster with 20 affected questions — dampened by NL
        # weight but still has the larger raw question_count.
        {
            "cluster_id": "C_high_impact_nl",
            "signal_type": "hard",
            "affected_judge": "schema_accuracy",  # SQL-shape weight
            "asi_failure_type": "wrong_table",    # high severity
            "affected_judges": ["schema_accuracy"],
            "question_ids": [f"q{i}" for i in range(20)],
        },
        # SQL-shape cluster with a single question — small impact
        # despite the favorable signal class.
        {
            "cluster_id": "C_low_impact_sql",
            "signal_type": "hard",
            "affected_judge": "schema_accuracy",
            "asi_failure_type": "wrong_table",
            "affected_judges": ["schema_accuracy"],
            "question_ids": ["q_single"],
        },
    ]

    ranked = rank_clusters(clusters)

    # 20× the question_count => impact_score dominates the tiebreakers.
    assert ranked[0]["cluster_id"] == "C_high_impact_nl"
