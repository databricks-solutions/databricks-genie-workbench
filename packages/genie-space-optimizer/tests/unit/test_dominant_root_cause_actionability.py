"""Pin actionability-first dominant root cause selection."""

from __future__ import annotations

from collections import defaultdict

from genie_space_optimizer.optimization.optimizer import (
    _select_dominant_root_cause,
    _ACTIONABLE_ROOT_CAUSES,
)


def test_actionable_label_beats_non_actionable_with_higher_weight() -> None:
    """missing_join_spec (actionable) must win over format_difference (not actionable)."""
    weighted = defaultdict(float, {
        "format_difference": 3.1,
        "missing_join_spec": 0.5,
        "wrong_aggregation": 0.5,
    })
    chosen = _select_dominant_root_cause(weighted)
    assert chosen == "missing_join_spec" or chosen == "wrong_aggregation"
    assert chosen != "format_difference"


def test_actionable_set_includes_disambiguation_and_temporal() -> None:
    """The actionable set must be the SQL_SHAPE union with disambiguation/temporal/join."""
    for label in (
        "column_disambiguation",
        "missing_filter",
        "missing_temporal_filter",
        "missing_join_spec",
    ):
        assert label in _ACTIONABLE_ROOT_CAUSES, (
            f"{label} must be actionable so it wins over format_difference"
        )


def test_two_actionable_labels_tie_break_by_weight_then_sql_shape() -> None:
    weighted = defaultdict(float, {
        "missing_filter": 0.3,
        "missing_join_spec": 0.5,
    })
    assert _select_dominant_root_cause(weighted) == "missing_join_spec"


def test_no_actionable_label_falls_back_to_weighted_max() -> None:
    weighted = defaultdict(float, {
        "format_difference": 3.1,
        "extra_columns_only": 0.5,
    })
    assert _select_dominant_root_cause(weighted) == "format_difference"
