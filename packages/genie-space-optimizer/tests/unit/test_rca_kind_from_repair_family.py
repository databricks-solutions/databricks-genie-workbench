"""Plan 3 Task 4 — open-vocab → closed RcaKind mapper."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.rca_evidence_typed import (
    rca_kind_from_repair_family,
)


@pytest.mark.parametrize(
    "family, expected",
    [
        # Top-N family
        ("top_n_with_ordering", RcaKind.TOP_N_CARDINALITY_COLLAPSE),
        ("top_n_cardinality", RcaKind.TOP_N_CARDINALITY_COLLAPSE),
        ("Top-N Cardinality Preservation", RcaKind.TOP_N_CARDINALITY_COLLAPSE),
        # Join family
        ("join_spec_addition_with_disambiguation", RcaKind.JOIN_SPEC_MISSING_OR_WRONG),
        ("missing_join", RcaKind.JOIN_SPEC_MISSING_OR_WRONG),
        ("wrong_join_spec", RcaKind.JOIN_SPEC_MISSING_OR_WRONG),
        # Filter family
        ("filter_logic_correction", RcaKind.FILTER_LOGIC_MISMATCH),
        ("filter_removal_for_unrequested_predicate", RcaKind.EXTRA_DEFENSIVE_FILTER),
        # Grain family
        ("grain_correction_to_state", RcaKind.GRAIN_OR_GROUPING_MISMATCH),
        ("grouping_dimension_swap", RcaKind.GRAIN_OR_GROUPING_MISMATCH),
        # Time-window family
        ("time_window_logic_correction", RcaKind.TIME_WINDOW_LOGIC_MISMATCH),
        # Measure family
        ("measure_swap_to_canonical", RcaKind.MEASURE_SWAP),
        # SQL-expression family
        ("sql_expression_addition", RcaKind.SQL_EXPRESSION_MISSING),
        # Synonym / entity-match family
        ("synonym_addition_for_entity_match", RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING),
        # Function/TVF family
        ("function_or_tvf_invocation", RcaKind.FUNCTION_OR_TVF_NOT_INVOKED),
        # Example-SQL shape family
        ("example_sql_shape_addition", RcaKind.EXAMPLE_SQL_SHAPE_NEEDED),
        # Metric-view family
        ("metric_view_routing_correction", RcaKind.METRIC_VIEW_ROUTING_CONFUSION),
        # Asset-type routing family
        ("asset_type_routing_correction", RcaKind.ASSET_TYPE_ROUTING_MISMATCH),
        # Canonical dimension family
        ("canonical_dimension_addition", RcaKind.CANONICAL_DIMENSION_MISSED),
        # Required-dimension family
        ("required_dimension_addition", RcaKind.MISSING_REQUIRED_DIMENSION),
        # Novel / unmatched family
        ("totally_novel_pattern_we_have_not_seen", RcaKind.UNKNOWN),
        ("", RcaKind.UNKNOWN),
    ],
)
def test_mapper_returns_expected_rca_kind(
    family: str, expected: RcaKind
) -> None:
    assert rca_kind_from_repair_family(family) is expected


def test_mapper_is_case_insensitive() -> None:
    assert (
        rca_kind_from_repair_family("TOP_N_WITH_ORDERING")
        is RcaKind.TOP_N_CARDINALITY_COLLAPSE
    )


def test_mapper_handles_whitespace_padding() -> None:
    assert (
        rca_kind_from_repair_family("  join_spec_addition  ")
        is RcaKind.JOIN_SPEC_MISSING_OR_WRONG
    )


def test_mapper_returns_unknown_for_none() -> None:
    assert rca_kind_from_repair_family(None) is RcaKind.UNKNOWN
