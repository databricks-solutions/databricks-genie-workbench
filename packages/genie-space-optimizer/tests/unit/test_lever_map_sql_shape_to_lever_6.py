"""Regression tests for the Phase A1 router remap.

SQL-shape root causes (missing_filter, wrong_aggregation, etc.) used to
route to Lever 2 (Metric Views), which can only update MV column
descriptions. Phase A1 reroutes them to Lever 6 (sql_snippet) so the
actual structural fix is available. Descriptive causes still route to
Lever 1; grouping/dimension causes remain on Lever 6.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.optimizer import (
    _SQL_SHAPE_ROOT_CAUSES,
    _map_to_lever,
)


# Phase A1: these structural causes MUST route to Lever 6 now.
SQL_SHAPE_TO_LEVER_6 = [
    "missing_filter",
    "missing_scd_filter",
    "missing_temporal_filter",
    "wrong_filter_condition",
    "wrong_aggregation",
    "wrong_measure",
    "missing_dimension",
    "wrong_grouping",
]


@pytest.mark.parametrize("root_cause", SQL_SHAPE_TO_LEVER_6)
def test_sql_shape_root_causes_route_to_lever_6(root_cause: str) -> None:
    assert _map_to_lever(root_cause) == 6


def test_description_mismatch_stays_on_lever_1() -> None:
    assert _map_to_lever("description_mismatch") == 1
    assert _map_to_lever("missing_synonym") == 1


def test_wrong_column_stays_on_lever_1() -> None:
    assert _map_to_lever("wrong_column") == 1
    assert _map_to_lever("wrong_table") == 1


def test_join_causes_route_to_lever_4() -> None:
    assert _map_to_lever("wrong_join") == 4
    assert _map_to_lever("missing_join_spec") == 4
    assert _map_to_lever("wrong_join_spec") == 4


def test_tvf_routes_to_lever_3() -> None:
    assert _map_to_lever("tvf_parameter_error") == 3


def test_routing_stays_on_lever_5() -> None:
    assert _map_to_lever("asset_routing_error") == 5
    assert _map_to_lever("ambiguous_question") == 5


def test_sql_shape_root_causes_frozenset_covers_router_remap() -> None:
    """The A1 reroute and the A3 gate must share the same taxonomy.

    Every root cause the router sends to Lever 6 via the SQL-shape branch
    must also be in ``_SQL_SHAPE_ROOT_CAUSES`` so the Lever 5 structural
    gate (A3) can recognise it and drop weak text-only proposals.
    """
    for rc in SQL_SHAPE_TO_LEVER_6:
        assert rc in _SQL_SHAPE_ROOT_CAUSES, rc
