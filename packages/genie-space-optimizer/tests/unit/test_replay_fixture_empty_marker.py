"""B5 Task 6 — replay_fixture_empty_marker formatter tests."""

from __future__ import annotations

import json
import re


def test_marker_has_canonical_shape() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        replay_fixture_empty_marker,
    )

    line = replay_fixture_empty_marker(
        optimization_run_id="2314bb2c",
        iterations_data_count=5,
        fixture_iterations_count=0,
        iterations_with_zero_eval_rows=(),
    )
    assert line.startswith("GSO_REPLAY_FIXTURE_EMPTY_V1 ")
    payload = json.loads(re.search(r"\s+(\{.*\})", line).group(1))
    assert payload["optimization_run_id"] == "2314bb2c"
    assert payload["iterations_data_count"] == 5
    assert payload["fixture_iterations_count"] == 0
    assert payload["iterations_with_zero_eval_rows"] == []


def test_marker_carries_per_iteration_zero_eval_rows() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        replay_fixture_empty_marker,
    )

    line = replay_fixture_empty_marker(
        optimization_run_id="2314bb2c",
        iterations_data_count=5,
        fixture_iterations_count=5,
        iterations_with_zero_eval_rows=(2, 3, 4, 5),
    )
    payload = json.loads(re.search(r"\s+(\{.*\})", line).group(1))
    assert payload["iterations_with_zero_eval_rows"] == [2, 3, 4, 5]


def test_marker_is_single_line() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        replay_fixture_empty_marker,
    )

    line = replay_fixture_empty_marker(
        optimization_run_id="r",
        iterations_data_count=0,
        fixture_iterations_count=0,
    )
    assert "\n" not in line.rstrip("\n")
