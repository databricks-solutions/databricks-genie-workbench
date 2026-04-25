"""Unit tests for the centralized metric-view error matchers.

Locks the contract that ``is_metric_view_error`` and
``metric_view_error_kind`` recognise every Spark/Databricks error class
GSO needs to dispatch on, regardless of which legacy spelling
(``METRIC_VIEW_UNSUPPORTED_USAGE`` vs ``UNSUPPORTED_METRIC_VIEW_USAGE``)
the runtime emits.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.evaluation import (
    is_metric_view_error,
    metric_view_error_kind,
)


@pytest.mark.parametrize(
    "reason,expected",
    [
        # Modern docs spelling.
        ("[UNSUPPORTED_METRIC_VIEW_USAGE] The metric view usage is not supported", True),
        # Legacy spelling Spark Connect emits today via GRPC.
        ("[METRIC_VIEW_UNSUPPORTED_USAGE] The metric view usage is not supported", True),
        # Bare measure-wrap failure.
        (
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] The usage of measure column [foo]",
            True,
        ),
        # Direct-join failure.
        ("[METRIC_VIEW_JOIN_NOT_SUPPORTED] Direct joins are unsupported", True),
        # Mixed case must still match (we upper() before matching).
        ("[metric_view_missing_measure_function] foo", True),
        # Embedded inside a larger payload.
        (
            "GRPC Error received: status = INTERNAL details = "
            "\"[UNSUPPORTED_METRIC_VIEW_USAGE] plan: Aggregate [...]\"",
            True,
        ),
        # Negative cases.
        ("UNRESOLVED_COLUMN.WITH_SUGGESTION foo", False),
        ("AnalysisException: cannot resolve column", False),
        ("", False),
        (None, False),
    ],
)
def test_is_metric_view_error(reason, expected):
    assert is_metric_view_error(reason) is expected


@pytest.mark.parametrize(
    "reason,expected_kind",
    [
        (
            "[UNSUPPORTED_METRIC_VIEW_USAGE] The metric view usage is not supported",
            "unsupported_usage",
        ),
        (
            "[METRIC_VIEW_UNSUPPORTED_USAGE] The metric view usage is not supported",
            "unsupported_usage",
        ),
        (
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] The usage of measure column [foo]",
            "missing_measure",
        ),
        (
            "[METRIC_VIEW_JOIN_NOT_SUPPORTED] Direct joins are unsupported",
            "join_not_supported",
        ),
        # When a payload mentions multiple kinds, the most specific
        # (missing_measure / join_not_supported) wins over the generic
        # unsupported_usage classifier.
        (
            "[UNSUPPORTED_METRIC_VIEW_USAGE] ... [METRIC_VIEW_MISSING_MEASURE_FUNCTION] ...",
            "missing_measure",
        ),
        (
            "[UNSUPPORTED_METRIC_VIEW_USAGE] ... [METRIC_VIEW_JOIN_NOT_SUPPORTED] ...",
            "join_not_supported",
        ),
        ("", None),
        (None, None),
        ("UNRESOLVED_COLUMN", None),
    ],
)
def test_metric_view_error_kind(reason, expected_kind):
    assert metric_view_error_kind(reason) == expected_kind


def test_centralized_matcher_unblocks_existing_callers():
    """Ensure the existing private helper still classifies measure failures.

    ``preflight_synthesis._is_measure_function_failure`` delegates to the
    centralized matcher; callers must continue to see the same boolean
    when handed a gate result with a measure-wrap reason.
    """
    from types import SimpleNamespace

    from genie_space_optimizer.optimization.preflight_synthesis import (
        _is_measure_function_failure,
    )

    measure_fail = SimpleNamespace(
        passed=False,
        reason="[METRIC_VIEW_MISSING_MEASURE_FUNCTION] The usage of measure column [m]",
    )
    other_fail = SimpleNamespace(
        passed=False,
        reason="[UNSUPPORTED_METRIC_VIEW_USAGE] plan: Aggregate ...",
    )
    passed = SimpleNamespace(passed=True, reason=None)

    assert _is_measure_function_failure(measure_fail) is True
    # The unsupported-usage payload is a metric-view error but NOT a
    # missing-measure error — the dispatcher must keep treating it as
    # not-a-measure-failure so it doesn't engage the measure-wrap retry
    # path.
    assert _is_measure_function_failure(other_fail) is False
    assert _is_measure_function_failure(passed) is False
    assert _is_measure_function_failure(None) is False
