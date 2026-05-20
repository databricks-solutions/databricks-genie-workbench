"""Plan 9 Task 8 — every anchor in every iteration emits exactly
one PLAN5_ANCHOR_ACTIVATION_V1 marker.

Integration smoke test against a synthetic 2-AG iteration. After
the iteration, parse stdout for markers; assert one marker per
(ag_id, iteration) pair.
"""
import pytest

# Test uses harness internals; mark for the integration runner.
pytestmark = pytest.mark.integration


def test_each_anchor_emits_exactly_one_activation_marker():
    """Run a 2-AG iteration through a stub harness path and verify
    each AG produces exactly one PLAN5_ANCHOR_ACTIVATION_V1 marker."""
    pytest.skip(
        "Placeholder — fill with harness stub when "
        "tests/fixtures/two_ag_synthetic_iter1/ lands."
    )
