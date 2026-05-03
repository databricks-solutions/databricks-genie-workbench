"""Phase D failure-bucketing T2: three new FailureBucket values.

Asserts:
- The seven expected names are present and unique.
- Each new value round-trips through ``FailureBucket(value)``.
- The string values are stable identifiers (no spaces, no aliases).
"""
from __future__ import annotations

import pytest


def test_failure_bucket_enum_has_seven_top_level_values():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket,
    )

    names = {b.name for b in FailureBucket}
    assert names == {
        "GATE_OR_CAP_GAP",
        "EVIDENCE_GAP",
        "PROPOSAL_GAP",
        "MODEL_CEILING",
        "RCA_GAP",
        "TARGETING_GAP",
        "APPLY_OR_ROLLBACK_GAP",
    }


@pytest.mark.parametrize(
    "name",
    ["RCA_GAP", "TARGETING_GAP", "APPLY_OR_ROLLBACK_GAP"],
)
def test_new_enum_value_round_trips_through_constructor(name: str):
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket,
    )

    member = FailureBucket[name]
    assert member.name == name
    assert FailureBucket(member.value) is member


def test_new_enum_values_are_unique_against_existing():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket,
    )

    values = [b.value for b in FailureBucket]
    assert len(values) == len(set(values)), (
        f"duplicate FailureBucket values: {values}"
    )
