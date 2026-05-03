"""TDD coverage for cycle 9 SEED_CATALOG additions (T9).

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T9.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.failure_buckets import (
    FailureBucket,
    SEED_CATALOG,
    match_pattern_id,
)


CYCLE9_PATTERN_IDS = (
    "dead_on_arrival_blocks_buffered_drain",
    "blast_radius_no_escape_hatch",
    "proposal_direction_inversion",
    "union_all_grain_split",
)


@pytest.mark.parametrize("pid", CYCLE9_PATTERN_IDS)
def test_pattern_present_in_catalog(pid):
    pattern = match_pattern_id(pid)
    assert pattern is not None, f"Missing seed pattern: {pid}"
    assert pattern.source_run.startswith("cycle9") or "cycle9" in pattern.source_run


def test_buckets_assigned():
    expected = {
        "dead_on_arrival_blocks_buffered_drain": FailureBucket.GATE_OR_CAP_GAP,
        "blast_radius_no_escape_hatch": FailureBucket.GATE_OR_CAP_GAP,
        "proposal_direction_inversion": FailureBucket.PROPOSAL_GAP,
        "union_all_grain_split": FailureBucket.MODEL_CEILING,
    }
    for pid, bucket in expected.items():
        p = match_pattern_id(pid)
        assert p is not None
        assert p.bucket is bucket, f"{pid} bucket={p.bucket} expected={bucket}"


def test_catalog_grows_by_four():
    pids = [p.pattern_id for p in SEED_CATALOG]
    for pid in CYCLE9_PATTERN_IDS:
        assert pid in pids, f"{pid} not appended to SEED_CATALOG"
