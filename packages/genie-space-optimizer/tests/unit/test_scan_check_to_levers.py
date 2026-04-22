"""Validity tests for the SCAN_CHECK_TO_LEVERS mapping.

Guards against typos and stale lever references in the static mapping from
IQ Scan check IDs to the levers that can plausibly fix them.
"""

from __future__ import annotations

from genie_space_optimizer.common.config import (
    DEFAULT_LEVER_ORDER,
    SCAN_CHECK_TO_LEVERS,
)
from genie_space_optimizer.iq_scan.scoring import CONFIG_CHECK_COUNT


def test_every_key_is_a_valid_1_indexed_check():
    # Checks are 1-indexed in the scan output. 12 total checks (10 config + 2 opt).
    for check_id in SCAN_CHECK_TO_LEVERS:
        assert 1 <= check_id <= 12, f"check_id {check_id} out of range"


def test_every_mapped_check_is_a_config_check():
    # Only config checks (1-10) can be fixed by optimizer levers; optimization
    # checks (11/12) measure the optimizer's own work.
    for check_id in SCAN_CHECK_TO_LEVERS:
        assert check_id <= CONFIG_CHECK_COUNT, (
            f"check {check_id} is an optimization check, not fixable by a lever"
        )


def test_no_mapping_for_unfixable_checks():
    # Checks 1 (data sources exist), 6 (data source count), and 10 (benchmarks)
    # are intentionally absent — no lever can add tables / reduce count / author
    # benchmarks.
    for unfixable in (1, 6, 10):
        assert unfixable not in SCAN_CHECK_TO_LEVERS, (
            f"check {unfixable} should not have a lever mapping"
        )


def test_every_lever_is_in_default_lever_order():
    valid = set(DEFAULT_LEVER_ORDER)
    for check_id, levers in SCAN_CHECK_TO_LEVERS.items():
        assert levers, f"check {check_id} has empty lever list"
        for lever in levers:
            assert lever in valid, (
                f"check {check_id} maps to lever {lever} which is not in DEFAULT_LEVER_ORDER"
            )


def test_lever_lists_are_unique_per_check():
    for check_id, levers in SCAN_CHECK_TO_LEVERS.items():
        assert len(levers) == len(set(levers)), (
            f"check {check_id} has duplicate lever entries: {levers}"
        )


def test_mapping_is_non_empty():
    # Sanity: the mapping should cover at least half of the config checks.
    assert len(SCAN_CHECK_TO_LEVERS) >= 5
