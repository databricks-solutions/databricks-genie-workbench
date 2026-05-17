"""Phase 4 (2026-05-16) — fixture loader resolves the directory↔run-id
inversion documented in Phase 4 Audit A.1.

The user-spec labels (Run A ab65fefe, Run B 59a173d3) address the
fixtures by run-id substring, NOT by directory name. The
``_postmortem_fixtures`` helper enforces that contract.
"""
from __future__ import annotations

import pytest

from tests.replay.active._postmortem_fixtures import (
    RUN_A_AB65FEFE,
    RUN_B_59A173D3,
    get_iteration,
    load_run_a_ab65fefe,
    load_run_b_59a173d3,
)


def test_run_a_loader_returns_ab65fefe_fixture():
    fixture = load_run_a_ab65fefe()
    assert "ab65fefe" in fixture["fixture_id"], (
        f"Run A loader must return the fixture whose run-id "
        f"contains 'ab65fefe'. Got fixture_id={fixture['fixture_id']!r}"
    )


def test_run_b_loader_returns_59a173d3_fixture():
    fixture = load_run_b_59a173d3()
    assert "59a173d3" in fixture["fixture_id"], (
        f"Run B loader must return the fixture whose run-id "
        f"contains '59a173d3'. Got fixture_id={fixture['fixture_id']!r}"
    )


def test_run_a_constant_matches_loader():
    fixture = load_run_a_ab65fefe()
    assert RUN_A_AB65FEFE in fixture["fixture_id"]


def test_run_b_constant_matches_loader():
    fixture = load_run_b_59a173d3()
    assert RUN_B_59A173D3 in fixture["fixture_id"]


def test_get_iteration_returns_iter_by_one_indexed_number():
    fixture = load_run_a_ab65fefe()
    iter1 = get_iteration(fixture, 1)
    assert iter1["iteration"] == 1
    iter4 = get_iteration(fixture, 4)
    assert iter4["iteration"] == 4


def test_get_iteration_raises_for_missing_iter():
    fixture = load_run_a_ab65fefe()
    with pytest.raises(LookupError):
        get_iteration(fixture, 99)


def test_loaders_do_not_swap_runs():
    """Defensive: if the directory↔run-id inversion ever gets
    'corrected' on disk without updating this loader, this test
    fails loud."""
    run_a = load_run_a_ab65fefe()
    run_b = load_run_b_59a173d3()
    assert "ab65fefe" in run_a["fixture_id"]
    assert "59a173d3" in run_b["fixture_id"]
    assert run_a["fixture_id"] != run_b["fixture_id"]
