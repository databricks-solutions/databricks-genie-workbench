"""Trial 32 W32.6 — decline-origin classifier (observability).

The same `no_top_n_archetype` skipped_reason is emitted whether the binding
constraint is blame-resolution, archetype matching, or a downstream gate —
three layers that three consecutive trials each mis-attributed. This pins the
classifier that the live `GSO_TRIAL32_DECLINE_ORIGIN_V1` marker carries so ONE
live trial names the binding layer deterministically.

Non-anchor fixture (`cat.sch.orders`).
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    classify_slice_decline_origin,
)

_SNAPSHOT = {
    "data_sources": {
        "tables": [
            {
                "identifier": "cat.sch.orders",
                "column_configs": [
                    {"column_name": "amount", "column_type": "double"},
                    {"column_name": "region", "column_type": "string"},
                ],
            },
        ],
        "metric_views": [],
    },
    "instructions": {"join_specs": []},
}

_AFS_COLUMN_BLAME = {
    "cluster_id": "H001",
    "failure_type": "extra_defensive_filter",
    "blame_set": ["cat.sch.orders.amount", "cat.sch.orders.region"],
}


@pytest.fixture
def _flag_on(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL32", "1")
    monkeypatch.setenv("GSO_TRIAL32_COLUMN_FQN_RESOLUTION", "1")


@pytest.fixture
def _flag_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL32_COLUMN_FQN_RESOLUTION", "0")


def test_column_blame_resolves_so_binding_is_downstream(_flag_on):
    """With W32.1 resolving column FQNs, the column-grained blame resolves AND
    pick_archetype returns the simple_enumerate safety-net — so the decline
    (if it happened) is NEITHER blame nor archetype: it's downstream. This is
    the signal that would redirect the airline fix to the L6/handoff layer."""
    out = classify_slice_decline_origin(_AFS_COLUMN_BLAME, _SNAPSHOT)
    assert out["blame_resolved"] == 2
    assert out["blame_total"] == 2
    assert out["archetype"] != "none"  # simple_enumerate safety-net matches
    assert out["origin"] == "slice_buildable_declined_downstream"
    assert out["failure_type"] == "extra_defensive_filter"


def test_column_blame_unresolved_when_fix_off_is_blame_resolution(_flag_off):
    """Flag OFF reproduces the pre-W32.1 state: column FQNs don't resolve, so
    the classifier correctly names blame-resolution as the binding layer."""
    out = classify_slice_decline_origin(_AFS_COLUMN_BLAME, _SNAPSHOT)
    assert out["blame_resolved"] == 0
    assert out["origin"] == "blame_unresolved"


def test_empty_blame_is_blame_empty(_flag_on):
    out = classify_slice_decline_origin(
        {"cluster_id": "H1", "failure_type": "x", "blame_set": []}, _SNAPSHOT
    )
    assert out["origin"] == "blame_empty"
    assert out["blame_total"] == 0
