"""Trial 32 W32.1 — column-FQN → owning-table blame resolution.

Confirmed via the airline W32.5 evidence bundle (task 660146417372063): the
Stage-1 blame_set named 4-part COLUMN FQNs
(`main.airline.fact_tickets.payment_currency_cd`), but
`_resolve_asset_by_identifier` only resolved 3-part TABLE/MV identifiers, so
the column never resolved to a table, `_derive_asset_slice_from_afs` returned
``None``, and the cluster declined with
`GSO_NO_STRUCTURAL_CANDIDATE_V1{skipped_reason:"no_top_n_archetype",
attempted_archetypes:[]}`.

The fix: a 4-part column FQN that doesn't match a table directly resolves to
its OWNING table (3-part prefix), so the slice builds and structural synthesis
proceeds. Gated by ``GSO_TRIAL32_COLUMN_FQN_RESOLUTION`` (default ON;
byte-stable when OFF).

Non-anchor fixture (``cat.sch.orders``) — proves generality, not an
airline-specific patch.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    _resolve_asset_by_identifier,
    _derive_asset_slice_from_afs,
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

# Column-grained blame (the airline extra_defensive_filter shape): 4-part FQNs.
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


# ── The fix: column FQN resolves to its owning table ─────────────────────

def test_column_fqn_resolves_to_owning_table(_flag_on):
    asset = _resolve_asset_by_identifier(_SNAPSHOT, "cat.sch.orders.amount")
    assert asset is not None
    assert (asset.get("identifier") or "").lower() == "cat.sch.orders"


def test_column_grained_blame_builds_a_slice(_flag_on):
    """The exact airline failure: a column-grained blame_set must now ground a
    slice instead of returning None (which declined with no_top_n_archetype)."""
    derived = _derive_asset_slice_from_afs(_AFS_COLUMN_BLAME, _SNAPSHOT)
    assert derived is not None
    slice_, _archetype = derived
    idents = [(t.get("identifier") or "").lower() for t in slice_.tables]
    assert "cat.sch.orders" in idents


# ── Byte-stable rollback when the flag is OFF ────────────────────────────

def test_flag_off_keeps_legacy_unresolved(_flag_off):
    assert _resolve_asset_by_identifier(_SNAPSHOT, "cat.sch.orders.amount") is None
    assert _derive_asset_slice_from_afs(_AFS_COLUMN_BLAME, _SNAPSHOT) is None


# ── No regression to 3-part table / short-name resolution ────────────────

def test_three_part_table_id_still_resolves(_flag_on):
    asset = _resolve_asset_by_identifier(_SNAPSHOT, "cat.sch.orders")
    assert asset is not None and (asset.get("identifier") or "").lower() == "cat.sch.orders"


def test_short_table_name_still_resolves(_flag_on):
    asset = _resolve_asset_by_identifier(_SNAPSHOT, "orders")
    assert asset is not None and (asset.get("identifier") or "").lower() == "cat.sch.orders"


def test_unresolvable_identifier_still_none(_flag_on):
    assert _resolve_asset_by_identifier(_SNAPSHOT, "cat.sch.nonexistent.col") is None
    assert _resolve_asset_by_identifier(_SNAPSHOT, "") is None
