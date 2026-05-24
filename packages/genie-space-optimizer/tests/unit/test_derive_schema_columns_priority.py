"""Trial 13i — priority chain test for ``_derive_schema_columns``.

The helper is the single source of truth for the run-level
``ctx.schema_columns`` list across the SM lane, the workbench, and the
Plan 11 batch lane. Pinning its priority order here protects every
caller from accidental drift.

Priority chain (first non-empty wins):

  1. ``metadata_snapshot["schema_columns"]`` -> ``"metadata_snapshot"``
  2. union of ``rca_evidence_typed[*].blame_set`` -> ``"typed_evidence_union"``
  3. ``_build_identifier_allowlist`` 4-part FQNs -> ``"identifier_allowlist"``
  4. () -> ``"empty"``
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

from genie_space_optimizer.optimization.schema_columns import (
    SCHEMA_COLUMNS_SOURCE_LABELS,
    _derive_schema_columns,
)


@dataclass(frozen=True)
class _FakeEv:
    blame_set: tuple[str, ...]


def test_source_labels_closed_vocabulary() -> None:
    """Closed-enum vocabulary — pin so postmortems can join on values."""
    assert SCHEMA_COLUMNS_SOURCE_LABELS == frozenset(
        {"metadata_snapshot", "typed_evidence_union", "identifier_allowlist", "empty"}
    )


def test_priority_1_metadata_snapshot_wins_over_typed_evidence() -> None:
    """Explicit metadata field beats typed-evidence union when both present."""
    cols, source = _derive_schema_columns(
        metadata_snapshot={
            "schema_columns": [
                "main.public.orders.revenue",
                "main.public.orders.customer_id",
            ],
        },
        rca_evidence_typed={
            "gs_001": _FakeEv(blame_set=("main.other.table.col",))
        },
    )
    assert cols == (
        "main.public.orders.revenue",
        "main.public.orders.customer_id",
    )
    assert source == "metadata_snapshot"


def test_priority_2_typed_evidence_union_when_no_metadata() -> None:
    """Empty metadata_snapshot -> typed-evidence union (mirror batch lane)."""
    cols, source = _derive_schema_columns(
        metadata_snapshot={},
        rca_evidence_typed={
            "gs_001": _FakeEv(
                blame_set=(
                    "main.public.orders.revenue",
                    "main.public.orders.customer_id",
                )
            ),
            "gs_002": _FakeEv(
                blame_set=(
                    "main.public.orders.revenue",  # dedup
                    "main.public.payments.amount",
                )
            ),
        },
    )
    assert set(cols) == {
        "main.public.orders.revenue",
        "main.public.orders.customer_id",
        "main.public.payments.amount",
    }
    # Order-preserving: first appearance wins.
    assert cols[0] == "main.public.orders.revenue"
    assert source == "typed_evidence_union"


def test_priority_3_identifier_allowlist_fallback() -> None:
    """No metadata, no typed evidence -> allowlist re-projected to FQNs."""
    metadata = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.airline.fact_flights",
                    "column_configs": [
                        {"column_name": "dest_airport_cd", "data_type": "STRING"},
                        {"column_name": "orig_airport_cd", "data_type": "STRING"},
                    ],
                },
                {
                    "identifier": "main.airline.dim_carriers",
                    "column_configs": [
                        {"column_name": "carrier_cd", "data_type": "STRING"},
                    ],
                },
            ],
        },
    }
    cols, source = _derive_schema_columns(
        metadata_snapshot=metadata,
        rca_evidence_typed=None,
    )
    assert source == "identifier_allowlist"
    assert "main.airline.fact_flights.dest_airport_cd" in cols
    assert "main.airline.fact_flights.orig_airport_cd" in cols
    assert "main.airline.dim_carriers.carrier_cd" in cols
    # Every entry MUST be a 4-part FQN.
    for c in cols:
        assert c.count(".") == 3


def test_priority_4_empty_when_no_source_populated() -> None:
    """All sources empty -> () + ``"empty"`` label."""
    cols, source = _derive_schema_columns(
        metadata_snapshot={},
        rca_evidence_typed=None,
    )
    assert cols == ()
    assert source == "empty"


def test_typed_evidence_with_empty_blame_set_does_not_match() -> None:
    """A typed-evidence dict with only empty blame_set entries does not
    populate the union; we still fall through to allowlist / empty."""
    cols, source = _derive_schema_columns(
        metadata_snapshot={},
        rca_evidence_typed={
            "gs_001": _FakeEv(blame_set=()),
            "gs_002": _FakeEv(blame_set=("",)),
        },
    )
    assert cols == ()
    assert source == "empty"


def test_metadata_snapshot_with_only_blank_entries_falls_through() -> None:
    """Defensive: a metadata_snapshot whose ``schema_columns`` is all
    whitespace must NOT short-circuit at priority 1."""
    cols, source = _derive_schema_columns(
        metadata_snapshot={"schema_columns": ["", "   ", None]},
        rca_evidence_typed={
            "gs_001": _FakeEv(blame_set=("main.public.orders.revenue",))
        },
    )
    assert cols == ("main.public.orders.revenue",)
    assert source == "typed_evidence_union"


def test_metadata_snapshot_non_list_schema_columns_is_ignored() -> None:
    """If ``schema_columns`` is the wrong shape (e.g. a string), skip it
    and fall through. We do not coerce."""
    cols, source = _derive_schema_columns(
        metadata_snapshot={"schema_columns": "not a list"},
        rca_evidence_typed=None,
    )
    assert cols == ()
    assert source == "empty"


def test_rca_evidence_dict_shape_supported() -> None:
    """``rca_evidence_typed`` values may be either typed objects with
    a ``blame_set`` attribute or plain dicts with a ``blame_set`` key."""
    cols, source = _derive_schema_columns(
        metadata_snapshot={},
        rca_evidence_typed={
            "gs_001": {"blame_set": ["main.public.orders.revenue"]},
        },
    )
    assert cols == ("main.public.orders.revenue",)
    assert source == "typed_evidence_union"
