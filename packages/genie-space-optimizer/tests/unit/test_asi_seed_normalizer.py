"""Trial 13i — pin behaviour of ``_normalize_seeds_to_fqn``.

The normalizer rescues ASI-only capture bundles whose seed entries are
free-text column-name tokens (``DEST_AIRPORT_CD``, ``zone_vp_name``)
rather than 4-part FQNs. It runs inside
``build_stage1_evidence_card`` after ``blame_set_seed`` is built so
the Stage 1 LLM sees only schema-grounded entries.

Rules under test:

  1. Already in ``schema_columns`` -> keep.
  2. Bare identifier with exactly-one suffix match -> swap to FQN.
  3. Bare identifier with 0 or >1 suffix matches -> drop.
  4. Anything else (compound text) -> drop.
  5. Empty ``schema_columns`` -> every input dropped (no-op universe).
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.schema_columns import (
    _normalize_seeds_to_fqn,
)


_SCHEMA = (
    "main.airline.fact_flights.dest_airport_cd",
    "main.airline.fact_flights.orig_airport_cd",
    "main.airline.dim_carriers.carrier_cd",
    "main.public.orders.revenue",
)


def test_already_fqn_passes_through_unchanged() -> None:
    seeds = [
        "main.airline.fact_flights.dest_airport_cd",
        "main.public.orders.revenue",
    ]
    out, normalized, dropped = _normalize_seeds_to_fqn(seeds, _SCHEMA)
    assert out == seeds
    assert normalized == 0
    assert dropped == 0


def test_bare_identifier_unique_suffix_resolves() -> None:
    """``DEST_AIRPORT_CD`` matches exactly one entry -> swap."""
    out, normalized, dropped = _normalize_seeds_to_fqn(
        ["DEST_AIRPORT_CD"], _SCHEMA
    )
    assert out == ["main.airline.fact_flights.dest_airport_cd"]
    assert normalized == 1
    assert dropped == 0


def test_bare_identifier_case_insensitive_match() -> None:
    out, normalized, dropped = _normalize_seeds_to_fqn(
        ["dest_airport_cd", "Revenue"], _SCHEMA
    )
    assert "main.airline.fact_flights.dest_airport_cd" in out
    assert "main.public.orders.revenue" in out
    assert normalized == 2
    assert dropped == 0


def test_bare_identifier_no_match_dropped() -> None:
    out, normalized, dropped = _normalize_seeds_to_fqn(
        ["unknown_column"], _SCHEMA
    )
    assert out == []
    assert normalized == 0
    assert dropped == 1


def test_bare_identifier_ambiguous_match_dropped() -> None:
    """Suffix matches > 1 entry -> drop (no guess)."""
    schema = (
        "main.airline.fact_flights.dest_airport_cd",
        "main.airline.alt_fact.dest_airport_cd",  # collision
    )
    out, normalized, dropped = _normalize_seeds_to_fqn(
        ["DEST_AIRPORT_CD"], schema
    )
    assert out == []
    assert normalized == 0
    assert dropped == 1


def test_compound_text_dropped() -> None:
    """``LIMIT 10 vs RANK() <= 10`` is not a bare identifier -> drop."""
    seeds = [
        "LIMIT 10 vs RANK() <= 10",
        "RANK vs LIMIT",
        "MAX() not ORDER BY + LIMIT",
    ]
    out, normalized, dropped = _normalize_seeds_to_fqn(seeds, _SCHEMA)
    assert out == []
    assert normalized == 0
    assert dropped == 3


def test_mixed_input_partial_resolution() -> None:
    """Real-world post-13h shape from 98ec/gs_009: mostly free-text but
    two column tokens that suffix-match."""
    seeds = [
        "RANK",  # SQL keyword - no match
        "LIMIT 10 vs RANK() <= 10",  # compound text - dropped
        "DEST_AIRPORT_CD",  # matches fact_flights
        "ORIG_AIRPORT_CD",  # matches fact_flights
        "RANK vs LIMIT",  # compound text - dropped
    ]
    out, normalized, dropped = _normalize_seeds_to_fqn(seeds, _SCHEMA)
    assert sorted(out) == sorted(
        [
            "main.airline.fact_flights.dest_airport_cd",
            "main.airline.fact_flights.orig_airport_cd",
        ]
    )
    assert normalized == 2
    assert dropped == 3


def test_empty_schema_columns_drops_everything() -> None:
    """Without a universe to match against, every input is dropped."""
    out, normalized, dropped = _normalize_seeds_to_fqn(
        ["DEST_AIRPORT_CD", "main.public.orders.revenue"], ()
    )
    assert out == []
    assert normalized == 0
    assert dropped == 2


def test_duplicate_seeds_deduped_in_output() -> None:
    """Duplicates collapse in the output and only the first successful
    swap increments the ``normalized`` counter (subsequent duplicates
    are silently skipped — they neither swap nor drop)."""
    out, normalized, dropped = _normalize_seeds_to_fqn(
        ["DEST_AIRPORT_CD", "DEST_AIRPORT_CD", "dest_airport_cd"], _SCHEMA
    )
    assert out == ["main.airline.fact_flights.dest_airport_cd"]
    assert normalized == 1
    assert dropped == 0


def test_blank_entries_treated_as_dropped() -> None:
    out, normalized, dropped = _normalize_seeds_to_fqn(
        ["", "   ", None], _SCHEMA  # type: ignore[list-item]
    )
    assert out == []
    assert normalized == 0
    assert dropped == 3
