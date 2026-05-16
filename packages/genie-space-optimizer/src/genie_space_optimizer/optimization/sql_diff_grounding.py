"""Extract deterministic grounding terms from ASI SqlDiff payloads.

The RCA card builder uses these so SQL-shape failures
(``wrong_aggregation``, ``filter_logic_mismatch``, etc.) always have
non-empty ``grounding_terms``, which is the contract that
``rca_card_grounded`` checks downstream.

Trial-5 Run A failed because the card builder produced empty
``grounding_terms`` for the airline space's ``wrong_aggregation``
clusters — the SqlDiff payload was present in the cluster but no code
read aggregation atoms out of it. This module is that code.
"""

from __future__ import annotations

from typing import Any


def _dedupe_preserve_order(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return tuple(out)


def _collect_expected_actual(section: Any) -> list[str]:
    if not isinstance(section, dict):
        return []
    expected = section.get("expected") or []
    actual = section.get("actual") or []
    return [
        str(x).strip()
        for x in list(expected) + list(actual)
        if str(x).strip()
    ]


def extract_aggregation_terms(sql_diff: Any) -> tuple[str, ...]:
    """Return de-duplicated aggregation atoms from
    ``sql_diff['aggregations'].{expected,actual}``.

    Order: expected atoms first (in source order), then actual atoms
    not already seen. Empty input or missing section returns ``()``.
    """
    if not isinstance(sql_diff, dict):
        return ()
    atoms = _collect_expected_actual(sql_diff.get("aggregations"))
    return _dedupe_preserve_order(atoms)


def extract_filter_terms(sql_diff: Any) -> tuple[str, ...]:
    """Return de-duplicated filter / WHERE-predicate atoms from
    ``sql_diff['filters'].{expected,actual}``."""
    if not isinstance(sql_diff, dict):
        return ()
    atoms = _collect_expected_actual(sql_diff.get("filters"))
    return _dedupe_preserve_order(atoms)
