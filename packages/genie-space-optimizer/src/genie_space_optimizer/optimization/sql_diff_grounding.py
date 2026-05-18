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


# ── Plan 4a (2026-05-18) — text-derived identifier mining ────────────


import re as _re


_BACKTICK_IDENT_RE = _re.compile(r"`([A-Za-z_][A-Za-z0-9_]{2,})`")
_UPPER_SNAKE_RE = _re.compile(r"\b([A-Z][A-Z0-9_]{2,}_[A-Z][A-Z0-9_]+)\b")
_LOWER_SNAKE_RE = _re.compile(r"\b([a-z][a-z0-9]*(?:_[a-z0-9]+)+)\b")

# Tokens that look like snake_case but are SQL keywords / noise.
_SQL_KEYWORD_DENYLIST = frozenset({
    "group_by",
    "order_by",
    "inner_join",
    "left_join",
    "right_join",
    "full_join",
    "outer_join",
    "select_from",
    "is_not_null",
    "is_null",
    "not_in",
    "not_like",
    "not_between",
})


def extract_sql_identifiers_from_text(text: str | None) -> frozenset[str]:
    """Plan 4a — mine SQL-shaped identifiers out of free-form fix prose.

    Three rules (in order of precision):
      1. Backtick-quoted identifiers — always accepted.
      2. Uppercase snake_case (``[A-Z][A-Z0-9_]{2,}_[A-Z][A-Z0-9_]+``)
         — typical column / view naming in the airline tape.
      3. Lowercase snake_case with at least one underscore and at
         least 6 characters total — typical column naming in the
         7now tape (``zone_vp_name``, ``time_window``).
    SQL keyword phrases ("group_by", "order_by", "is_not_null", ...)
    that match rule 3 are filtered out by a denylist.

    Empty / None input → empty frozenset. Pure function: no I/O,
    no LLM, byte-stable.
    """
    if not text:
        return frozenset()

    out: set[str] = set()
    for m in _BACKTICK_IDENT_RE.finditer(text):
        out.add(m.group(1))
    for m in _UPPER_SNAKE_RE.finditer(text):
        out.add(m.group(1))
    for m in _LOWER_SNAKE_RE.finditer(text):
        token = m.group(1)
        if token in _SQL_KEYWORD_DENYLIST:
            continue
        if len(token) < 6:
            continue
        out.add(token)
    return frozenset(out)
