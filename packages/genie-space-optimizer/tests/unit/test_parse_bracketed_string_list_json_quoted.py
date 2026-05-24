"""Trial 13k — ``_parse_bracketed_string_list`` must JSON-parse
bracket-wrapped strings whose elements are quoted.

The Trial 13j live workbench runs surfaced this bug: production ASI
rows emit ``metadata/<judge>/blame_set`` as a JSON-stringified list
(e.g. ``'["zone_name", "region_director_name"]'``). The legacy
parser stripped only the outer brackets and split on ``,`` without
removing the surrounding ``"`` characters, leaving every token with
literal quote characters attached. ``_is_bare_identifier`` then
rejected each token and ``_normalize_seeds_to_fqn`` dropped the
entire seed set — every capture-lane QID terminated at
``evidence_card_empty:blame_set_empty`` even when its raw seeds were
unique-suffix matches against ``schema_columns``.

The Trial 13k fix tries ``json.loads`` first for bracket-wrapped
inputs and falls back to a comma-split path that strips one matching
pair of surrounding ``'`` / ``"`` characters per piece. This keeps
the legacy ``"[a, b]"`` shape working while correcting the
JSON-stringified shape and the Python-repr ``"['a', 'b']"`` shape.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.eval_row_access import (
    _parse_bracketed_string_list,
    _strip_one_pair_of_quotes,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Headline Trial 13k case: JSON-stringified list with double-quoted
        # elements. Previously returned ['"zone_name"', '"region_director_name"'].
        (
            '["zone_name", "region_director_name", "cy_cust_count"]',
            ["zone_name", "region_director_name", "cy_cust_count"],
        ),
        # Python-repr / single-quote shape — falls into the comma-split
        # fallback because ``json.loads`` rejects single quotes.
        ("['a', 'b']", ["a", "b"]),
        # Legacy unquoted CSV shape — must remain unchanged.
        ("[a, b]", ["a", "b"]),
        # JSON with embedded commas inside quoted strings: ``json.loads``
        # preserves them, the legacy comma-split would have shredded them.
        ('["a, b", "c"]', ["a, b", "c"]),
        # Empty bracketed list.
        ("[]", []),
        # No brackets, plain CSV — unchanged path.
        ("a, b", ["a", "b"]),
        # Mixed-quote shape: outer JSON-style but one element single-quoted.
        # ``json.loads`` rejects -> fallback strips the surrounding quote.
        ("[\"a\", 'b']", ["a", "b"]),
        # Single-element JSON list with a SQL fragment (compound text):
        # parser returns the fragment intact; the normalizer is responsible
        # for dropping it.
        (
            '["PAYMENT_CURRENCY_CD = \'USD\' filter incorrectly added"]',
            ["PAYMENT_CURRENCY_CD = 'USD' filter incorrectly added"],
        ),
        # Empty / whitespace input.
        ("", []),
        ("   ", []),
        # Single bare token without brackets, no commas.
        ("solo", ["solo"]),
    ],
)
def test_parser_handles_known_shapes(raw: str, expected: list[str]) -> None:
    assert _parse_bracketed_string_list(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('"value"', "value"),
        ("'value'", "value"),
        ("value", "value"),
        ("", ""),
        ("'", "'"),  # single char — no matching pair to strip
        ('"mismatched\'', '"mismatched\''),
    ],
)
def test_strip_one_pair_of_quotes(raw: str, expected: str) -> None:
    assert _strip_one_pair_of_quotes(raw) == expected


def test_parser_recovers_bare_identifiers_for_normalizer() -> None:
    """Smoke test: the corrected parser feeds bare identifiers that the
    FQN normalizer can actually resolve (Trial 13k v Trial 13j gap)."""
    from genie_space_optimizer.optimization.schema_columns import (
        _normalize_seeds_to_fqn,
    )

    raw = '["zone_name", "region_director_name", "cy_cust_count"]'
    schema_columns = (
        "prashanth_subrahmanyam_catalog.sales_reports."
        "mv_esr_dim_location.zone_name",
        "prashanth_subrahmanyam_catalog.sales_reports."
        "mv_esr_dim_location.region_director_name",
        "prashanth_subrahmanyam_catalog.sales_reports."
        "mv_7now_fact_sales.cy_cust_count",
    )

    seeds = _parse_bracketed_string_list(raw)
    resolved, normalized, dropped = _normalize_seeds_to_fqn(
        seeds, schema_columns
    )

    assert seeds == ["zone_name", "region_director_name", "cy_cust_count"]
    assert normalized == 3
    assert dropped == 0
    assert resolved == list(schema_columns)
