"""Plan C2 — SQL-shape overlap helpers for the t2.4 counterfactual
scan.

The pre-Plan-C version of ``_t24_counterfactual_scan`` flagged any
benchmark whose ``required_tables`` contained the proposal's target
table as a passing dependent. That over-counts the blast radius for
SQL-snippet patches: a benchmark mentioning ``orders`` is not at risk
just because we're adding a NEW snippet on ``orders`` — only if the
benchmark uses the SPECIFIC pattern the snippet introduces (a derived
column, a filter clause, a metric definition).

This module narrows the dependent match by extracting "shape tokens"
from the patch (the column name + identifier tokens parsed from the
snippet's SQL body, minus SQL keyword stopwords) and providing a
predicate that checks whether the benchmark uses any of those tokens.

Public surface
==============
  - ``extract_snippet_shape_tokens(patch) -> frozenset[str]``
  - ``benchmark_has_shape_overlap(benchmark, shape_tokens) -> bool``

The two are designed to compose with the existing table-name check in
``_t24_counterfactual_scan``: a benchmark is flagged ONLY when both
the table overlaps AND the shape overlaps. When the patch has no
shape tokens (e.g., non-snippet proposals like ``add_instruction``),
``extract_snippet_shape_tokens`` returns an empty frozenset; the
caller treats empty shape tokens as "shape check skipped, fall back
to table-only" so non-snippet patches keep their current behaviour.
"""
from __future__ import annotations

import re
from typing import Any

# SQL keywords and aggregate functions that would dominate the token
# set and defeat the gate's purpose. Lowercase, frozen, exact match.
_SQL_KEYWORD_STOPWORDS: frozenset[str] = frozenset({
    # Clauses
    "select", "from", "where", "group", "order", "having", "limit",
    "offset", "with", "union", "intersect", "except",
    # Joins
    "join", "left", "right", "inner", "outer", "cross", "natural", "on",
    "using",
    # Boolean operators
    "and", "or", "not",
    # Comparison
    "is", "in", "between", "like", "ilike", "regexp",
    # Sort
    "asc", "desc",
    # Distinct
    "distinct", "all",
    # Constants
    "true", "false", "null", "unknown",
    # Conditional
    "case", "when", "then", "else", "end", "if",
    # Aliasing
    "as",
    # Aggregates (the snippet body usually contains them; we want the
    # ARGUMENTS, not the function names)
    "sum", "avg", "count", "min", "max", "stddev", "variance",
    "approx_count_distinct", "first", "last", "median",
    # Window-related
    "over", "partition", "rows", "range", "preceding", "following",
    "unbounded", "current", "row",
    # Type-related stop tokens
    "cast", "int", "bigint", "string", "double", "float", "decimal",
    "boolean", "date", "timestamp", "varchar", "numeric",
    # Common helper tokens
    "into", "values", "set", "let", "for", "while",
})

# Minimum identifier length parsed from SQL body. Two-char tokens
# (``id``, ``ts``) are too noisy when free-floating in SQL but useful
# when they appear via the explicit ``column`` field.
_MIN_SQL_BODY_TOKEN_LENGTH: int = 3

_IDENTIFIER_REGEX = re.compile(r"[a-z_][a-z0-9_]*")


def extract_snippet_shape_tokens(patch: Any) -> frozenset[str]:
    """Return the set of lower-case shape tokens for ``patch``.

    A shape token represents an identifier that the snippet operates
    on. The token set is the union of:

      1. The patch's ``column`` field (if present and non-empty),
         normalised to lower case. Always included regardless of
         length — operators sometimes use 2-char column names (``id``,
         ``ts``).
      2. Identifiers parsed from the patch's ``sql`` body, filtered by:
         - length >= ``_MIN_SQL_BODY_TOKEN_LENGTH`` (3 chars),
         - not in ``_SQL_KEYWORD_STOPWORDS``.

    Returns an empty frozenset for non-dict input, for dicts missing
    both ``column`` and ``sql``, or when no identifier survives the
    filters. The caller (``_t24_counterfactual_scan``) treats empty
    shape tokens as "shape check skipped — fall back to table-only
    matching" so non-snippet patches retain their current behaviour.
    """
    if not isinstance(patch, dict):
        return frozenset()

    tokens: set[str] = set()

    column = str(patch.get("column") or "").strip().lower()
    if column:
        tokens.add(column)

    sql_body = str(patch.get("sql") or "").lower()
    if sql_body:
        for tok in _IDENTIFIER_REGEX.findall(sql_body):
            if len(tok) < _MIN_SQL_BODY_TOKEN_LENGTH:
                continue
            if tok in _SQL_KEYWORD_STOPWORDS:
                continue
            tokens.add(tok)

    return frozenset(tokens)
