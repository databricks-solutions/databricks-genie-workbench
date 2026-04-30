"""Shared table-naming heuristics for the optimizer.

Two pieces of the optimizer rely on table-naming conventions:

1. SQL Expression naming disambiguation (``optimizer.py``) — extracts a
   compact "domain" qualifier from identifiers like ``mv_<domain>_*``
   so display names like ``Month-to-Date Filter`` get prefixed (e.g.
   ``ORDERS Month-to-Date Filter``) when multiple fact tables in the
   same Genie Space could plausibly share a generic concept.
2. Identifier stem promotion (``preflight_synthesis.py``) — registers
   "soft stems" by stripping known leaf prefixes (``mv_``, ``vw_``,
   ``f_``, ``d_``, …) so the LLM emitting ``FROM dim_date`` can be
   deterministically rewritten to ``FROM cat.sch.mv_<domain>_dim_date``
   when the allowlist contains exactly one match.

Both behaviors are pattern-based, conservative (they no-op when the
convention does not match), and fully data-driven. There is no
hardcoded customer or domain in this module.

Customers using non-standard table-naming conventions can extend the
prefix vocabulary or supply explicit domain regexes via the
``GSO_DOMAIN_TABLE_PATTERNS`` environment variable (see
``domain_qualifier_from_identifier``).
"""

from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)


# ── Leaf prefix vocabulary ─────────────────────────────────────────────

# Single-segment leaf prefixes we strip when building "soft stems" for
# stem promotion. Covers the common medallion + Databricks-convention
# shapes seen in the wild:
#   mv_, vw_, dim_, fact_, agg_, tbl_  — Databricks medallion convention
#   f_, d_, stg_, raw_, br_, sl_, gld_ — short-form medallion variants
#   metric_, view_                     — descriptive prefixes
LEAF_SOFT_PREFIXES: tuple[str, ...] = (
    "mv_", "vw_", "dim_", "fact_", "agg_", "tbl_",
    "f_", "d_", "stg_", "raw_", "br_", "sl_", "gld_",
    "metric_", "view_",
)

# Two-segment leaf prefix regex (``mv_<domain>_tail``). Used to strip
# both the medallion qualifier AND a domain qualifier so a leaf like
# ``mv_orders_dim_date`` registers ``dim_date`` as a soft stem. Covers
# the same broadened prefix vocabulary as ``LEAF_SOFT_PREFIXES``.
LEAF_TWO_SEG_PREFIX: re.Pattern[str] = re.compile(
    r"^(?:mv|vw|f|d|stg|br|sl|gld|metric|view)_[a-z0-9]+_(?P<tail>.+)$",
    re.IGNORECASE,
)


# ── Domain qualifier extraction ────────────────────────────────────────

# Default pattern for extracting a compact ``<domain>`` qualifier from a
# leaf like ``mv_<domain>_*``. The match group MUST be named ``domain``.
# Supports the broadened prefix vocabulary so customers using
# ``vw_orders_*``, ``f_orders_*``, ``stg_orders_*``, etc. all benefit.
DEFAULT_DOMAIN_PREFIX_RE: re.Pattern[str] = re.compile(
    r"^(?:mv|vw|f|d|stg|br|sl|gld|metric|view)_(?P<domain>[A-Za-z0-9]+)_",
    re.IGNORECASE,
)


def _load_extra_domain_patterns() -> tuple[re.Pattern[str], ...]:
    """Parse ``GSO_DOMAIN_TABLE_PATTERNS`` into an ordered tuple.

    Each entry must be a regex with a named ``domain`` group. Comma-
    separated. Bad patterns are logged and skipped.
    """
    raw = os.environ.get("GSO_DOMAIN_TABLE_PATTERNS", "").strip()
    if not raw:
        return ()
    out: list[re.Pattern[str]] = []
    for fragment in raw.split(","):
        fragment = fragment.strip()
        if not fragment:
            continue
        try:
            pattern = re.compile(fragment, re.IGNORECASE)
        except re.error as exc:
            logger.warning(
                "naming: ignoring malformed regex in GSO_DOMAIN_TABLE_PATTERNS: %r (%s)",
                fragment,
                exc,
            )
            continue
        if "domain" not in pattern.groupindex:
            logger.warning(
                "naming: pattern %r in GSO_DOMAIN_TABLE_PATTERNS lacks a (?P<domain>...) group; skipping.",
                fragment,
            )
            continue
        out.append(pattern)
    return tuple(out)


_EXTRA_DOMAIN_PATTERNS = _load_extra_domain_patterns()


def domain_qualifier_from_identifier(identifier: str) -> str:
    """Return the compact source qualifier for ``identifier``, or ``""``.

    Resolution order:

    1. Any user-supplied regex from ``GSO_DOMAIN_TABLE_PATTERNS`` (in
       declaration order).
    2. The default ``mv|vw|f|d|stg|...|view_<domain>_*`` pattern.

    The result is upper-cased. Generic table names (no recognised
    prefix) return ``""`` so callers do not add noisy artificial
    prefixes to a plain ``cat.sch.fact_orders`` table.
    """
    ident = (identifier or "").strip()
    if not ident:
        return ""
    short = ident.rsplit(".", 1)[-1]
    for pattern in _EXTRA_DOMAIN_PATTERNS:
        match = pattern.match(short)
        if match:
            try:
                return match.group("domain").upper()
            except (IndexError, KeyError):
                continue
    match = DEFAULT_DOMAIN_PREFIX_RE.match(short)
    if not match:
        return ""
    return match.group("domain").upper()


def schema_qualifier_from_identifier(
    identifier: str,
    *,
    distinct_schemas: int,
) -> str:
    """Fallback qualifier derived from the schema name when no leaf
    prefix matches.

    Only emits a qualifier when ``distinct_schemas >= 2`` so a single-
    schema space does not get a redundant ``SALES `` prefix on every
    SQL Expression. Returns the upper-cased schema name (last segment
    of the dotted path before the leaf), trimmed of generic suffixes
    that would not actually disambiguate (``default``, ``public``).

    Returns ``""`` when no fallback should be applied.
    """
    if distinct_schemas < 2:
        return ""
    ident = (identifier or "").strip()
    parts = ident.split(".")
    if len(parts) < 3:
        return ""
    schema = parts[-2].strip()
    if not schema:
        return ""
    if schema.lower() in {"default", "public", "main"}:
        return ""
    return schema.upper()
