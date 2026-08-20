"""Deterministic repair for uniquely stemmed SQL asset identifiers."""

from __future__ import annotations

import re

from genie_space_optimizer.common.naming import (  # noqa: E402 — sibling helper
    LEAF_SOFT_PREFIXES as _LEAF_SOFT_PREFIXES,
    LEAF_TWO_SEG_PREFIX as _LEAF_TWO_SEG_PREFIX,
)

def _build_stem_map(canonical: list[str]) -> dict[str, str]:
    """Build a ``stem_lower → canonical`` map containing only stems that
    resolve UNIQUELY to a single canonical identifier.

    Ambiguous stems (e.g. two tables with the same trailing
    underscore-delimited suffix across different schemas) are dropped
    so a false-positive rewrite is impossible — the LLM retry handles
    those via the feedback path.

    Three stem classes, each registered into the same map. The
    uniqueness check on the way out is what guarantees correctness:

    * **Hard stems** — every suffix of the dotted identifier path
      (``mv_<domain>_dim_date``, ``schema.mv_<domain>_dim_date``,
      full FQ). These are always safe rewrites when unique because
      they differ only in qualifier scope.
    * **Prefix-strip soft stems** — leaf with a medallion or
      two-segment leaf prefix stripped (``dim_date`` from
      ``mv_<domain>_dim_date``). This is the shape observed most
      often in the production log.
    * **Underscore-suffix soft stems** — every suffix of the leaf
      after an underscore boundary (``other_dim_date`` registers
      ``dim_date`` and ``date``). This makes the uniqueness check
      aware of conceptual overlap: two canonicals that share a
      trailing underscore-delimited component will both register the
      same suffix, so neither gets rewritten deterministically — the
      LLM retry must decide.
    """
    stems: dict[str, list[str]] = {}

    def _register(stem: str, full: str) -> None:
        key = stem.lower()
        if not key:
            return
        bucket = stems.setdefault(key, [])
        if full not in bucket:
            bucket.append(full)

    for full in canonical:
        parts = full.split(".")
        # Hard stems: every suffix of the dotted path.
        for i in range(1, len(parts) + 1):
            _register(".".join(parts[-i:]), full)
        leaf = parts[-1]
        # Single-segment soft stems (strip one known prefix).
        for prefix in _LEAF_SOFT_PREFIXES:
            if leaf.lower().startswith(prefix):
                _register(leaf[len(prefix):], full)
        # Two-segment soft stem (mv_<domain>_, vw_<domain>_, ...).
        m = _LEAF_TWO_SEG_PREFIX.match(leaf)
        if m:
            _register(m.group("tail"), full)
        # Underscore-suffix soft stems: every trailing substring of
        # the leaf that starts at an underscore boundary. This is
        # what makes ``other_dim_date`` contribute ``dim_date`` and
        # ``date`` to the map — so the ``dim_date`` stem becomes
        # ambiguous with ``mv_<domain>_dim_date`` (which also
        # registers ``dim_date`` via the two-segment prefix strip)
        # and no rewrite fires.
        underscore_positions = [i for i, ch in enumerate(leaf) if ch == "_"]
        for pos in underscore_positions:
            tail = leaf[pos + 1:]
            if tail:
                _register(tail, full)

    return {s: v[0] for s, v in stems.items() if len(v) == 1}

def repair_stemmed_identifiers_in_sql(
    sql: str, canonical: list[str],
) -> tuple[str, list[tuple[str, str]]]:
    """Flat-input counterpart of :func:`_repair_stemmed_identifiers`.

    Takes a SQL string and a list of canonical fully-qualified
    identifiers (tables + metric views), returns
    ``(rewritten_sql, [(before, after), ...])``.

    Extracted so the unified correction pipeline in ``evaluation.py``
    — which operates on corrected candidates (flat ``expected_sql``
    strings) without an ``AssetSlice`` — can apply the exact same
    deterministic repair. The preflight wrapper
    :func:`_repair_stemmed_identifiers` delegates here so both
    pipelines stay in lockstep.

    A substitution fires only when the stemmed token appears as a
    unique stem of EXACTLY ONE canonical identifier. Ambiguous stems
    are left untouched so the LLM retry can disambiguate with
    business context — no deterministic wrong answer.
    """
    if not sql or not canonical:
        return sql, []

    unique_stems = _build_stem_map(canonical)
    if not unique_stems:
        return sql, []

    # Longest-first so ``schema.mv_<domain>_dim_date`` wins over
    # ``dim_date`` when both are present in the SQL. This keeps the
    # final SQL as close to the LLM's intent as possible.
    #
    # Two complementary passes per stem:
    #
    #   * Pass A — bare table reference (``FROM dim_date``,
    #     ``JOIN dim_date d``). Lookahead ``(?![\w.])`` blocks
    #     ``dim_date_extra`` (substring) and ``dim_date.col``
    #     (qualifier — handled by Pass B instead).
    #   * Pass B — table qualifier before a column reference
    #     (``dim_date.day_of_week``). This is the dominant LLM error
    #     shape in production: the model treats the stem as a table
    #     alias before a dotted column. The lookahead ``(?=\.\w)``
    #     requires a literal dot followed by a word char, so a
    #     malformed trailing-dot ``dim_date.`` does not match. The
    #     lookbehind is unchanged across both passes so an
    #     alias-qualified column ``t.dim_date`` is still skipped
    #     (``t`` is ``\w`` and blocks ``(?<![\w.])``).
    #
    # The two passes are disjoint by construction (Pass A excludes
    # next ``.`` via ``(?![\w.])``; Pass B requires next ``.`` via
    # ``(?=\.\w)``) so order between them does not matter and they
    # never double-rewrite the same span.
    subs: list[tuple[str, str]] = []
    new_sql = sql
    for stem in sorted(unique_stems, key=len, reverse=True):
        canonical_name = unique_stems[stem]
        if stem == canonical_name.lower():
            # Already the canonical form — nothing to do.
            continue
        pass_a_pattern = rf"(?<![\w.]){re.escape(stem)}(?![\w.])"
        pass_b_pattern = rf"(?<![\w.]){re.escape(stem)}(?=\.\w)"
        replaced_this_round: list[str] = []

        def _repl(
            match: re.Match,
            _canon: str = canonical_name,
            _stem: str = stem,
            _log: list[str] = replaced_this_round,
        ) -> str:
            _log.append(match.group(0))
            return _canon

        new_sql, _ = re.subn(
            pass_a_pattern, _repl, new_sql, flags=re.IGNORECASE,
        )
        new_sql, _ = re.subn(
            pass_b_pattern, _repl, new_sql, flags=re.IGNORECASE,
        )
        for orig in replaced_this_round:
            subs.append((orig, canonical_name))

    return new_sql, subs

