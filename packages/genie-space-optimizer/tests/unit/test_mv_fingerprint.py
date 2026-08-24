"""SQL canonicalization and fingerprinting for the metric view advisor.

The corpus below is the contract. ``DISCOUNTED_REVENUE_VARIANTS`` are spellings
of one measure that MUST collapse to a single fingerprint; ``NEAR_MISSES`` are
expressions a sloppier canonicalizer would merge with them and MUST NOT. Both
groups are named so a failure says which variant broke, not just that a set had
the wrong size.

Fingerprints depend on the pinned sqlglot parser, so this suite is only
meaningful under ``uv run --frozen``. ``test_sqlglot_version_matches_the_pin``
fails loudly rather than let an ambient interpreter produce plausible-looking
fingerprints from a different parser.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot

from genie_space_optimizer.optimization import mv_fingerprint as mf
from genie_space_optimizer.optimization.mv_fingerprint import (
    SHAPE_CONDITIONAL_COUNT,
    SHAPE_PCT_OF_TOTAL,
    SHAPE_RATIO,
    Provenance,
    canonicalize_expr,
    canonicalize_sql_ast,
    classify_shapes,
    corpus_scan,
    expr_fingerprint,
    extract_dimensions,
    extract_filters,
    extract_join_keys,
    extract_measures,
    shapes_in_statement,
    statement_grain,
)
from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

LINEITEM = "samples.tpch.lineitem"
ORDERS = "samples.tpch.orders"

DISCOUNTED_REVENUE = "SUM(l_extendedprice * (1 - l_discount))"

# ── Positive collapse set: one measure, eight spellings ──────────────────

DISCOUNTED_REVENUE_VARIANTS: dict[str, str] = {
    "bare": f"SELECT {DISCOUNTED_REVENUE} AS revenue FROM {LINEITEM}",
    "alias_l": (
        f"SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS rev FROM {LINEITEM} AS l"
    ),
    "alias_li": (
        f"SELECT SUM(li.l_extendedprice*(1-li.l_discount)) FROM {LINEITEM} li"
    ),
    "qualified_by_table_name": (
        f"SELECT SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) FROM {LINEITEM}"
    ),
    "upper_case_and_whitespace": (
        f"select   SUM( L_EXTENDEDPRICE  *  ( 1 - L_DISCOUNT ) )\n  from {LINEITEM.upper()}"
    ),
    "comments": (
        f"SELECT /* revenue */ {DISCOUNTED_REVENUE} -- discounted\nFROM {LINEITEM}"
    ),
    "backtick_quoted": (
        f"SELECT SUM(`l_extendedprice` * (1 - `l_discount`)) AS `Revenue` FROM {LINEITEM}"
    ),
    "with_filters_and_grouping": (
        f"SELECT l_returnflag, {DISCOUNTED_REVENUE} AS revenue FROM {LINEITEM} "
        "WHERE l_shipdate >= '1994-01-01' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
    ),
}

# ── Negative set: differences that must survive canonicalization ─────────

NEAR_MISSES: dict[str, str] = {
    "tax_instead_of_discount": "SUM(l_extendedprice * (1 - l_tax))",
    "no_discount_factor": "SUM(l_extendedprice)",
    "plus_instead_of_minus": "SUM(l_extendedprice * (1 + l_discount))",
    "avg_instead_of_sum": "AVG(l_extendedprice * (1 - l_discount))",
    "quantity_instead_of_price": "SUM(l_quantity * (1 - l_discount))",
    "extra_tax_factor": "SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))",
    "count_distinct_orders": "COUNT(DISTINCT l_orderkey)",
    "discount_alone": "SUM(l_discount)",
}

JOIN_QUERY = (
    f"SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
    f"FROM {LINEITEM} AS l, {ORDERS} AS o "
    "WHERE l.l_orderkey = o.o_orderkey AND o.o_orderstatus = 'F'"
)

EXPLICIT_JOIN_QUERY = (
    f"SELECT o.o_orderpriority, COUNT(*) AS cnt FROM {ORDERS} AS o "
    f"JOIN {LINEITEM} AS l ON o.o_orderkey = l.l_orderkey "
    "WHERE o.o_orderdate >= '1993-07-01' GROUP BY 1"
)

RATIO_QUERY = (
    f"SELECT l_returnflag, SUM(l_extendedprice) / SUM(l_quantity) AS avg_unit_price "
    f"FROM {LINEITEM} GROUP BY l_returnflag"
)

CONDITIONAL_COUNT_QUERY = (
    f"SELECT SUM(CASE WHEN l_returnflag = 'R' THEN 1 ELSE 0 END) AS returned FROM {LINEITEM}"
)

CONDITIONAL_COUNT_NO_ELSE = (
    f"SELECT SUM(CASE WHEN l_returnflag = 'R' THEN 1 END) AS returned FROM {LINEITEM}"
)

PCT_OF_TOTAL_QUERY = (
    "SELECT l_returnflag, SUM(l_extendedprice) / SUM(SUM(l_extendedprice)) OVER () AS pct "
    f"FROM {LINEITEM} GROUP BY l_returnflag"
)

PII_QUERY = (
    "SELECT COUNT(1) FROM samples.tpch.customer "
    "WHERE c_comment = 'jdoe@example.com' AND c_acctbal = 1234.56 "
    "AND c_phone = '123-45-6789' AND c_name LIKE '%Smith%'"
)

# ── History-derived corpus (firewall, MV-D10(b)) ─────────────────────────
#
# The D producer (``optimization/mv_signals.demand_signal``) feeds raw
# ``system.query.history`` ``statement_text`` through ``corpus_scan`` to derive
# demand. That text is real user SQL carrying literals a user would actually
# type — emails, ids, phone numbers, LIKE patterns, dates. The firewall contract
# is that canonicalization erases every one before a fingerprint is formed, so
# nothing history-derived can reach a comment, display_name, synonym or any
# shipped surface. These are realistic history shapes (joins, IN-lists, a CTE)
# whose literals must not survive.
HISTORY_DERIVED: dict[str, str] = {
    "email_filter": (
        f"SELECT {DISCOUNTED_REVENUE} AS revenue FROM {LINEITEM} l "
        f"JOIN {ORDERS} o ON l.l_orderkey = o.o_orderkey "
        "WHERE o.o_comment = 'urgent: jdoe@example.com' AND l.l_shipdate >= '1995-06-01'"
    ),
    "in_list_of_ids": (
        f"SELECT COUNT(*) FROM {ORDERS} WHERE o_orderkey IN (12345, 67890, 111213)"
    ),
    "cte_with_phone": (
        "WITH flagged AS (SELECT * FROM samples.tpch.customer "
        "WHERE c_phone = '123-45-6789') "
        "SELECT COUNT(1) FROM flagged WHERE c_acctbal > 1000.50"
    ),
    "like_pattern": (
        f"SELECT l_returnflag, {DISCOUNTED_REVENUE} FROM {LINEITEM} "
        "WHERE l_comment LIKE '%Smith, John%' GROUP BY 1"
    ),
}

CORPUS: tuple[str, ...] = (
    *DISCOUNTED_REVENUE_VARIANTS.values(),
    JOIN_QUERY,
    EXPLICIT_JOIN_QUERY,
    RATIO_QUERY,
    CONDITIONAL_COUNT_QUERY,
    CONDITIONAL_COUNT_NO_ELSE,
    PCT_OF_TOTAL_QUERY,
    PII_QUERY,
    *HISTORY_DERIVED.values(),
    f"SELECT MIN(l_shipdate), MAX(l_shipdate) FROM {LINEITEM}",
)


def test_corpus_is_at_least_fifteen_statements() -> None:
    assert len(CORPUS) >= 15


def test_sqlglot_version_matches_the_pin() -> None:
    """A fingerprint is only comparable against one parser version.

    Canonical output can change across sqlglot patch releases, so a suite run on
    an ambient interpreter carrying a different version proves nothing about the
    version that ships. This asserts the pin in the package manifest.
    """
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    pinned = re.search(r'"sqlglot==([0-9][^"]*)"', pyproject.read_text(encoding="utf-8"))
    assert pinned is not None, "sqlglot pin not found in pyproject.toml"
    assert sqlglot.__version__ == pinned.group(1)


# ── Collapse ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("variant", sorted(DISCOUNTED_REVENUE_VARIANTS))
def test_every_discounted_revenue_variant_collapses_to_the_base(variant: str) -> None:
    measures = extract_measures(DISCOUNTED_REVENUE_VARIANTS[variant])
    base = expr_fingerprint(DISCOUNTED_REVENUE)
    assert [m.fingerprint for m in measures if m.fingerprint == base], (
        f"{variant} did not collapse: {[m.canonical_expr for m in measures]}"
    )


def test_the_whole_variant_set_yields_exactly_one_measure_fingerprint() -> None:
    fingerprints = {
        extract_measures(sql)[0].fingerprint for sql in DISCOUNTED_REVENUE_VARIANTS.values()
    }
    assert len(fingerprints) == 1


def test_canonical_measure_expr_is_alias_free_and_literal_free() -> None:
    measure = extract_measures(DISCOUNTED_REVENUE_VARIANTS["alias_li"])[0]
    assert measure.canonical_expr == "sum(l_extendedprice * (?n - l_discount))"
    assert measure.aggregate == "SUM"
    assert measure.source_columns == ("l_discount", "l_extendedprice")
    assert measure.source_tables == (LINEITEM,)


# ── Near misses ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", sorted(NEAR_MISSES))
def test_near_miss_does_not_collapse_into_discounted_revenue(name: str) -> None:
    assert expr_fingerprint(NEAR_MISSES[name]) != expr_fingerprint(DISCOUNTED_REVENUE)


def test_near_misses_are_pairwise_distinct() -> None:
    fingerprints = {
        name: expr_fingerprint(expr)
        for name, expr in {**NEAR_MISSES, "discounted_revenue": DISCOUNTED_REVENUE}.items()
    }
    collisions = [
        (a, b)
        for a in fingerprints
        for b in fingerprints
        if a < b and fingerprints[a] == fingerprints[b]
    ]
    assert not collisions


def test_literal_only_difference_collapses_by_design() -> None:
    """Documented consequence of the firewall, not a defect.

    The firewall forbids literals in canonical output, so two expressions that
    differ only in a literal value are one shape. A generator must recover
    concrete values from profiling — there is nothing left in the fingerprint to
    read them out of.
    """
    assert expr_fingerprint("SUM(l_extendedprice * (2 - l_discount))") == expr_fingerprint(
        DISCOUNTED_REVENUE
    )


# ── Statement-level canonicalization ─────────────────────────────────────


def test_predicate_order_and_literal_values_collapse() -> None:
    first = (
        f"SELECT SUM(l_extendedprice) FROM {LINEITEM} "
        "WHERE l_shipdate >= '1994-01-01' AND l_returnflag = 'N' AND l_quantity > 5"
    )
    second = (
        f"SELECT SUM(l_extendedprice) FROM {LINEITEM} "
        "WHERE l_quantity > 24 AND l_returnflag = 'R' AND l_shipdate >= '1996-12-31'"
    )
    assert canonicalize_sql_ast(first) == canonicalize_sql_ast(second)


def test_parenthesized_conjunction_flattens_into_the_chain() -> None:
    flat = f"SELECT 1 FROM {LINEITEM} WHERE l_quantity > 5 AND l_discount > 1 AND l_tax > 1"
    nested = f"SELECT 1 FROM {LINEITEM} WHERE (l_discount > 1 AND l_tax > 1) AND l_quantity > 5"
    assert canonicalize_sql_ast(flat) == canonicalize_sql_ast(nested)


def test_negated_conjunction_keeps_its_parentheses() -> None:
    """``NOT (a AND b)`` is not ``NOT a AND b``. Flattening here would invert
    the predicate, so the paren survives."""
    canonical = canonicalize_sql_ast(
        f"SELECT 1 FROM {LINEITEM} WHERE NOT (l_quantity > 5 AND l_tax > 1)"
    )
    assert "not (" in canonical


def test_relation_aliases_become_positional() -> None:
    canonical = canonicalize_sql_ast(JOIN_QUERY)
    assert " as t1" in canonical and " as t2" in canonical
    assert " as l" not in canonical and " as o" not in canonical


def test_group_by_ordinal_and_alias_reference_resolve_alike() -> None:
    ordinal = f"SELECT l_returnflag, SUM(l_extendedprice) AS rev FROM {LINEITEM} GROUP BY 1"
    named = (
        f"SELECT l_returnflag, SUM(l_extendedprice) AS revenue FROM {LINEITEM} "
        "GROUP BY l_returnflag"
    )
    assert canonicalize_sql_ast(ordinal) == canonicalize_sql_ast(named)
    assert statement_grain(ordinal) == statement_grain(named)
    assert [d.canonical_expr for d in extract_dimensions(ordinal)] == ["l_returnflag"]


def test_group_by_ordinal_survives_literal_erasure() -> None:
    """The ordinal is a numeric literal; resolving it must happen first or the
    grain dissolves into ``group by ?n``."""
    canonical = canonicalize_sql_ast(
        f"SELECT l_returnflag, SUM(l_extendedprice) FROM {LINEITEM} GROUP BY 1"
    )
    assert "group by l_returnflag" in canonical
    assert "group by ?n" not in canonical


def test_order_by_reference_does_not_double_count_a_measure() -> None:
    """``ORDER BY 2`` resolves to a copy of the projection. Counting both the
    projection and the reference would double every measure in a
    ``GROUP BY 1 ORDER BY 2`` statement."""
    measures = extract_measures(DISCOUNTED_REVENUE_VARIANTS["with_filters_and_grouping"])
    assert len(measures) == 1


def test_count_star_and_count_one_are_one_measure() -> None:
    star = extract_measures(f"SELECT COUNT(*) FROM {LINEITEM}")[0]
    one = extract_measures(f"SELECT COUNT(1) FROM {LINEITEM}")[0]
    assert star.fingerprint == one.fingerprint


def test_select_star_is_not_erased() -> None:
    assert canonicalize_sql_ast(f"SELECT * FROM {LINEITEM}") == f"select * from {LINEITEM} as t1"


def test_boolean_literals_survive() -> None:
    canonical = canonicalize_sql_ast(f"SELECT 1 FROM {LINEITEM} WHERE l_is_current = TRUE")
    assert "true" in canonical


# ── Dimensions, filters, join keys ───────────────────────────────────────


def test_derived_group_key_is_marked_as_an_expression() -> None:
    dimensions = extract_dimensions(
        f"SELECT DATE_TRUNC('month', l_shipdate) AS m, SUM(l_quantity) FROM {LINEITEM} GROUP BY 1"
    )
    assert len(dimensions) == 1
    assert dimensions[0].is_expression is True
    assert dimensions[0].source_columns == ("l_shipdate",)
    assert "'" not in dimensions[0].canonical_expr


def test_time_grain_survives_as_part_of_the_function_name() -> None:
    """The date-part unit is grammar, not data.

    Erasing it with the literals would merge a monthly grain with a daily one;
    leaving it quoted would put a token in the canonical form that no firewall
    grep can distinguish from a value. It is folded into the name instead.
    """
    monthly = canonicalize_expr("DATE_TRUNC('month', l_shipdate)")
    daily = canonicalize_expr("DATE_TRUNC('day', l_shipdate)")
    assert monthly != daily
    assert "'" not in monthly
    assert "month" in monthly and "l_shipdate" in monthly


def test_time_grain_is_case_insensitive() -> None:
    assert canonicalize_expr("DATE_TRUNC('MONTH', l_shipdate)") == canonicalize_expr(
        "date_trunc('month', l_shipdate)"
    )


def test_ungrouped_statement_has_no_grain() -> None:
    assert statement_grain(f"SELECT SUM(l_quantity) FROM {LINEITEM}") == ""


def test_filters_exclude_the_join_equality() -> None:
    filters = extract_filters(JOIN_QUERY)
    assert [f.canonical_expr for f in filters] == ["o_orderstatus = ?s"]
    assert all(f.clause == "where" for f in filters)


def test_having_conjuncts_are_labelled() -> None:
    filters = extract_filters(
        f"SELECT l_returnflag, SUM(l_quantity) FROM {LINEITEM} "
        "GROUP BY 1 HAVING SUM(l_quantity) > 100"
    )
    assert [(f.clause, f.canonical_expr) for f in filters] == [("having", "sum(l_quantity) > ?n")]


def test_join_keys_come_from_both_on_clauses_and_comma_joins() -> None:
    implicit = extract_join_keys(JOIN_QUERY)
    explicit = extract_join_keys(EXPLICIT_JOIN_QUERY)
    assert [(k.left, k.right, k.origin) for k in implicit] == [
        (f"{LINEITEM}.l_orderkey", f"{ORDERS}.o_orderkey", "where")
    ]
    assert [(k.left, k.right, k.origin) for k in explicit] == [
        (f"{LINEITEM}.l_orderkey", f"{ORDERS}.o_orderkey", "on")
    ]
    assert implicit[0].fingerprint == explicit[0].fingerprint


def test_join_key_orientation_does_not_change_the_fingerprint() -> None:
    flipped = JOIN_QUERY.replace(
        "l.l_orderkey = o.o_orderkey", "o.o_orderkey = l.l_orderkey"
    )
    assert extract_join_keys(flipped)[0].fingerprint == extract_join_keys(JOIN_QUERY)[0].fingerprint


def test_same_table_equality_is_a_filter_not_a_join_key() -> None:
    sql = f"SELECT 1 FROM {LINEITEM} AS l WHERE l.l_orderkey = l.l_linenumber"
    assert extract_join_keys(sql) == ()
    assert len(extract_filters(sql)) == 1


def test_unqualified_column_in_a_multi_table_statement_is_flagged() -> None:
    """Guessing which table a bare column came from is how a measure gets
    proposed against the wrong source, so the ambiguity is reported instead."""
    measure = extract_measures(
        f"SELECT SUM(l_extendedprice) FROM {LINEITEM} AS l, {ORDERS} AS o "
        "WHERE l.l_orderkey = o.o_orderkey"
    )[0]
    assert measure.has_unresolved_columns is True
    assert measure.source_tables == ()


def test_qualified_measure_in_a_join_resolves_to_one_table() -> None:
    measure = extract_measures(JOIN_QUERY)[0]
    assert measure.source_tables == (LINEITEM,)
    assert measure.has_unresolved_columns is False


# ── Shapes ───────────────────────────────────────────────────────────────


def test_ratio_shape_emits_atomic_components() -> None:
    (shape,) = shapes_in_statement(RATIO_QUERY)
    assert shape.kind == SHAPE_RATIO
    assert dict(shape.components) == {
        "numerator": "sum(l_extendedprice)",
        "denominator": "sum(l_quantity)",
    }
    assert "MEASURE()-composed" in shape.guidance


def test_conditional_count_shape_carries_the_filter_rewrite() -> None:
    (shape,) = shapes_in_statement(CONDITIONAL_COUNT_QUERY)
    assert shape.kind == SHAPE_CONDITIONAL_COUNT
    assert dict(shape.components)["rewrite"] == "count(1) filter (where l_returnflag = ?s)"
    assert shape.target_form == "count(1) filter (where l_returnflag = ?s)"
    assert "COUNT(1) FILTER" in shape.guidance


def test_conditional_count_is_one_shape_with_or_without_else_zero() -> None:
    with_else = shapes_in_statement(CONDITIONAL_COUNT_QUERY)[0]
    without_else = shapes_in_statement(CONDITIONAL_COUNT_NO_ELSE)[0]
    assert with_else.fingerprint == without_else.fingerprint
    assert with_else.canonical_expr != without_else.canonical_expr


def test_conditional_sum_of_a_column_is_not_a_conditional_count() -> None:
    """``THEN l_quantity`` has no ``COUNT(1) FILTER`` form, so the shape must not
    claim it."""
    shapes = shapes_in_statement(
        f"SELECT SUM(CASE WHEN l_returnflag = 'R' THEN l_quantity END) FROM {LINEITEM}"
    )
    assert [s.kind for s in shapes] == []


def test_percent_of_total_is_not_classified_as_a_plain_ratio() -> None:
    """A windowed denominator read as a ratio becomes ``MEASURE()/MEASURE()``,
    which always evaluates to 1.0 — the exact defect MV-D8 forbids."""
    (shape,) = shapes_in_statement(PCT_OF_TOTAL_QUERY)
    assert shape.kind == SHAPE_PCT_OF_TOTAL
    assert dict(shape.components)["windowed_total"].endswith("over ()")
    assert "ANY_VALUE" in shape.guidance
    assert "NEVER MEASURE()/MEASURE()" in shape.guidance


def test_windowed_aggregate_is_reported_but_flagged() -> None:
    measures = extract_measures(PCT_OF_TOTAL_QUERY)
    assert any(m.is_windowed for m in measures)


def test_shape_to_dict_renders_components_as_a_mapping() -> None:
    payload = shapes_in_statement(RATIO_QUERY)[0].to_dict()
    assert payload["components"]["numerator"] == "sum(l_extendedprice)"
    assert payload["kind"] == SHAPE_RATIO


# ── Firewall ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "literal",
    ["jdoe@example.com", "123-45-6789", "1234.56", "%Smith%"],
)
def test_no_literal_value_survives_canonicalization(literal: str) -> None:
    canonical = canonicalize_sql_ast(PII_QUERY)
    assert literal not in canonical


def test_canonical_form_contains_placeholders_and_no_quotes() -> None:
    canonical = canonicalize_sql_ast(PII_QUERY)
    assert "'" not in canonical and '"' not in canonical
    assert mf.STRING_PLACEHOLDER in canonical
    assert mf.NUMERIC_PLACEHOLDER in canonical
    assert not re.search(r"\d", canonical.replace("t1", "")), canonical


def test_email_shaped_literal_produces_a_placeholder_only_fingerprint() -> None:
    with_pii = "SELECT COUNT(1) FROM samples.tpch.customer WHERE c_comment = 'jdoe@example.com'"
    without = "SELECT COUNT(1) FROM samples.tpch.customer WHERE c_comment = 'x'"
    assert canonicalize_sql_ast(with_pii) == canonicalize_sql_ast(without)
    assert "@" not in canonicalize_sql_ast(with_pii)


@pytest.mark.parametrize("sql", CORPUS)
def test_no_corpus_statement_leaks_a_quoted_literal(sql: str) -> None:
    assert "'" not in canonicalize_sql_ast(sql)


@pytest.mark.parametrize(
    "sql", list(HISTORY_DERIVED.values()), ids=list(HISTORY_DERIVED)
)
def test_history_derived_statement_leaks_no_literal(sql: str) -> None:
    """MV-D10(b): the D producer's raw ``system.query.history`` input is
    literal-free once canonicalized, so nothing history-derived can reach a
    comment, display_name, synonym or any shipped surface. Asserts the specific
    values, not just the absence of a quote, so a canonicalizer that dropped the
    quotes but kept the payload would still fail."""
    canonical = canonicalize_sql_ast(sql)
    assert "'" not in canonical and '"' not in canonical
    assert "@" not in canonical
    for literal in (
        "jdoe@example.com",
        "123-45-6789",
        "Smith, John",
        "1995-06-01",
        "12345",
        "67890",
        "1000.50",
    ):
        assert literal not in canonical, f"{literal!r} survived in {canonical!r}"


# ── Corpus scan ──────────────────────────────────────────────────────────


def _entries() -> list[tuple[str, dict[str, str]]]:
    return [
        (
            sql,
            {"id": f"bmk_{index}", "kind": "benchmark", "seen_at": f"2026-08-{10 + index:02d}T00:00:00Z"},
        )
        for index, sql in enumerate(DISCOUNTED_REVENUE_VARIANTS.values())
    ]


def test_corpus_scan_counts_recurrence_and_distinct_provenance() -> None:
    scan = corpus_scan(_entries())
    top = scan.measures[0]
    assert top.recurrence == len(DISCOUNTED_REVENUE_VARIANTS)
    assert top.provenance_count == len(DISCOUNTED_REVENUE_VARIANTS)
    assert top.provenance_ids[0] == "bmk_0"
    assert top.first_seen == "2026-08-10T00:00:00Z"
    assert top.last_seen == "2026-08-17T00:00:00Z"
    assert top.source_tables == (LINEITEM,)
    assert top.kind == "measure"


def test_corpus_scan_counts_curated_provenance_as_a_subset_of_distinct(
) -> None:
    """MV-D17: only sources whose kind is ``curated`` raise the curated count.

    Three occurrences of one measure, two of them from curated sources. The
    distinct count is 3 (nothing changed there); the curated count is 2, and it
    is what ``mv_scoring`` reads to up-weight Y. A generated occurrence must not
    move it — otherwise the up-weight would fire for measures no human curated.
    """
    sql = DISCOUNTED_REVENUE_VARIANTS["bare"]
    scan = corpus_scan(
        [
            (sql, Provenance(id="bmk_0", kind="benchmark")),
            (sql, Provenance(id="trusted_asset:eq_1", kind=mf.CURATED_PROVENANCE_KIND)),
            (sql, Provenance(id="sql_snippet:measures:m1", kind=mf.CURATED_PROVENANCE_KIND)),
        ]
    )
    top = scan.measures[0]
    assert top.recurrence == 3
    assert top.provenance_count == 3
    assert top.curated_provenance_count == 2


def test_curated_count_ignores_a_curated_id_without_a_curated_kind() -> None:
    """The prefix is not the signal (MV-D17). An id that merely looks curated but
    carries no curated ``kind`` must not raise the curated count — curated-ness is
    a recorded kind, never an inference from ``provenance_ids``."""
    sql = DISCOUNTED_REVENUE_VARIANTS["bare"]
    scan = corpus_scan([(sql, Provenance(id="trusted_asset:eq_1", kind=""))])
    assert scan.measures[0].curated_provenance_count == 0


def test_fingerprint_recurrence_to_dict_key_set_is_pinned() -> None:
    """A frozen-contract pin so ``curated_provenance_count`` (and any later field)
    is a deliberate addition to persisted ``evidence_json``, not an accident."""
    scan = corpus_scan([(DISCOUNTED_REVENUE_VARIANTS["bare"], "bmk_0")])
    assert set(scan.measures[0].to_dict()) == {
        "fingerprint",
        "canonical_expr",
        "kind",
        "recurrence",
        "provenance_ids",
        "provenance_count",
        "curated_provenance_count",
        "first_seen",
        "last_seen",
        "source_columns",
        "source_tables",
        "shapes",
    }


@pytest.mark.parametrize("sql", CORPUS)
def test_no_curated_corpus_statement_leaks_a_quoted_literal(sql: str) -> None:
    """The firewall holds for the curated half too (MV-D17). Curated SQL routed
    through the scan is canonicalized by the same path, so a quoted literal a
    human wrote into a snippet or example is erased before it can reach a bucket's
    ``canonical_expr`` — the text that survives into persisted evidence."""
    scan = corpus_scan([(sql, {"id": "curated_1", "kind": mf.CURATED_PROVENANCE_KIND})])
    for bucket in (*scan.measures, *scan.dimensions, *scan.filters, *scan.join_keys):
        assert "'" not in bucket.canonical_expr


def test_corpus_scan_counts_parse_failures_without_raising() -> None:
    scan = corpus_scan([(DISCOUNTED_REVENUE_VARIANTS["bare"], "bmk_1"), ("NOT SQL AT ALL (((", "q_9")])
    assert scan.statements_scanned == 1
    assert scan.parse_failures == 1


def test_corpus_scan_accepts_string_provenance_and_dataclass_provenance() -> None:
    scan = corpus_scan(
        [
            (DISCOUNTED_REVENUE_VARIANTS["bare"], "bmk_1"),
            (DISCOUNTED_REVENUE_VARIANTS["alias_l"], Provenance(id="stmt_2", kind="history")),
            DISCOUNTED_REVENUE_VARIANTS["alias_li"],
        ]
    )
    assert scan.measures[0].recurrence == 3
    assert scan.measures[0].provenance_ids == ("bmk_1", "stmt_2")


def test_corpus_scan_is_deterministic() -> None:
    assert corpus_scan(_entries()).to_dict() == corpus_scan(_entries()).to_dict()


def test_corpus_scan_ranks_by_recurrence() -> None:
    scan = corpus_scan(list(CORPUS))
    counts = [m.recurrence for m in scan.measures]
    assert counts == sorted(counts, reverse=True)
    assert scan.measures[0].canonical_expr == "sum(l_extendedprice * (?n - l_discount))"


def test_corpus_scan_surfaces_dimensions_filters_and_join_keys() -> None:
    scan = corpus_scan(list(CORPUS))
    assert any(d.canonical_expr == "l_returnflag" for d in scan.dimensions)
    assert any(f.canonical_expr == "o_orderstatus = ?s" for f in scan.filters)
    assert any("l_orderkey" in j.canonical_expr for j in scan.join_keys)
    assert all("'" not in f.canonical_expr for f in scan.filters)


def test_classify_shapes_merges_across_the_corpus() -> None:
    shapes = classify_shapes(
        [
            (CONDITIONAL_COUNT_QUERY, {"id": "bmk_1", "seen_at": "2026-08-01T00:00:00Z"}),
            (CONDITIONAL_COUNT_NO_ELSE, {"id": "bmk_2", "seen_at": "2026-08-05T00:00:00Z"}),
            (RATIO_QUERY, {"id": "bmk_3", "seen_at": "2026-08-03T00:00:00Z"}),
        ]
    )
    by_kind = {shape.kind: shape for shape in shapes}
    assert by_kind[SHAPE_CONDITIONAL_COUNT].recurrence == 2
    assert by_kind[SHAPE_CONDITIONAL_COUNT].provenance_ids == ("bmk_1", "bmk_2")
    assert by_kind[SHAPE_CONDITIONAL_COUNT].first_seen == "2026-08-01T00:00:00Z"
    assert by_kind[SHAPE_CONDITIONAL_COUNT].last_seen == "2026-08-05T00:00:00Z"
    assert by_kind[SHAPE_RATIO].recurrence == 1


def test_measure_bucket_records_the_shapes_observed_on_it() -> None:
    scan = corpus_scan([(RATIO_QUERY, "bmk_1")])
    tagged = [m for m in scan.measures if SHAPE_RATIO in m.shapes]
    assert tagged, [m.to_dict() for m in scan.measures]


# ── MV-D10: two fingerprint levels, permanently distinct ─────────────────


def test_expr_fingerprint_is_not_the_candidate_dedup_key() -> None:
    measure = extract_measures(DISCOUNTED_REVENUE_VARIANTS["bare"])[0]
    candidate_key = mv_candidate_fingerprint("01ef_genie", measure.canonical_expr, [LINEITEM])
    assert measure.fingerprint != candidate_key
    assert len(candidate_key) == 64


def test_candidate_key_is_stable_across_measure_spellings() -> None:
    """The MV-D7 key consumes ``canonical_expr`` from here, so alias spelling
    cannot produce two candidate rows for one measure."""
    keys = {
        mv_candidate_fingerprint(
            "01ef_genie", extract_measures(sql)[0].canonical_expr, [LINEITEM]
        )
        for sql in DISCOUNTED_REVENUE_VARIANTS.values()
    }
    assert len(keys) == 1


def test_candidate_key_separates_spaces_and_source_sets() -> None:
    expr = extract_measures(DISCOUNTED_REVENUE_VARIANTS["bare"])[0].canonical_expr
    assert mv_candidate_fingerprint("space_a", expr, [LINEITEM]) != mv_candidate_fingerprint(
        "space_b", expr, [LINEITEM]
    )
    assert mv_candidate_fingerprint("space_a", expr, [LINEITEM]) != mv_candidate_fingerprint(
        "space_a", expr, [LINEITEM, ORDERS]
    )


def test_expr_fingerprint_appears_in_no_persistence_path() -> None:
    """MV-D10 is a contract, not a convention.

    ``expr_fingerprint`` is expression-grained: it collides across spaces and
    across source sets, so writing it to ``dedup_fingerprint`` or
    ``content_hash`` would silently merge unrelated candidates. This asserts no
    other module in the package references it, and that this module contains no
    Delta write vocabulary of its own.
    """
    src = Path(mf.__file__).resolve().parents[1]
    offenders = [
        path.relative_to(src).as_posix()
        for path in src.rglob("*.py")
        if path.name != "mv_fingerprint.py" and "expr_fingerprint" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []

    module_source = Path(mf.__file__).read_text(encoding="utf-8")
    for forbidden in ("merge_row", "dedup_fingerprint =", "content_hash =", "spark"):
        assert forbidden not in module_source, forbidden


# ── Robustness ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("sql", ["", "   ", None, "SELECT FROM WHERE ((("])
def test_unusable_input_yields_empty_results_rather_than_raising(sql: object) -> None:
    assert canonicalize_sql_ast(sql) == ""  # type: ignore[arg-type]
    assert expr_fingerprint(sql) == ""  # type: ignore[arg-type]
    assert extract_measures(sql) == ()  # type: ignore[arg-type]
    assert extract_dimensions(sql) == ()  # type: ignore[arg-type]
    assert extract_filters(sql) == ()  # type: ignore[arg-type]
    assert extract_join_keys(sql) == ()  # type: ignore[arg-type]
    assert shapes_in_statement(sql) == ()  # type: ignore[arg-type]


def test_cte_statements_canonicalize_without_leaking_aliases() -> None:
    sql = (
        f"WITH flagged AS (SELECT * FROM {LINEITEM} WHERE l_returnflag = 'R') "
        "SELECT SUM(l_extendedprice * (1 - l_discount)) FROM flagged"
    )
    assert extract_measures(sql)[0].fingerprint == expr_fingerprint(DISCOUNTED_REVENUE)
    assert "'" not in canonicalize_sql_ast(sql)


def test_canonicalize_expr_accepts_a_parsed_expression() -> None:
    parsed = sqlglot.parse_one(DISCOUNTED_REVENUE, read=mf.DIALECT)
    assert canonicalize_expr(parsed) == canonicalize_expr(DISCOUNTED_REVENUE)
