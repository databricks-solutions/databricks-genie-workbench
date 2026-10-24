"""The table resolver over the space's own identifiers (MV-D123, Rulings 2-4).

A spelled table name resolves to one lowercase, unquoted, three-part name, or is
table-less (it names no table), or is unresolved (the space cannot say which
table it means). Unresolved never matches; a table-less side matches only when
the caller says the sole-row rule holds.
"""

from __future__ import annotations

import pytest
from genie_space_optimizer.optimization.mv_fingerprint import source_table_name
from genie_space_optimizer.optimization.mv_tables import (
    TABLELESS,
    UNRESOLVED,
    TableResolver,
    fq_tables_overlap,
    same_fq_tables,
)

from genie_space_optimizer.optimization import mv_tables


def _config(*tables: str, metric_views: tuple[str, ...] = ()) -> dict:
    return {
        "data_sources": {
            "tables": [{"identifier": t} for t in tables],
            "metric_views": [{"identifier": m} for m in metric_views],
        }
    }


SPACE = _config("main.sales.orders", "main.sales.lineitem", "main.ref.nation")


# ── Resolution ───────────────────────────────────────────────────────────


def test_a_three_part_name_resolves_to_itself() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve("main.sales.orders") == "main.sales.orders"


def test_a_three_part_name_outside_the_space_still_resolves_to_itself() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve("other.sales.orders") == "other.sales.orders"


def test_a_two_part_name_resolves_to_the_one_identifier_ending_in_it() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve("sales.orders") == "main.sales.orders"
    assert resolver.resolve("ref.nation") == "main.ref.nation"


def test_a_one_part_name_resolves_to_the_one_identifier_with_that_leaf() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve("orders") == "main.sales.orders"
    assert resolver.resolve("nation") == "main.ref.nation"


@pytest.mark.parametrize(
    "spelling",
    [
        "MAIN.Sales.ORDERS",
        "`main`.`sales`.`orders`",
        "`MAIN`.sales.`Orders`",
        " main . sales . orders ",
        "Sales.`ORDERS`",
        "`sales`.`orders`",
        "ORDERS",
        "`orders`",
    ],
)
def test_backticks_and_case_do_not_change_the_resolved_name(spelling: str) -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve(spelling) == "main.sales.orders"


def test_a_space_identifier_spelled_with_backticks_and_case_is_indexed_normalized() -> (
    None
):
    resolver = TableResolver.from_config(_config("`Main`.`SALES`.Orders"))
    assert resolver.resolve("orders") == "main.sales.orders"
    assert resolver.resolve("sales.orders") == "main.sales.orders"


@pytest.mark.parametrize(
    "spelling",
    [
        "main.sales.orders",
        "MAIN.Sales.ORDERS",
        "`main`.`sales`.`orders`",
        "`Main`.`My Schema`.`Order Lines`",
        "`a``b`.c.d",
    ],
)
def test_a_resolved_three_part_name_equals_what_the_extractor_produces(
    spelling: str,
) -> None:
    assert TableResolver.from_config(SPACE).resolve(spelling) == source_table_name(
        spelling
    )
    assert TableResolver.from_config(None).resolve(spelling) == source_table_name(
        spelling
    )


@pytest.mark.parametrize(
    "spelling",
    [
        "`a.b`.c",
        "`main.sales`.orders",
        "`main.sales.orders`",
        "`orders",
        "main..orders",
        ".orders",
        "orders.",
        "x.main.sales.orders",
        "``.sales.orders",
    ],
)
def test_a_name_whose_parts_cannot_be_read_unambiguously_is_unresolved(
    spelling: str,
) -> None:
    assert TableResolver.from_config(SPACE).resolve(spelling) is UNRESOLVED
    assert TableResolver.from_config(None).resolve(spelling) is UNRESOLVED


def test_a_dotted_backticked_part_is_not_read_as_two_parts() -> None:
    resolver = TableResolver.from_config(_config("a.b.c"))
    assert resolver.resolve("`a.b`.c") is UNRESOLVED
    assert resolver.resolve("a.b.c") == "a.b.c"


def test_a_leaf_in_two_catalogs_is_unresolved() -> None:
    resolver = TableResolver.from_config(_config("c1.s.orders", "c2.s.orders"))
    assert resolver.resolve("orders") is UNRESOLVED
    assert resolver.resolve("s.orders") is UNRESOLVED
    assert resolver.resolve("c1.s.orders") == "c1.s.orders"
    assert resolver.resolve("c2.s.orders") == "c2.s.orders"


def test_a_two_part_name_tells_two_schemas_apart_when_the_leaf_cannot() -> None:
    resolver = TableResolver.from_config(_config("main.s1.orders", "main.s2.orders"))
    assert resolver.resolve("orders") is UNRESOLVED
    assert resolver.resolve("s1.orders") == "main.s1.orders"
    assert resolver.resolve("s2.orders") == "main.s2.orders"


def test_the_same_identifier_listed_twice_is_one_match() -> None:
    resolver = TableResolver.from_config(
        _config("main.sales.orders", "MAIN.SALES.`orders`")
    )
    assert resolver.resolve("orders") == "main.sales.orders"


def test_an_absent_leaf_is_unresolved() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve("customer") is UNRESOLVED
    assert resolver.resolve("sales.customer") is UNRESOLVED
    assert resolver.resolve("ref.orders") is UNRESOLVED


def test_a_cte_name_is_unresolved() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve("recent_orders") is UNRESOLVED


def test_metric_view_identifiers_are_read_too() -> None:
    resolver = TableResolver.from_config(
        _config("main.sales.orders", metric_views=("main.sales.revenue_mv",))
    )
    assert resolver.resolve("revenue_mv") == "main.sales.revenue_mv"
    assert resolver.resolve("sales.revenue_mv") == "main.sales.revenue_mv"


def test_a_metric_view_and_a_table_sharing_a_leaf_make_it_unresolved() -> None:
    resolver = TableResolver.from_config(
        _config("main.sales.orders", metric_views=("main.mv.orders",))
    )
    assert resolver.resolve("orders") is UNRESOLVED


@pytest.mark.parametrize("name", ["", "   ", None])
def test_no_name_is_tableless(name: str | None) -> None:
    assert TableResolver.from_config(SPACE).resolve(name) is TABLELESS
    assert TableResolver.from_config(None).resolve(name) is TABLELESS


def test_only_the_space_identifiers_are_read() -> None:
    config = {
        "data_sources": {
            "tables": [
                {"identifier": "main.sales.orders"},
                {"identifier": "sales.lineitem"},
                {"identifier": 7},
                {"name": "main.sales.customer"},
                "main.sales.nation",
            ],
            "metric_views": None,
        },
        "instructions": {
            "sql_snippets": {"measures": [{"sql": ["main.sales.region"]}]}
        },
        "_tables": ["main.sales.part"],
    }
    resolver = TableResolver.from_config(config)
    assert resolver.resolve("orders") == "main.sales.orders"
    for leaf in ("lineitem", "customer", "nation", "region", "part"):
        assert resolver.resolve(leaf) is UNRESOLVED


@pytest.mark.parametrize("config", [{}, {"data_sources": None}, {"data_sources": {}}])
def test_a_config_with_no_tables_resolves_only_three_part_names(config: dict) -> None:
    resolver = TableResolver.from_config(config)
    assert resolver.resolve("orders") is UNRESOLVED
    assert resolver.resolve("sales.orders") is UNRESOLVED
    assert resolver.resolve("main.sales.orders") == "main.sales.orders"


def test_a_config_that_is_not_a_mapping_is_refused() -> None:
    with pytest.raises(TypeError):
        TableResolver.from_config(["main.sales.orders"])  # type: ignore[arg-type]


# ── No table list: today's reading (Ruling 4) ────────────────────────────


@pytest.mark.parametrize(
    ("spelling", "expected"),
    [
        ("main.sales.orders", "main.sales.orders"),
        ("`MAIN`.Sales.`ORDERS`", "main.sales.orders"),
        ("sales.orders", "sales.orders"),
        ("Sales.`Orders`", "sales.orders"),
        ("orders", "orders"),
        ("`ORDERS`", "orders"),
        ("recent_orders", "recent_orders"),
    ],
)
def test_no_table_list_resolves_every_name_as_written(
    spelling: str, expected: str
) -> None:
    resolved = TableResolver.from_config(None).resolve(spelling)
    assert resolved == expected
    assert resolved == source_table_name(spelling)


@pytest.mark.parametrize(
    ("config", "expected"),
    [(None, False), ({}, True), ({"data_sources": {}}, True), (SPACE, True)],
)
def test_only_no_config_has_no_table_list(config, expected: bool) -> None:
    assert TableResolver.from_config(config).has_table_list is expected


# ── Many names ───────────────────────────────────────────────────────────


def test_resolve_all_returns_the_resolved_set() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve_all(["orders", "sales.lineitem"]) == frozenset(
        {"main.sales.orders", "main.sales.lineitem"}
    )


def test_resolve_all_collapses_two_spellings_of_one_table() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve_all(
        ["orders", "`MAIN`.sales.orders", "sales.orders"]
    ) == frozenset({"main.sales.orders"})


def test_resolve_all_is_unresolved_when_any_name_is() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve_all(["orders", "customer"]) is UNRESOLVED
    assert resolver.resolve_all(["main.sales.orders", "`a.b`.c"]) is UNRESOLVED


@pytest.mark.parametrize("names", [(), [], ["", "  "]])
def test_resolve_all_over_no_names_is_tableless(names) -> None:
    assert TableResolver.from_config(SPACE).resolve_all(names) is TABLELESS
    assert TableResolver.from_config(None).resolve_all(names) is TABLELESS


def test_resolve_all_skips_a_blank_beside_a_table() -> None:
    resolver = TableResolver.from_config(SPACE)
    assert resolver.resolve_all(["", "orders"]) == frozenset({"main.sales.orders"})


def test_resolve_all_refuses_a_bare_string() -> None:
    with pytest.raises(TypeError):
        TableResolver.from_config(SPACE).resolve_all("orders")


# ── Comparing table sets ─────────────────────────────────────────────────


ORDERS = frozenset({"main.sales.orders"})
LINEITEM = frozenset({"main.sales.lineitem"})
BOTH = ORDERS | LINEITEM


@pytest.mark.parametrize("tableless_matches", [True, False])
def test_same_fq_tables_compares_full_names(tableless_matches: bool) -> None:
    assert same_fq_tables(ORDERS, ORDERS, tableless_matches=tableless_matches)
    assert not same_fq_tables(ORDERS, BOTH, tableless_matches=tableless_matches)
    assert not same_fq_tables(ORDERS, LINEITEM, tableless_matches=tableless_matches)


@pytest.mark.parametrize("tableless_matches", [True, False])
def test_fq_tables_overlap_compares_full_names(tableless_matches: bool) -> None:
    assert fq_tables_overlap(ORDERS, BOTH, tableless_matches=tableless_matches)
    assert fq_tables_overlap(BOTH, BOTH, tableless_matches=tableless_matches)
    assert not fq_tables_overlap(ORDERS, LINEITEM, tableless_matches=tableless_matches)


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
@pytest.mark.parametrize("tableless_matches", [True, False])
def test_one_leaf_in_two_catalogs_is_two_tables(
    helper, tableless_matches: bool
) -> None:
    assert not helper(
        frozenset({"c1.s.orders"}),
        frozenset({"c2.s.orders"}),
        tableless_matches=tableless_matches,
    )


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
@pytest.mark.parametrize("tableless_matches", [True, False])
@pytest.mark.parametrize("other", [ORDERS, BOTH, TABLELESS, UNRESOLVED])
def test_unresolved_never_matches(helper, tableless_matches: bool, other) -> None:
    assert not helper(UNRESOLVED, other, tableless_matches=tableless_matches)
    assert not helper(other, UNRESOLVED, tableless_matches=tableless_matches)


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
@pytest.mark.parametrize("other", [ORDERS, BOTH, TABLELESS])
def test_a_tableless_side_matches_only_under_the_sole_row_rule(helper, other) -> None:
    assert helper(TABLELESS, other, tableless_matches=True)
    assert helper(other, TABLELESS, tableless_matches=True)
    assert not helper(TABLELESS, other, tableless_matches=False)
    assert not helper(other, TABLELESS, tableless_matches=False)


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
def test_the_sole_row_rule_has_no_default(helper) -> None:
    with pytest.raises(TypeError):
        helper(TABLELESS, ORDERS)


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
def test_the_helpers_read_resolve_all_output(helper) -> None:
    resolver = TableResolver.from_config(SPACE)
    left = resolver.resolve_all(["orders"])
    right = resolver.resolve_all(["main.sales.orders"])
    assert helper(left, right, tableless_matches=False)
    assert not helper(left, resolver.resolve_all(["customer"]), tableless_matches=True)
    assert helper(resolver.resolve_all([]), right, tableless_matches=True)
    assert not helper(resolver.resolve_all([]), right, tableless_matches=False)


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
def test_the_helpers_normalize_case_and_backticks_of_resolved_names(helper) -> None:
    assert helper(
        ("`MAIN`.sales.Orders",), ["main.sales.orders"], tableless_matches=False
    )


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
def test_an_empty_collection_is_tableless_in_the_helpers(helper) -> None:
    assert helper((), ORDERS, tableless_matches=True)
    assert not helper((), ORDERS, tableless_matches=False)
    assert not helper([""], ORDERS, tableless_matches=False)


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
def test_an_unreadable_name_in_the_helpers_is_unresolved(helper) -> None:
    assert not helper(("`a.b`.c",), ("a.b.c",), tableless_matches=True)


@pytest.mark.parametrize("helper", [same_fq_tables, fq_tables_overlap])
def test_the_helpers_refuse_a_bare_string(helper) -> None:
    with pytest.raises(TypeError):
        helper("main.sales.orders", ORDERS, tableless_matches=False)


def test_the_module_exports_the_interface() -> None:
    assert {
        "TABLELESS",
        "UNRESOLVED",
        "TableResolver",
        "same_fq_tables",
        "fq_tables_overlap",
    } <= set(mv_tables.__all__)
