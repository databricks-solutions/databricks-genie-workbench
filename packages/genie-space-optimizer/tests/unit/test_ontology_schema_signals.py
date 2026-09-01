"""Pure structural-signal extraction — offline unit tests (Stage 1, MV-D52).

FK/PK + shared-join extraction from a fixture ``information_schema``, MV membership
from fixture MV YAML, and shared-schema affinity — all pure, no Spark.
"""

from __future__ import annotations

from genie_space_optimizer.ontology import schema_signals as ss


# ── FK reconstruction from information_schema ───────────────────────────────


def _kcu(constraint, cat, sch, tbl, col):
    return {
        "constraint_catalog": "c", "constraint_schema": sch, "constraint_name": constraint,
        "table_catalog": cat, "table_schema": sch, "table_name": tbl, "column_name": col,
    }


def test_fk_edges_referencing_to_referenced():
    # FK fk_rev on revenue.fact_revenue(route_id) → revenue.dim_route(route_id).
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "revenue", "constraint_name": "fk_rev",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "revenue",
        "unique_constraint_name": "pk_route",
    }]
    key_col = [_kcu("fk_rev", "c", "revenue", "fact_revenue", "route_id")]
    constr_col = [_kcu("fk_rev", "c", "revenue", "dim_route", "route_id")]
    edges = ss.fk_edges(referential, key_col, constr_col)
    assert edges == [("c.revenue.fact_revenue", "c.revenue.dim_route")]


def test_fk_edges_falls_back_to_unique_constraint_kcu():
    # No constraint_column_usage row → resolve the referenced table via the linked
    # unique/PK constraint's key_column_usage entry.
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "res", "constraint_name": "fk_pnr",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "res",
        "unique_constraint_name": "pk_pnr",
    }]
    key_col = [
        _kcu("fk_pnr", "c", "res", "bookings", "pnr_id"),
        _kcu("pk_pnr", "c", "res", "pnr", "pnr_id"),
    ]
    edges = ss.fk_edges(referential, key_col, [])
    assert edges == [("c.res.bookings", "c.res.pnr")]


def test_fk_edges_drops_self_reference_and_dedupes():
    referential = [
        {"constraint_catalog": "c", "constraint_schema": "s", "constraint_name": "fk_self",
         "unique_constraint_catalog": "c", "unique_constraint_schema": "s", "unique_constraint_name": "pk_self"},
    ]
    key_col = [_kcu("fk_self", "c", "s", "t", "x")]
    constr_col = [_kcu("fk_self", "c", "s", "t", "x")]  # references itself
    assert ss.fk_edges(referential, key_col, constr_col) == []


# ── Shared-join-column proxy ────────────────────────────────────────────────


def _col(cat, sch, tbl, col):
    return {"table_catalog": cat, "table_schema": sch, "table_name": tbl, "column_name": col}


def test_shared_join_column_star_edges_and_suffix_filter():
    # A 2-schema join column (≤ max_schemas default) still stars its tables.
    rows = [
        _col("c", "rev", "fact_revenue", "route_id"),
        _col("c", "route", "segments", "route_id"),
        _col("c", "route", "legs", "route_id"),
        _col("c", "rev", "fact_revenue", "amount"),   # not a join suffix → ignored
    ]
    edges = ss.shared_join_column_edges(rows)
    # 3 tables in 2 schemas share route_id → a 2-edge star from the sorted-first table.
    assert len(edges) == 2
    hub = sorted({"c.rev.fact_revenue", "c.route.segments", "c.route.legs"})[0]
    assert all(a == hub and w == ss.SHARED_JOIN_WEIGHT and src == "shared_join_column" for (a, b, w, src) in edges)
    # A column on a single table produces no edge (min_tables).
    assert ss.shared_join_column_edges([_col("c", "rev", "t", "x_id")]) == []


def test_shared_join_column_skips_over_generic_columns():
    # A column touching more than MAX_TABLES_PER_SHARED_COLUMN tables is a generic unit
    # (currency_code, if re-enabled as a suffix) → skipped so it does not fuse the estate.
    rows = [_col("c", "s", f"t{i}", "widget_id") for i in range(ss.MAX_TABLES_PER_SHARED_COLUMN + 1)]
    assert ss.shared_join_column_edges(rows) == []


def test_code_suffix_dropped_by_default_but_reenabled_via_config():
    # MV-D61: ``_code`` is a reference/enum value, not a join key — no proxy edge under
    # the default suffixes. An enterprise can pass it back via ``join_suffixes``.
    rows = [_col("c", "s", "orders", "state_code"), _col("c", "s", "customers", "state_code")]
    assert ss.shared_join_column_edges(rows) == []
    reenabled = ss.shared_join_column_edges(rows, join_suffixes=("_id", "_key", "_code"))
    assert len(reenabled) == 1 and reenabled[0][3] == "shared_join_column"


def test_shared_join_column_span_cap_skips_three_schemas_keeps_two():
    # A column spanning 3 schemas (≤15 tables) is a generic bridge → skipped at the
    # default max_schemas=2; the same column across 2 schemas still stars.
    three = [
        _col("c", "rev", "fact", "trip_id"),
        _col("c", "route", "seg", "trip_id"),
        _col("c", "infra", "audit", "trip_id"),
    ]
    assert ss.shared_join_column_edges(three) == []
    two = [_col("c", "rev", "fact", "trip_id"), _col("c", "route", "seg", "trip_id")]
    assert len(ss.shared_join_column_edges(two)) == 1
    # Lifting the cap re-admits the 3-schema star.
    assert len(ss.shared_join_column_edges(three, max_schemas=3)) == 2


def test_generic_name_denylist_skips_surrogate_keys():
    # A generic surrogate/audit key (``user_id``) is skipped outright even within the
    # schema-span cap; a real business key on the same tables still stars.
    denied = [_col("c", "s", "a", "user_id"), _col("c", "s", "b", "user_id")]
    assert ss.shared_join_column_edges(denied) == []
    ok = [_col("c", "s", "a", "booking_id"), _col("c", "s", "b", "booking_id")]
    assert len(ss.shared_join_column_edges(ok)) == 1


def test_join_key_edges_combines_fk_and_proxy():
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "s", "constraint_name": "fk1",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "s", "unique_constraint_name": "pk1",
    }]
    key_col = [_kcu("fk1", "c", "s", "child", "k_id")]
    constr_col = [_kcu("fk1", "c", "s", "parent", "k_id")]
    cols = [_col("c", "s", "child", "k_id"), _col("c", "s", "other", "k_id")]
    combined = ss.join_key_edges(referential, key_col, constr_col, cols)
    sources = {(e[3] if len(e) > 3 else "foreign_key") for e in combined}
    assert sources == {"foreign_key", "shared_join_column"}


def test_join_key_edges_span_cap_is_proxy_only_declared_fk_kept():
    # A declared FK across THREE schemas is decisive — kept regardless of the span cap;
    # a proxy column across the same three schemas is dropped (cap is proxy-only, MV-D61).
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "rev", "constraint_name": "fk_x",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "loyalty",
        "unique_constraint_name": "pk_x",
    }]
    key_col = [_kcu("fk_x", "c", "rev", "fact", "member_id")]
    # The FK's constraint_column_usage row keys on the FK's own (referencing) schema but
    # names the REFERENCED table — so it crosses schema (rev → loyalty).
    constr_col = [{
        "constraint_catalog": "c", "constraint_schema": "rev", "constraint_name": "fk_x",
        "table_catalog": "c", "table_schema": "loyalty", "table_name": "members",
        "column_name": "member_id",
    }]
    cols = [
        _col("c", "rev", "fact", "member_id"),
        _col("c", "loyalty", "members", "member_id"),
        _col("c", "infra", "audit", "member_id"),  # 3rd schema → proxy dropped
    ]
    combined = ss.join_key_edges(referential, key_col, constr_col, cols)
    fk_pairs = [(e[0], e[1]) for e in combined if (len(e) < 4 or e[3] == "foreign_key")]
    proxy = [e for e in combined if len(e) > 3 and e[3] == "shared_join_column"]
    assert ("c.rev.fact", "c.loyalty.members") in fk_pairs  # declared FK kept
    assert proxy == []                                       # proxy span-capped away


# ── Non-business-schema denylist (MV-D61) ───────────────────────────────────


def test_filter_denylisted_schemas_drops_from_every_edge_kind():
    # A denylisted schema's tables are dropped from EVERY row input before edges build,
    # so neither the FK nor the proxy can reference them. ``information_schema`` (the
    # shipped default) and a bare/glob entry all match.
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "migration", "constraint_name": "fk_m",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "migration",
        "unique_constraint_name": "pk_m",
    }]
    key_col = [_kcu("fk_m", "c", "migration", "child", "k_id")]
    constr_col = [_kcu("fk_m", "c", "migration", "parent", "k_id")]
    cols = [_col("c", "migration", "child", "k_id"), _col("c", "e2e_run", "t", "k_id")]
    dl = ["migration", "e2e_*"]
    combined = ss.join_key_edges(
        ss.filter_denylisted_schemas(referential, denylist=dl),
        ss.filter_denylisted_schemas(key_col, denylist=dl),
        ss.filter_denylisted_schemas(constr_col, denylist=dl),
        ss.filter_denylisted_schemas(cols, denylist=dl),
    )
    assert combined == []
    # A row with no resolvable catalog+schema is kept (never judged).
    assert ss.filter_denylisted_schemas([{"other": 1}], denylist=dl) == [{"other": 1}]
    # Empty/absent denylist is a passthrough.
    assert ss.filter_denylisted_schemas(cols, denylist=None) == cols


def test_filter_denylisted_schemas_matches_exact_and_glob():
    tbls = [
        {"table_catalog": "c", "table_schema": "information_schema", "table_name": "columns"},
        {"table_catalog": "c", "table_schema": "skyloyalty_dev", "table_name": "t"},
        {"table_catalog": "c", "table_schema": "airline_demo", "table_name": "flights"},
    ]
    kept = ss.filter_denylisted_schemas(tbls, denylist=["information_schema", "*_dev"])
    assert ss.schema_affinity_map(kept) == {"c.airline_demo": ["c.airline_demo.flights"]}


# ── MV membership ───────────────────────────────────────────────────────────


def test_mv_membership_map_from_yaml_source():
    yamls = {
        "c.metrics.revenue_mv": {"source": "c.revenue.fact_revenue", "measures": []},
        "c.metrics.backtick_mv": {"source": "`c`.`revenue`.`bookings`"},
        "c.metrics.subquery_mv": {"source": "SELECT * FROM c.revenue.x"},  # subquery → skip
        "c.metrics.empty_mv": {"measures": []},                             # no source → skip
    }
    got = ss.mv_membership_map(yamls)
    assert got == {
        "c.metrics.revenue_mv": ["c.revenue.fact_revenue"],
        "c.metrics.backtick_mv": ["c.revenue.bookings"],
    }


# ── Schema affinity ─────────────────────────────────────────────────────────


def test_schema_affinity_map_groups_by_catalog_schema():
    rows = [
        {"table_catalog": "c", "table_schema": "revenue", "table_name": "fact_revenue"},
        {"table_catalog": "c", "table_schema": "revenue", "table_name": "dim_route"},
        {"table_catalog": "c", "table_schema": "loyalty", "table_name": "members"},
        {"table_catalog": "c", "table_schema": "loyalty", "table_name": None},  # skipped
    ]
    got = ss.schema_affinity_map(rows)
    assert got == {
        "c.loyalty": ["c.loyalty.members"],
        "c.revenue": ["c.revenue.dim_route", "c.revenue.fact_revenue"],
    }
