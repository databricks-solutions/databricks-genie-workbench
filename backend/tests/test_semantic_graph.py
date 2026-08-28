"""Tests for the space-scoped semantic model graph route (Prompt 12 + 12b, MV-D23).

Seams tested without Databricks:

- **Assembly** (``_build_semantic_graph``): tables split into source/fact (col 0)
  vs joined dimension (col 1); join edges carry the decoded ON predicate,
  relationship, and an SCD2 flag from the ``is_current`` guard; measure concepts
  land on the governance ladder — governed (DESCRIBE-enumerated MV measures,
  Prompt 12b Debt 2), curated (``sql_snippets.measures`` + measures harvested
  from ``example_question_sqls``, Debt 1), ungoverned (recurs only in proposal
  evidence) — with CANONICALIZED-EXPR identity so two spellings of one measure
  are one chip and a higher rung absorbs its twin (Debt 3).
- **Coverage lens** (``_apply_coverage``): curated-SQL touch counts per node,
  cold spots at 0, the MV-D15 status vocabulary (EMPTY / UNAVAILABLE / COMPUTED).
- **The route**: the base graph is read the OBO-tolerant way ``/space/fetch``
  reads (``get_serialized_space``); the governed chips ride a best-effort
  DESCRIBE read; proposals ride the SP-side Delta read; a never-optimized space
  still renders; a config-read failure surfaces as 502.
- **Compatibility**: a Prompt 12 client that never learned the lens keeps working
  — every 12b field is additive.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models import MvProposal
from backend.routers import auto_optimize
from genie_space_optimizer.common import warehouse


_SPACE = {
    "data_sources": {
        "tables": [
            {"identifier": "finance.sales.orders"},
            {"identifier": "finance.sales.order_items"},
            {"identifier": "finance.ref.customer"},
        ],
        "metric_views": [
            {"identifier": "finance.sales.orders_metrics"},
        ],
    },
    "instructions": {
        "join_specs": [
            {
                "id": "a" * 32,
                "left": {"identifier": "finance.sales.order_items"},
                "right": {"identifier": "finance.sales.orders"},
                "sql": [
                    "`order_items`.`order_id` = `orders`.`order_id`",
                    "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
                ],
            },
            {
                "id": "b" * 32,
                "left": {"identifier": "finance.sales.orders"},
                "right": {"identifier": "finance.ref.customer"},
                "sql": [
                    "`orders`.`customer_id` = `customer`.`id` AND `customer`.`is_current` = true",
                    "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
                ],
            },
        ],
        "sql_snippets": {
            "measures": [
                {"id": "c" * 32, "display_name": "gross_margin", "sql": ["SUM(o.rev - o.cost)"]}
            ]
        },
    },
}


# A DESCRIBE-derived governed measure (Prompt 12b Debt 2). Field-shaped mapping;
# _field_attr reads either a mapping or a MetricViewField object.
def _governed(field_name: str, canonical_expr: str = "", mv_fqn: str = "finance.sales.orders_metrics"):
    return {
        "mv_fqn": mv_fqn,
        "field_name": field_name,
        "kind": "measure",
        "canonical_expr": canonical_expr,
    }


def _proposal(proposed_object: str, recurrence: int = 14, **over) -> MvProposal:
    base = dict(
        suggestion_id="sug1",
        dedup_fingerprint="fp1",
        target_space_id="space-1",
        candidate_type="PROPOSE",
        proposed_object=proposed_object,
        evidence={"recurrence_count": recurrence, "source_tables": ["finance.sales.orders"]},
    )
    base.update(over)
    return MvProposal(**base)


def _node(nodes: list[dict], node_id: str) -> dict | None:
    return next((n for n in nodes if n["id"] == node_id), None)


def _build(space, proposals, **kw):
    """Unpack the 4-tuple (nodes, edges, coverage_status, coverage_reason)."""
    return auto_optimize._build_semantic_graph(space, proposals, **kw)


# ── Pure assembly ───────────────────────────────────────────────────────────


def test_build_graph_splits_tables_joins_and_ladders_measures():
    nodes, edges, _status, _reason = _build(
        _SPACE, [], governed_fields=[_governed("order_count", "count(1)")]
    )
    node_by_id = {n.id: n for n in nodes}

    # order_items is only ever a left/fact table → col 0; customer is a join's
    # right (dimension) side → col 1.
    assert node_by_id["finance.sales.order_items"].col == 0
    assert node_by_id["finance.ref.customer"].col == 1

    # Metric view node in col 2; its DESCRIBE-enumerated measure is a governed
    # chip tied to it by membership.
    assert node_by_id["finance.sales.orders_metrics"].kind == "metric_view"
    assert node_by_id["measure:order_count"].governance == "governed"

    # Curated concept from sql_snippets.measures.
    assert node_by_id["measure:gross_margin"].governance == "curated"

    # The SCD2 join carries the decoded relationship, cleaned ON, and scd2 flag.
    scd2_edge = next(
        e for e in edges
        if e.kind == "join" and e.to == "finance.ref.customer"
    )
    assert scd2_edge.relationship == "many-to-one"
    assert scd2_edge.scd2 is True
    assert "`" not in (scd2_edge.on or "")
    assert "customer.is_current = true" in (scd2_edge.on or "")

    # A membership edge ties the governed measure to its metric view.
    assert any(
        e.kind == "membership" and e.from_ == "measure:order_count"
        and e.to == "finance.sales.orders_metrics"
        for e in edges
    )


def test_build_graph_governed_chips_come_from_describe_not_config_markers():
    """Debt 2: with no DESCRIBE read (empty governed_fields), the MV still renders
    as a governed node but WITHOUT fabricated measure chips — the honest fallback
    the deleted _is_measure_column probe used to produce."""
    nodes, _edges, _s, _r = _build(_SPACE, [])
    node_by_id = {n.id: n for n in nodes}
    assert node_by_id["finance.sales.orders_metrics"].kind == "metric_view"
    assert not any(n.governance == "governed" for n in nodes)
    # The deleted speculative probe is gone.
    assert not hasattr(auto_optimize, "_is_measure_column")


def test_build_graph_curated_concept_harvested_from_example_sql():
    """Debt 1: a measure in example_question_sqls joins the ladder as curated."""
    space = {
        "data_sources": {"tables": [{"identifier": "finance.sales.orders"}]},
        "instructions": {
            "example_question_sqls": [
                {"id": "q1", "sql": ["SELECT SUM(o.qty) FROM finance.sales.orders o"]}
            ]
        },
    }
    nodes, _edges, _s, _r = _build(space, [])
    curated = [n for n in nodes if n.kind == "measure" and n.governance == "curated"]
    assert len(curated) == 1
    assert "sum(qty)" in curated[0].label.lower()


def test_build_graph_expr_identity_merges_two_spellings():
    """Debt 3: a snippet measure and an example-SQL measure that canonicalize to
    the same expression are ONE chip (qualifiers/aliases differ, identity does
    not)."""
    space = {
        "data_sources": {"tables": [{"identifier": "finance.sales.orders"}]},
        "instructions": {
            "sql_snippets": {
                "measures": [{"id": "m1", "display_name": "margin", "sql": ["SUM(o.rev - o.cost)"]}]
            },
            "example_question_sqls": [
                {"id": "q1", "sql": ["SELECT SUM(ord.rev - ord.cost) FROM finance.sales.orders ord"]}
            ],
        },
    }
    nodes, _edges, _s, _r = _build(space, [])
    curated = [n for n in nodes if n.kind == "measure" and n.governance == "curated"]
    assert len(curated) == 1
    # The human label from the snippet wins over the raw-expr label.
    assert curated[0].label == "margin"


def test_build_graph_governed_absorbs_its_curated_twin():
    """Debt 3: a governed measure and a curated snippet that canonicalize the same
    are one chip at the governed rung (highest rung wins by expr identity)."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],
            "metric_views": [{"identifier": "finance.sales.m"}],
        },
        "instructions": {
            "sql_snippets": {
                "measures": [{"id": "m1", "display_name": "revenue", "sql": ["SUM(o.rev)"]}]
            }
        },
    }
    nodes, _e, _s, _r = _build(
        space, [], governed_fields=[_governed("revenue", "sum(rev)", mv_fqn="finance.sales.m")]
    )
    measures = [n for n in nodes if n.kind == "measure"]
    assert len(measures) == 1
    assert measures[0].governance == "governed"


def test_build_graph_adds_ungoverned_from_proposal_evidence():
    nodes, _edges, _s, _r = _build(
        _SPACE, [_proposal("finance.sales.order_revenue", recurrence=14)]
    )
    node_by_id = {n.id: n for n in nodes}
    ungoverned = node_by_id["measure:order_revenue"]
    assert ungoverned.governance == "ungoverned"
    assert "14" in (ungoverned.origin or "")


def test_build_graph_ungoverned_carries_benchmark_question_ids():
    """The evidence lens: benchmark question ids ride the ungoverned node."""
    nodes, _e, _s, _r = _build(
        _SPACE,
        [_proposal("finance.sales.order_revenue", evidence={"benchmark_question_ids": ["bq_1", "bq_2"]})],
    )
    node = next(n for n in nodes if n.id == "measure:order_revenue")
    assert node.benchmark_question_ids == ["bq_1", "bq_2"]


def test_build_graph_name_match_prefers_higher_rung():
    """A proposal whose name matches a curated concept is not re-added ungoverned."""
    nodes, _edges, _s, _r = _build(_SPACE, [_proposal("finance.sales.gross_margin")])
    margins = [n for n in nodes if n.label == "gross_margin"]
    assert len(margins) == 1
    assert margins[0].governance == "curated"


def test_build_graph_empty_space_has_no_measures_or_edges():
    nodes, edges, status, _reason = _build(
        {"data_sources": {"tables": [{"identifier": "finance.sales.orders"}]}}, []
    )
    assert [n.kind for n in nodes] == ["table"]
    assert edges == []
    # No curated SQL → the coverage lens is honestly EMPTY, not a zero it invented.
    assert status == "EMPTY"


# ── Prompt 12e / MV-D33: the metric view drawn as a semantic model ───────────


def test_metric_view_internals_parses_source_and_joins():
    """A readable MV YAML yields available=True with its source fact and joined
    dims (the arrow proof). Backticks in the ON clause are cleaned."""
    yamls = {
        "finance.sales.orders_metrics": {
            "source": "finance.sales.orders",
            "joins": [
                {"name": "customer", "source": "finance.ref.customer", "on": "`orders`.`cid` = `customer`.`id`"},
                {"name": "item", "source": "finance.sales.order_items", "using": "order_id"},
            ],
            "measures": [{"name": "order_count", "expr": "count(1)"}],
        }
    }
    internals = auto_optimize._metric_view_internals(yamls, ["finance.sales.orders_metrics"])
    info = internals["finance.sales.orders_metrics"]
    assert info["available"] is True
    assert info["source"] == "finance.sales.orders"
    assert {j["table"] for j in info["joins"]} == {"finance.ref.customer", "finance.sales.order_items"}
    on_clause = next(j["on"] for j in info["joins"] if j["table"] == "finance.ref.customer")
    assert "`" not in on_clause and "orders.cid = customer.id" in on_clause


def test_metric_view_internals_unreadable_is_unproven():
    """An MV absent from the read, or parsed without a real source (a skeleton),
    is available=False — unreadable is unproven (MV-D33 constraint 2)."""
    yamls = {"finance.sales.skeleton": {"measures": [{"name": "x", "expr": "count(1)"}]}}  # no source
    internals = auto_optimize._metric_view_internals(
        yamls, ["finance.sales.skeleton", "finance.sales.absent"]
    )
    assert internals["finance.sales.skeleton"]["available"] is False
    assert internals["finance.sales.skeleton"]["joins"] == []
    assert internals["finance.sales.absent"]["available"] is False


def test_build_graph_emits_uses_edges_and_marks_definition_available():
    """A readable MV emits a ``uses`` edge to each member table (the at-rest arrow
    + the select-time boundary member set) and a proven MV-YAML join edge; the MV
    node is definition_available=True. A member table the space did not declare is
    ADDED once (deduplicated), never copied."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],  # only the fact is declared
            "metric_views": [{"identifier": "finance.sales.orders_metrics"}],
        }
    }
    internals = {
        "finance.sales.orders_metrics": {
            "available": True,
            "source": "finance.sales.orders",
            "joins": [{"table": "finance.ref.customer", "on": "orders.cid = customer.id", "relationship": None}],
        }
    }
    nodes, edges, _s, _r = _build(space, [], mv_internals=internals)
    node_by_id = {n.id: n for n in nodes}

    assert node_by_id["finance.sales.orders_metrics"].definition_available is True
    # The un-declared join table is added once as a dimension (col 1); the source
    # is a fact (col 0).
    assert node_by_id["finance.ref.customer"].kind == "table"
    assert node_by_id["finance.ref.customer"].col == 1
    assert node_by_id["finance.sales.orders"].col == 0
    # uses edges MV → each member table.
    uses = {(e.from_, e.to) for e in edges if e.kind == "uses"}
    assert uses == {
        ("finance.sales.orders_metrics", "finance.sales.orders"),
        ("finance.sales.orders_metrics", "finance.ref.customer"),
    }
    # A proven MV-YAML join edge (source → dim), carrying the ON clause.
    join = next(e for e in edges if e.kind == "join")
    assert (join.from_, join.to) == ("finance.sales.orders", "finance.ref.customer")
    assert "customer.id" in (join.on or "")
    # No duplicate customer node.
    assert sum(1 for n in nodes if n.id == "finance.ref.customer") == 1


def test_build_graph_marks_proven_fact_dim_role_and_leaves_join_only_neutral():
    """Round-5: ``role`` is set ONLY where a metric view DEFINITION proves it — the
    MV source is a fact, an MV-joined table is a dim. A table known only from
    ``join_specs`` stays ``role=None`` so the UI labels it a neutral "table" rather
    than guessing fact/dim from column position (the mislabel fix)."""
    space = {
        "data_sources": {
            "tables": [
                {"identifier": "finance.sales.orders"},
                {"identifier": "finance.ref.customer"},
                {"identifier": "finance.ref.region"},
            ],
            "metric_views": [{"identifier": "finance.sales.orders_metrics"}],
        },
        "instructions": {
            "join_specs": [
                {
                    "id": "a" * 32,
                    "left": {"identifier": "finance.ref.customer"},
                    "right": {"identifier": "finance.ref.region"},
                    "sql": ["`customer`.`region_id` = `region`.`id`"],
                }
            ]
        },
    }
    internals = {
        "finance.sales.orders_metrics": {
            "available": True,
            "source": "finance.sales.orders",
            "joins": [{"table": "finance.ref.customer", "on": "orders.cid = customer.id"}],
        }
    }
    nodes, _e, _s, _r = _build(space, [], mv_internals=internals)
    by_id = {n.id: n for n in nodes}
    assert by_id["finance.sales.orders"].role == "fact"  # the MV's declared source
    assert by_id["finance.ref.customer"].role == "dim"  # an MV-joined table
    # region is proven by NO metric view — only a join_spec — so it stays neutral.
    assert by_id["finance.ref.region"].role is None


def test_build_graph_measure_nodes_carry_expression_and_description():
    """Round-5: a governed measure rides its canonical expression (and a
    description when the field carries one); a curated snippet measure rides its
    snippet SQL; an ungoverned proposal exposes a name only, so ``expr`` is None."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],
            "metric_views": [{"identifier": "finance.sales.orders_metrics"}],
        },
        "instructions": {
            "sql_snippets": {
                "measures": [
                    {"id": "c" * 32, "display_name": "gross_margin", "sql": ["SUM(o.rev - o.cost)"]}
                ]
            }
        },
    }
    governed = [
        {
            "mv_fqn": "finance.sales.orders_metrics",
            "field_name": "order_count",
            "kind": "measure",
            "canonical_expr": "count(1)",
            "description": "rows per order",
        }
    ]
    nodes, _e, _s, _r = _build(
        space, [_proposal("finance.sales.new_measure")], governed_fields=governed
    )
    by_id = {n.id: n for n in nodes}
    assert by_id["measure:order_count"].expr == "count(1)"
    assert by_id["measure:order_count"].description == "rows per order"
    assert by_id["measure:gross_margin"].expr == "SUM(o.rev - o.cost)"
    prop = by_id.get("measure:new_measure")
    assert prop is not None and prop.governance == "ungoverned"
    assert prop.expr is None


# ── Round-7: measure→table lineage (`derives`) for loose measures ────────────


def test_expr_table_refs_extracts_fully_qualified_tables():
    """A 4-part ``catalog.schema.table.column`` yields its table (first 3), marked
    add-eligible and de-duplicated; bare functions and columns yield nothing."""
    refs = auto_optimize._expr_table_refs(
        "ROUND(SUM(cat.sch.fact.is_biz) / NULLIF(COUNT(cat.sch.fact.id), 0), 2)"
    )
    assert ("cat.sch.fact", True) in refs
    assert len([r for r in refs if r[0] == "cat.sch.fact"]) == 1  # deduped

    # 3-part run is ambiguous → match-only (never added blindly).
    assert ("a.b.c", False) in auto_optimize._expr_table_refs("SUM(a.b.c)")
    # A 2-part column ref and a bare function reference nothing.
    assert auto_optimize._expr_table_refs("SUM(t.col) + COUNT(x)") == []


def test_build_graph_loose_measure_derives_edge_adds_referenced_table():
    """A curated (loose) measure whose expression reads a fully-qualified table
    gets a ``derives`` edge to that table; a table the expr proves but the space
    never modeled is ADDED (it lands in the unmodeled region), so selecting the
    measure has a source to light."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "cat.sch.orders"}],
            "metric_views": [],
        },
        "instructions": {
            "sql_snippets": {
                "measures": [
                    {
                        "id": "d" * 32,
                        "display_name": "biz_rate",
                        "sql": [
                            "ROUND(SUM(cat.sch.fact_detail.is_biz) / "
                            "NULLIF(COUNT(cat.sch.fact_detail.id), 0), 2)"
                        ],
                    }
                ]
            }
        },
    }
    nodes, edges, _s, _r = _build(space, [])
    by_id = {n.id: n for n in nodes}
    # The referenced table was added as a neutral (unproven) source table.
    assert "cat.sch.fact_detail" in by_id
    assert by_id["cat.sch.fact_detail"].kind == "table"
    assert by_id["cat.sch.fact_detail"].role is None
    # And a derives edge links the loose measure to it (one, deduped).
    derives = [e for e in edges if e.kind == "derives" and e.from_ == "measure:biz_rate"]
    assert len(derives) == 1
    assert derives[0].to == "cat.sch.fact_detail"


def test_build_graph_governed_measure_gets_no_derives_edge():
    """Governed measures already wrap their MV on select, so they are skipped by
    the loose-measure lineage pass — no ``derives`` edge even if the (canonical)
    expression carries a fully-qualified reference."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "cat.sch.orders"}],
            "metric_views": [{"identifier": "cat.sch.orders_metrics"}],
        },
        "instructions": {},
    }
    governed = [
        {
            "mv_fqn": "cat.sch.orders_metrics",
            "field_name": "order_count",
            "kind": "measure",
            "canonical_expr": "SUM(cat.sch.fact_detail.amount)",
        }
    ]
    _nodes, edges, _s, _r = _build(space, [], governed_fields=governed)
    assert not any(e.kind == "derives" for e in edges)


def test_build_graph_unavailable_mv_draws_no_arrows():
    """definition_available=False (unreadable YAML) contributes NO uses edges and
    NO added tables — arrows require proof (MV-D33 constraint 2)."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],
            "metric_views": [{"identifier": "finance.sales.orders_metrics"}],
        }
    }
    internals = {"finance.sales.orders_metrics": {"available": False, "source": None, "joins": []}}
    nodes, edges, _s, _r = _build(space, [], mv_internals=internals)
    node_by_id = {n.id: n for n in nodes}
    assert node_by_id["finance.sales.orders_metrics"].definition_available is False
    assert not any(e.kind == "uses" for e in edges)
    # No phantom join table was invented.
    assert not any(n.id == "finance.ref.customer" for n in nodes)


def test_build_graph_no_internals_leaves_definition_available_none():
    """With no internals passed (no read attempted), an MV node's
    definition_available stays None — no "unavailable" badge, no arrows."""
    nodes, edges, _s, _r = _build(_SPACE, [])
    mv = next(n for n in nodes if n.kind == "metric_view")
    assert mv.definition_available is None
    assert not any(e.kind == "uses" for e in edges)


# ── Prompt 12f: the rest of the definition the curator inset reports ─────────


def test_metric_view_internals_carries_filter_materialization_and_dimensions():
    """The same parsed YAML already proves the filter, the materialization posture
    and the declared dimensions — the reader now carries all three so the inset
    needs no second read. A dimension qualified by a join ALIAS binds to that
    join's table; an unqualified one binds to the source."""
    yamls = {
        "finance.sales.orders_metrics": {
            "source": "finance.sales.orders",
            "filter": "order_status  !=  'CANCELLED'",
            "joins": [
                {"name": "customer", "source": "finance.ref.customer", "on": "orders.cid = customer.id"},
            ],
            "fields": [
                {"name": "region", "expr": "customer.region"},
                {"name": "order_day", "expr": "date_trunc('DAY', order_ts)"},
            ],
            "materialization": {
                "mode": "relaxed",
                "schedule": "EVERY 1 DAY",
                "materialized_views": [
                    {"name": "daily", "type": "aggregated", "dimensions": ["order_day"]},
                    {"name": "raw", "type": "unaggregated"},
                ],
            },
            "measures": [{"name": "order_count", "expr": "count(1)"}],
        }
    }
    info = auto_optimize._metric_view_internals(
        yamls, ["finance.sales.orders_metrics"]
    )["finance.sales.orders_metrics"]

    # Verbatim predicate, whitespace collapsed — never paraphrased.
    assert info["filter"] == "order_status != 'CANCELLED'"
    assert info["materialization"] == "2 materializations · EVERY 1 DAY"
    by_name = {d["name"]: d for d in info["dimensions"]}
    assert by_name["region"]["binding"] == "finance.ref.customer"
    assert by_name["order_day"]["binding"] == "finance.sales.orders"
    # The join's alias is retained (it is what resolved the binding).
    assert info["joins"][0]["alias"] == "customer"


def test_metric_view_internals_omits_what_the_yaml_does_not_declare():
    """A view with no filter and no materialization reports None for both — an
    absent materialization must not read as one with an unknown schedule. A
    ``using`` list renders as a USING clause rather than a bare column name."""
    yamls = {
        "finance.sales.plain": {
            "source": "finance.sales.orders",
            "joins": [{"name": "item", "source": "finance.sales.order_items", "using": ["order_id"]}],
            "measures": [{"name": "n", "expr": "count(1)"}],
        }
    }
    info = auto_optimize._metric_view_internals(yamls, ["finance.sales.plain"])["finance.sales.plain"]
    assert info["filter"] is None
    assert info["materialization"] is None
    assert info["dimensions"] == []
    assert info["joins"][0]["on"] == "USING (order_id)"


def test_materialization_with_no_entries_is_none_not_a_false_posture():
    """An object present but empty declares no materialization."""
    assert auto_optimize._materialization_summary({"mode": "relaxed", "materialized_views": []}) is None
    assert auto_optimize._materialization_summary(None) is None
    # A single entry with no schedule reports the spec's own default.
    assert auto_optimize._materialization_summary(
        {"materialized_views": [{"name": "a", "type": "unaggregated"}]}
    ) == "1 materialization · manual refresh"


def test_dimensions_accept_the_legacy_dimensions_keyword():
    """``fields`` is the current spelling; ``dimensions`` is the accepted synonym."""
    yamls = {
        "a.b.c": {
            "source": "a.b.t",
            "dimensions": [{"name": "region", "expr": "region"}],
        }
    }
    info = auto_optimize._metric_view_internals(yamls, ["a.b.c"])["a.b.c"]
    assert [d["name"] for d in info["dimensions"]] == ["region"]


def test_unreadable_yaml_reports_no_filter_or_dimensions():
    """available=False proves nothing, so it carries no detail either."""
    info = auto_optimize._metric_view_internals({}, ["a.b.absent"])["a.b.absent"]
    assert info["filter"] is None
    assert info["materialization"] is None
    assert info["dimensions"] == []


def test_build_graph_puts_the_parsed_definition_on_the_mv_node():
    """The node carries filter / materialization / dimensions for a READABLE YAML
    so the inset renders from the graph payload alone."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],
            "metric_views": [{"identifier": "finance.sales.orders_metrics"}],
        }
    }
    internals = {
        "finance.sales.orders_metrics": {
            "available": True,
            "source": "finance.sales.orders",
            "joins": [],
            "filter": "order_status != 'CANCELLED'",
            "materialization": "1 materialization · EVERY 1 DAY",
            "dimensions": [
                {"name": "region", "expr": "customer.region", "binding": "finance.ref.customer"}
            ],
        }
    }
    nodes, _edges, _s, _r = _build(space, [], mv_internals=internals)
    mv = next(n for n in nodes if n.kind == "metric_view")
    # The RESOLVED canvas node id, so the inset's join tree roots by identity.
    assert mv.mv_source == "finance.sales.orders"
    assert mv.mv_filter == "order_status != 'CANCELLED'"
    assert mv.materialization == "1 materialization · EVERY 1 DAY"
    assert mv.dimensions is not None
    assert mv.dimensions[0].name == "region"
    assert mv.dimensions[0].binding == "finance.ref.customer"


def test_build_graph_unavailable_mv_node_carries_no_definition_detail():
    """Unreadable is unproven for the detail too: no filter, no materialization,
    no dimensions on a definition_available=False node."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],
            "metric_views": [{"identifier": "finance.sales.orders_metrics"}],
        }
    }
    internals = {
        "finance.sales.orders_metrics": {
            "available": False, "source": None, "joins": [],
            # Even if a caller passed detail, an unavailable definition drops it.
            "filter": "x = 1", "materialization": "1 materialization · manual refresh",
            "dimensions": [{"name": "region", "expr": "r", "binding": "t"}],
        }
    }
    nodes, _edges, _s, _r = _build(space, [], mv_internals=internals)
    mv = next(n for n in nodes if n.kind == "metric_view")
    assert mv.mv_source is None
    assert mv.mv_filter is None
    assert mv.materialization is None
    assert mv.dimensions is None


def test_loose_measure_reusing_a_governed_name_is_flagged_as_an_overlap():
    """Same name, different expression — the dangerous case identity dedup cannot
    merge. The loose concept carries the governing MV so the Space-config panel
    can warn; the governed one itself never claims to overlap."""
    space = {
        "data_sources": {
            "tables": [{"identifier": "finance.sales.orders"}],
            "metric_views": [{"identifier": "finance.sales.orders_metrics"}],
        },
        "instructions": {
            "sql_snippets": {
                "measures": [
                    # Same NAME as the governed measure, a DIFFERENT expression.
                    {"id": "revenue", "display_name": "revenue", "sql": ["sum(net_amount)"]},
                    {"id": "units", "display_name": "units", "sql": ["sum(qty)"]},
                ]
            }
        },
    }
    governed = [{
        "mv_fqn": "finance.sales.orders_metrics",
        "field_name": "revenue",
        "canonical_expr": "sum(gross_amount)",
    }]
    nodes, _edges, _s, _r = _build(space, [], governed_fields=governed)
    by_label = {(n.label, n.governance): n for n in nodes if n.kind == "measure"}
    assert by_label[("revenue", "curated")].overlaps == "finance.sales.orders_metrics"
    assert by_label[("revenue", "governed")].overlaps is None
    # A loose measure with no governed twin makes no claim.
    assert by_label[("units", "curated")].overlaps is None


# ── Coverage lens ────────────────────────────────────────────────────────────


def test_coverage_lens_counts_touches_and_marks_cold_spots():
    space = {
        "data_sources": {
            "tables": [
                {"identifier": "finance.sales.orders"},
                {"identifier": "finance.ref.customer"},
                {"identifier": "finance.ref.unused"},
            ],
        },
        "instructions": {
            "example_question_sqls": [
                {"id": "q1", "sql": ["SELECT SUM(o.qty) FROM finance.sales.orders o"]},
                {
                    "id": "q2",
                    "sql": [
                        "SELECT COUNT(1) FROM finance.sales.orders o "
                        "JOIN finance.ref.customer c ON o.cid = c.id"
                    ],
                },
            ]
        },
    }
    nodes, _edges, status, reason = _build(space, [])
    assert status == "COMPUTED"
    assert reason is None
    node_by_id = {n.id: n for n in nodes}
    # orders is touched by both statements; customer by one; unused is a cold spot.
    assert node_by_id["finance.sales.orders"].coverage == 2
    assert node_by_id["finance.ref.customer"].coverage == 1
    assert node_by_id["finance.ref.unused"].coverage == 0


def test_coverage_lens_unavailable_when_all_statements_fail_to_parse():
    space = {
        "data_sources": {"tables": [{"identifier": "finance.sales.orders"}]},
        "instructions": {
            "example_question_sqls": [{"id": "q1", "sql": ["not valid ;; sql @@@"]}]
        },
    }
    _nodes, _edges, status, reason = _build(space, [])
    assert status == "UNAVAILABLE"
    assert reason and "parse" in reason


# ── Route ───────────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    # The metric-view YAML read is best-effort and Databricks-bound; default it
    # to the honest empty dict in the offline route tests (no governed chips, no
    # MV internals), overridden where a chip or internal is asserted. This is the
    # ONE batched read the route derives both governed measures and MV internals
    # from (Prompt 12e cache posture).
    monkeypatch.setattr(auto_optimize, "_read_metric_view_yamls", lambda space_data: {})
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_semantic_graph_reads_base_from_the_obo_tolerant_config_path(client, monkeypatch):
    """The base graph comes from get_serialized_space (the /space/fetch OBO path),
    not from a run artifact or an SP-only read; governed chips ride the DESCRIBE
    read."""
    seen: dict = {}

    def fake_serialized(space_id):
        seen["space_id"] = space_id
        return dict(_SPACE)

    monkeypatch.setattr(auto_optimize, "get_serialized_space", fake_serialized)
    # The batched YAML read returns the MV's parsed definition; the route derives
    # both the governed chip (a measure) AND the internals (source/joins) from it.
    monkeypatch.setattr(
        auto_optimize, "_read_metric_view_yamls",
        lambda space_data: {
            "finance.sales.orders_metrics": {
                "source": "finance.sales.orders",
                "measures": [{"name": "order_count", "expr": "count(1)"}],
            }
        },
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])

    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    assert seen["space_id"] == "space-1"
    assert data["space_id"] == "space-1"
    assert _node(data["nodes"], "measure:order_count")["governance"] == "governed"
    assert _node(data["nodes"], "measure:gross_margin")["governance"] == "curated"
    # Edges serialize with the "from" alias, not "from_".
    assert all("from" in e for e in data["edges"])
    assert data["proposals"] == []


def test_semantic_graph_lens_free_response_is_backward_compatible(client, monkeypatch):
    """Compatibility: the additive lens fields are present but a Prompt 12 client
    that ignores them sees an unchanged base graph."""
    monkeypatch.setattr(auto_optimize, "get_serialized_space", lambda space_id: dict(_SPACE))
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    # New top-level lens keys exist (additive), and the base shape is unchanged.
    assert "coverage_status" in data
    assert {"space_id", "nodes", "edges", "proposals"} <= set(data)


def test_semantic_graph_carries_proposals_and_ungoverned_overlay(client, monkeypatch):
    captured: dict = {}

    def fake_load(*args, **kwargs):
        captured.update(kwargs)
        return [{
            "suggestion_id": "sug_x", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1", "candidate_type": "PROPOSE",
            "proposed_object": "finance.sales.order_revenue",
            "evidence": {"recurrence_count": 9},
        }]

    monkeypatch.setattr(auto_optimize, "get_serialized_space", lambda space_id: dict(_SPACE))
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", fake_load)

    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    # Proposals ride the SP-side Delta read, space-scoped (never run-keyed).
    assert captured.get("target_space_id") == "space-1"
    assert data["proposals"][0]["suggestion_id"] == "sug_x"
    ungoverned = _node(data["nodes"], "measure:order_revenue")
    assert ungoverned["governance"] == "ungoverned"


def test_semantic_graph_renders_for_a_never_optimized_space(client, monkeypatch):
    monkeypatch.setattr(
        auto_optimize, "get_serialized_space",
        lambda space_id: {"data_sources": {"tables": [{"identifier": "finance.sales.orders"}]}},
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 200
    data = resp.json()
    assert [n["kind"] for n in data["nodes"]] == ["table"]
    assert data["edges"] == []
    assert data["proposals"] == []


def test_semantic_graph_502_when_config_read_fails(client, monkeypatch):
    def boom(space_id):
        raise RuntimeError("no access")

    monkeypatch.setattr(auto_optimize, "get_serialized_space", boom)
    resp = client.get("/api/auto-optimize/spaces/space-1/semantic-graph")
    assert resp.status_code == 502


# ── Phase 2: ON-predicate column parser + column model (v4 §6) ───────────────


def test_parse_join_columns_matches_qualifiers_to_sides_by_short_name():
    """The alias-qualified equality resolves to (left_col, right_col) matched to
    the two join sides by short name, regardless of authored order."""
    assert auto_optimize._parse_join_columns(
        "`order_items`.`order_id` = `orders`.`order_id`",
        "finance.sales.order_items",
        "finance.sales.orders",
    ) == ("order_id", "order_id")
    # Reversed authored order still maps to the correct physical sides.
    assert auto_optimize._parse_join_columns(
        "orders.order_id = order_items.oid",
        "finance.sales.order_items",
        "finance.sales.orders",
    ) == ("oid", "order_id")


def test_parse_join_columns_ignores_is_current_guard_and_unparseable():
    """An is_current guard (column = literal) is skipped for the real column =
    column equality; a function join that parses to no equality is (None, None)."""
    assert auto_optimize._parse_join_columns(
        "`orders`.`customer_id` = `customer`.`id` AND `customer`.`is_current` = true",
        "finance.sales.orders",
        "finance.ref.customer",
    ) == ("customer_id", "id")
    assert auto_optimize._parse_join_columns(
        "LOWER(orders.email) = LOWER(customer.email)",
        "finance.sales.orders",
        "finance.ref.customer",
    ) == (None, None)


def test_build_graph_emits_join_column_endpoints_and_participating_columns():
    """Join edges carry from_column/to_column parsed from the ON predicate, and
    each table node lists the participating (join-key) columns for the Columns
    LOD — join keys only, deduped and order-preserving."""
    nodes, edges, _s, _r = _build(_SPACE, [])
    by_id = {n.id: n for n in nodes}

    join_oi = next(
        e for e in edges if e.kind == "join" and e.to == "finance.sales.orders"
    )
    assert (join_oi.from_column, join_oi.to_column) == ("order_id", "order_id")

    scd2 = next(e for e in edges if e.kind == "join" and e.to == "finance.ref.customer")
    assert (scd2.from_column, scd2.to_column) == ("customer_id", "id")

    # orders participates in two joins (order_id as the "one" side of one, and
    # customer_id as the "many" side of the other) — both keys, deduped.
    assert by_id["finance.sales.orders"].columns == ["order_id", "customer_id"]
    assert by_id["finance.sales.order_items"].columns == ["order_id"]
    assert by_id["finance.ref.customer"].columns == ["id"]


def test_build_graph_column_model_is_additive_absent_without_joins():
    """A space with no joins emits no column endpoints and no `columns` — a
    client predating the column model renders identically (Phase-1 additivity)."""
    space = {
        "data_sources": {"tables": [{"identifier": "cat.sch.events_wide"}]},
        "instructions": {},
    }
    nodes, edges, _s, _r = _build(space, [])
    assert [e for e in edges if e.kind == "join"] == []
    assert all(n.columns is None for n in nodes if n.kind == "table")
