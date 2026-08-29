"""Tests for the single metric view YAML renderer and its static validator.

Golden coverage is organised around the thing that varies: the multi-hop ladder
rung. One rung-by-rung golden pins the ``source`` / ``joins`` region each rung
produces (the only region a rung decides), and one whole-document golden pins
everything else — comment structure, field ordering, quoting of ``version`` and
``'on'``, format blocks. Splitting it that way keeps a rung change readable in a
diff instead of re-baselining four near-identical documents.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import sqlglot
import yaml

from genie_space_optimizer.common.config import (
    MV_CAPABILITY_NESTED_JOINS,
    MV_COMMENT_SECTIONS,
    MV_ECHO_CHECK_COMPARED,
    MV_ECHO_CHECK_NOT_COMPARED,
    MV_JOIN_STRATEGY_DENORMALIZED,
    MV_JOIN_STRATEGY_DIRECT,
    MV_JOIN_STRATEGY_NESTED,
    MV_JOIN_STRATEGY_SUBQUERY,
    MV_SYNONYMS_MAX,
    MV_SYNONYMS_MIN,
)
from genie_space_optimizer.optimization import mv_yaml as mv_yaml_module
from genie_space_optimizer.optimization.leakage import BenchmarkCorpus, LeakageOracle
from genie_space_optimizer.optimization.mv_fingerprint import (
    SHAPE_CONDITIONAL_COUNT,
    SHAPE_GUIDANCE,
    SHAPE_PCT_OF_TOTAL,
    SHAPE_RATIO,
    ShapeMatch,
)
from genie_space_optimizer.optimization.mv_scoring import (
    VERDICT_CONFLICT,
    VERDICT_PROPOSE,
    DedupOutcome,
    MetricViewCandidate,
)
from genie_space_optimizer.optimization.mv_yaml import (
    UNIQUENESS_EXACT,
    UNIQUENESS_SAMPLED,
    UNIQUENESS_UC_CONSTRAINT,
    ColumnFacts,
    JoinHop,
    KeyUniqueness,
    MeasureRequest,
    MvProfiling,
    RequestedAttribute,
    create_ddl,
    generate,
    validate,
)

FACT = "main.sales.fact_orders"
DIM_CUSTOMER = "main.sales.dim_customer"
DIM_NATION = "main.sales.dim_nation"


def _columns(*names: str) -> tuple[ColumnFacts, ...]:
    return tuple(ColumnFacts(name=n) for n in names)


def _candidate(**overrides) -> MetricViewCandidate:
    kwargs = dict(
        space_id="space-abc",
        concept="revenue",
        measure_expr="SUM(net_revenue)",
        source_tables=(FACT,),
        benchmark_question_ids=("rev_001",),
    )
    kwargs.update(overrides)
    return MetricViewCandidate(**kwargs)


def _profiling(
    *,
    hops: tuple[JoinHop, ...] = (),
    attributes: tuple[RequestedAttribute, ...] = (),
    uniqueness: dict | None = None,
    capabilities: dict | None = None,
    dim_customer_columns: tuple[str, ...] = (
        "customer_id",
        "customer_name",
        "market_segment",
        "nation_id",
        "customer_balance",
        "is_current",
    ),
) -> MvProfiling:
    return MvProfiling(
        source_table=FACT,
        table_columns={
            FACT: _columns("order_id", "customer_id", "order_date", "net_revenue", "status"),
            DIM_CUSTOMER: _columns(*dim_customer_columns),
            DIM_NATION: _columns("nation_id", "nation_name", "region_id"),
        },
        uniqueness=uniqueness or {},
        hops=hops,
        attributes=attributes,
        measures=(
            MeasureRequest(
                name="total_revenue",
                expr="SUM(net_revenue)",
                comment="Net revenue after discounts.",
            ),
        ),
        capabilities=capabilities or {},
        domain="sales",
    )


CUSTOMER_HOP = JoinHop(
    alias="dim_customer",
    table=DIM_CUSTOMER,
    left_key="customer_id",
    right_key="customer_id",
    is_current_column="is_current",
    description="customer attributes",
)
NATION_HOP = JoinHop(
    alias="dim_nation",
    table=DIM_NATION,
    left_key="nation_id",
    right_key="nation_id",
    parent="dim_customer",
    description="nation attributes",
)

ORDER_DATE_ATTR = RequestedAttribute(name="order_date", column="order_date")
SEGMENT_ATTR = RequestedAttribute(
    name="market_segment", column="market_segment", hop_alias="dim_customer"
)
NATION_ATTR = RequestedAttribute(
    name="nation_name", column="nation_name", hop_alias="dim_nation"
)

PROVEN = {
    (DIM_CUSTOMER, "customer_id"): KeyUniqueness(
        table=DIM_CUSTOMER,
        column="customer_id",
        kind=UNIQUENESS_EXACT,
        row_count=150_000,
        distinct_count=150_000,
    ),
    (DIM_NATION, "nation_id"): KeyUniqueness(
        table=DIM_NATION,
        column="nation_id",
        kind=UNIQUENESS_EXACT,
        row_count=25,
        distinct_count=25,
    ),
}


def _joins_region(yaml_text: str) -> str:
    """The ``source`` + ``joins`` region — the part a ladder rung decides."""
    lines = yaml_text.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("source:"))
    end = next(
        (i for i, line in enumerate(lines) if line.startswith("dimensions:")), len(lines)
    )
    return "\n".join(lines[start:end]).rstrip()


# ── Ladder rung goldens ──────────────────────────────────────────────────


def test_direct_rung_golden_whole_document():
    """A single-hop proposal: the whole document is pinned.

    This is the golden that guards everything a rung does not decide — quoted
    ``version``, the quoted ``'on'`` key, comment section order, dimension and
    measure field order, and the currency format block.
    """
    result = generate(
        _candidate(),
        _profiling(
            hops=(CUSTOMER_HOP,),
            attributes=(ORDER_DATE_ATTR, SEGMENT_ATTR),
            uniqueness=PROVEN,
        ),
    )

    assert result.ok
    assert result.join_strategy == MV_JOIN_STRATEGY_DIRECT
    assert result.yaml_text == (
        'version: \'1.1\'\n'
        'comment: |\n'
        '  PURPOSE: revenue metrics over fact_orders\n'
        '\n'
        '  BEST FOR: total revenue overall | total revenue by order date | total revenue by market segment | total revenue trend over time\n'
        '\n'
        '  NOT FOR: Row-level inspection or record lookup (query main.sales.fact_orders directly instead)\n'
        '\n'
        '  DIMENSIONS: order_date, market_segment\n'
        '\n'
        '  MEASURES: total_revenue\n'
        '\n'
        '  SOURCE: fact_orders (sales domain)\n'
        '\n'
        '  JOINS: dim_customer (customer attributes)\n'
        '\n'
        '  NOTE: Joined dimensions are current-version only where the dimension is versioned\n'
        'source: main.sales.fact_orders\n'
        'joins:\n'
        '  - name: dim_customer\n'
        '    source: main.sales.dim_customer\n'
        '    \'on\': source.customer_id = dim_customer.customer_id AND dim_customer.is_current = true\n'
        '    rely:\n'
        '      at_most_one_match: true\n'
        'dimensions:\n'
        '  - name: order_date\n'
        '    expr: source.order_date\n'
        '    comment: Order Date for slicing revenue.\n'
        '    display_name: Order Date\n'
        '    synonyms:\n'
        '      - order date\n'
        '      - date\n'
        '      - revenue order date\n'
        '      - revenue\n'
        '  - name: market_segment\n'
        '    expr: dim_customer.market_segment\n'
        '    comment: Market Segment for slicing revenue.\n'
        '    display_name: Market Segment\n'
        '    synonyms:\n'
        '      - market segment\n'
        '      - segment\n'
        '      - revenue market segment\n'
        '      - revenue\n'
        'measures:\n'
        '  - name: total_revenue\n'
        '    expr: SUM(source.net_revenue)\n'
        '    comment: Net revenue after discounts.\n'
        '    display_name: Total Revenue\n'
        '    format:\n'
        '      type: currency\n'
        '      currency_code: USD\n'
        '      decimal_places:\n'
        '        type: exact\n'
        '        places: 2\n'
        '      abbreviation: compact\n'
        '    synonyms:\n'
        '      - total revenue\n'
        '      - revenue\n'
        '      - revenue total revenue\n'
    )
    assert validate(result.yaml_text).ok


def test_denormalized_rung_collapses_the_far_hop():
    """Rung 1: the far attribute is carried on the near dimension, so the hop goes."""
    nation_denormalized = RequestedAttribute(
        name="nation_name",
        column="nation_name",
        hop_alias="dim_nation",
        denormalized_on="dim_customer",
        denormalized_column="customer_nation_name",
    )
    profiling = _profiling(
        hops=(CUSTOMER_HOP, NATION_HOP),
        attributes=(ORDER_DATE_ATTR, SEGMENT_ATTR, nation_denormalized),
        uniqueness=PROVEN,
        dim_customer_columns=(
            "customer_id",
            "market_segment",
            "nation_id",
            "customer_nation_name",
            "is_current",
        ),
    )
    result = generate(_candidate(), profiling)

    assert result.ok
    assert result.join_strategy == MV_JOIN_STRATEGY_DENORMALIZED
    assert _joins_region(result.yaml_text) == (
        "source: main.sales.fact_orders\n"
        "joins:\n"
        "  - name: dim_customer\n"
        "    source: main.sales.dim_customer\n"
        "    'on': source.customer_id = dim_customer.customer_id AND dim_customer.is_current = true\n"
        "    rely:\n"
        "      at_most_one_match: true"
    )
    definition = yaml.safe_load(result.yaml_text)
    nation = next(d for d in definition["dimensions"] if d["name"] == "nation_name")
    assert nation["expr"] == "dim_customer.customer_nation_name"
    assert [j["name"] for j in definition["joins"]] == ["dim_customer"]
    assert validate(result.yaml_text).ok


def test_nested_rung_requires_capability_and_proven_keys():
    """Rung 2: nested joins, with the documented ``parent.child.column`` reference."""
    profiling = _profiling(
        hops=(CUSTOMER_HOP, NATION_HOP),
        attributes=(ORDER_DATE_ATTR, SEGMENT_ATTR, NATION_ATTR),
        uniqueness=PROVEN,
        capabilities={MV_CAPABILITY_NESTED_JOINS: "GRANTED"},
    )
    result = generate(_candidate(), profiling)

    assert result.ok
    assert result.join_strategy == MV_JOIN_STRATEGY_NESTED
    assert _joins_region(result.yaml_text) == (
        "source: main.sales.fact_orders\n"
        "joins:\n"
        "  - name: dim_customer\n"
        "    source: main.sales.dim_customer\n"
        "    'on': source.customer_id = dim_customer.customer_id AND dim_customer.is_current = true\n"
        "    rely:\n"
        "      at_most_one_match: true\n"
        "    joins:\n"
        "      - name: dim_nation\n"
        "        source: main.sales.dim_nation\n"
        "        'on': dim_customer.nation_id = dim_nation.nation_id\n"
        "        rely:\n"
        "          at_most_one_match: true"
    )
    definition = yaml.safe_load(result.yaml_text)
    nation = next(d for d in definition["dimensions"] if d["name"] == "nation_name")
    assert nation["expr"] == "dim_customer.dim_nation.nation_name"
    assert validate(result.yaml_text, capabilities=profiling.capabilities).ok


def test_subquery_rung_when_nested_capability_is_unknown():
    """Rung 3: no nested capability, so the chain is pre-joined in the source."""
    profiling = _profiling(
        hops=(CUSTOMER_HOP, NATION_HOP),
        attributes=(ORDER_DATE_ATTR, SEGMENT_ATTR, NATION_ATTR),
        uniqueness=PROVEN,
        capabilities={MV_CAPABILITY_NESTED_JOINS: "UNKNOWN"},
    )
    result = generate(_candidate(), profiling)

    assert result.ok
    assert result.join_strategy == MV_JOIN_STRATEGY_SUBQUERY
    definition = yaml.safe_load(result.yaml_text)
    assert "joins" not in definition

    source_sql = definition["source"]
    assert "LEFT JOIN" in source_sql
    assert "GROUP BY customer_id" in source_sql
    assert "GROUP BY nation_id" in source_sql
    assert "WHERE is_current = true" in source_sql
    assert sqlglot.parse_one(source_sql, read="databricks") is not None

    nation = next(d for d in definition["dimensions"] if d["name"] == "nation_name")
    assert nation["expr"] == "source.nation_name"
    assert validate(result.yaml_text, capabilities=profiling.capabilities).ok


def test_subquery_rung_when_an_intermediate_key_is_not_proven():
    """Capability alone is not enough — an unproven key also drops to rung 3."""
    sampled = dict(PROVEN)
    sampled[(DIM_NATION, "nation_id")] = KeyUniqueness(
        table=DIM_NATION,
        column="nation_id",
        kind=UNIQUENESS_SAMPLED,
        row_count=25,
        distinct_count=25,
    )
    result = generate(
        _candidate(),
        _profiling(
            hops=(CUSTOMER_HOP, NATION_HOP),
            attributes=(ORDER_DATE_ATTR, SEGMENT_ATTR, NATION_ATTR),
            uniqueness=sampled,
            capabilities={MV_CAPABILITY_NESTED_JOINS: "GRANTED"},
        ),
    )

    assert result.join_strategy == MV_JOIN_STRATEGY_SUBQUERY
    assert any("not proof" in reason for reason in result.evidence["unproven_keys"])


# ── rely.at_most_one_match ───────────────────────────────────────────────


def test_rely_is_omitted_unless_uniqueness_is_exactly_proven():
    """Sampled and UC-declared evidence are both refused as proof."""
    for kind in (UNIQUENESS_SAMPLED, UNIQUENESS_UC_CONSTRAINT):
        profiling = _profiling(
            hops=(CUSTOMER_HOP,),
            attributes=(SEGMENT_ATTR,),
            uniqueness={
                (DIM_CUSTOMER, "customer_id"): KeyUniqueness(
                    table=DIM_CUSTOMER,
                    column="customer_id",
                    kind=kind,
                    row_count=1000,
                    distinct_count=1000,
                )
            },
        )
        definition = yaml.safe_load(generate(_candidate(), profiling).yaml_text)
        assert "rely" not in definition["joins"][0], kind


def test_exact_evidence_showing_duplicates_is_not_proof():
    profiling = _profiling(
        hops=(CUSTOMER_HOP,),
        attributes=(SEGMENT_ATTR,),
        uniqueness={
            (DIM_CUSTOMER, "customer_id"): KeyUniqueness(
                table=DIM_CUSTOMER,
                column="customer_id",
                kind=UNIQUENESS_EXACT,
                row_count=1200,
                distinct_count=1000,
            )
        },
    )
    definition = yaml.safe_load(generate(_candidate(), profiling).yaml_text)
    assert "rely" not in definition["joins"][0]


# ── Transitive joins ─────────────────────────────────────────────────────


TRANSITIVE_YAML = """
version: '1.1'
comment: |
  PURPOSE: p

  BEST FOR: a

  NOT FOR: b

  DIMENSIONS: c

  MEASURES: d

  SOURCE: e

  JOINS: f

  NOTE: g
source: main.sales.fact_orders
joins:
  - name: dim_customer
    source: main.sales.dim_customer
    'on': source.customer_id = dim_customer.customer_id
  - name: dim_nation
    source: main.sales.dim_nation
    'on': dim_customer.nation_id = dim_nation.nation_id
dimensions:
  - name: nation_name
    expr: dim_nation.nation_name
    synonyms: [nation, country, nation name]
measures:
  - name: total_revenue
    expr: SUM(source.net_revenue)
    synonyms: [revenue, sales, total revenue]
"""


def test_transitive_sibling_join_is_caught():
    """The whole point of the left-head check: a sibling reference is rejected."""
    report = validate(TRANSITIVE_YAML)

    assert not report.ok
    assert any("transitive join" in error for error in report.errors)
    assert any("dim_nation" in error for error in report.errors)


def test_reversed_operand_order_is_a_warning_not_an_error():
    """``dim.pk = source.fk`` is accepted by metric views, so it must not fail."""
    reversed_yaml = TRANSITIVE_YAML.replace(
        "'on': dim_customer.nation_id = dim_nation.nation_id",
        "'on': dim_nation.nation_id = source.nation_id",
    ).replace("expr: dim_nation.nation_name", "expr: dim_nation.nation_name")

    report = validate(reversed_yaml)

    assert report.ok, report.errors
    assert any("on the left" in warning for warning in report.warnings)


def test_nested_join_may_reference_its_parent_alias():
    nested_yaml = TRANSITIVE_YAML.replace(
        "  - name: dim_nation\n"
        "    source: main.sales.dim_nation\n"
        "    'on': dim_customer.nation_id = dim_nation.nation_id\n",
        "",
    ).replace(
        "    'on': source.customer_id = dim_customer.customer_id\n",
        "    'on': source.customer_id = dim_customer.customer_id\n"
        "    joins:\n"
        "      - name: dim_nation\n"
        "        source: main.sales.dim_nation\n"
        "        'on': dim_customer.nation_id = dim_nation.nation_id\n",
    )

    report = validate(nested_yaml, capabilities={MV_CAPABILITY_NESTED_JOINS: "GRANTED"})

    assert report.ok, report.errors
    assert report.downgrade_to is None


def test_nested_join_without_capability_asks_for_a_downgrade():
    nested_yaml = TRANSITIVE_YAML.replace(
        "  - name: dim_nation\n"
        "    source: main.sales.dim_nation\n"
        "    'on': dim_customer.nation_id = dim_nation.nation_id\n",
        "",
    ).replace(
        "    'on': source.customer_id = dim_customer.customer_id\n",
        "    'on': source.customer_id = dim_customer.customer_id\n"
        "    joins:\n"
        "      - name: dim_nation\n"
        "        source: main.sales.dim_nation\n"
        "        'on': dim_customer.nation_id = dim_nation.nation_id\n",
    )

    report = validate(nested_yaml, capabilities={MV_CAPABILITY_NESTED_JOINS: "UNKNOWN"})

    assert report.ok
    assert report.downgrade_to == MV_JOIN_STRATEGY_SUBQUERY


def test_capability_rows_may_be_typed_objects():
    """The probe's typed rows work without this package importing the backend."""

    class Row:
        capability = MV_CAPABILITY_NESTED_JOINS
        status = "GRANTED"

    nested_yaml = TRANSITIVE_YAML.replace(
        "  - name: dim_nation\n"
        "    source: main.sales.dim_nation\n"
        "    'on': dim_customer.nation_id = dim_nation.nation_id\n",
        "",
    ).replace(
        "    'on': source.customer_id = dim_customer.customer_id\n",
        "    'on': source.customer_id = dim_customer.customer_id\n"
        "    joins:\n"
        "      - name: dim_nation\n"
        "        source: main.sales.dim_nation\n"
        "        'on': dim_customer.nation_id = dim_nation.nation_id\n",
    )

    report = validate(nested_yaml, capabilities=[Row()])

    assert report.downgrade_to is None


# ── Format types ─────────────────────────────────────────────────────────


def test_percent_and_decimal_are_rejected_at_generation_time():
    for bad, correction in (("percent", "percentage"), ("decimal", "number")):
        profiling = _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,))
        profiling = MvProfiling(
            source_table=profiling.source_table,
            table_columns=profiling.table_columns,
            uniqueness=profiling.uniqueness,
            hops=profiling.hops,
            attributes=profiling.attributes,
            measures=(
                MeasureRequest(
                    name="conversion_rate",
                    expr="SUM(net_revenue)",
                    format_type=bad,
                ),
            ),
            capabilities=profiling.capabilities,
            domain=profiling.domain,
        )

        result = generate(_candidate(), profiling)

        assert not result.ok
        assert any(bad in reason for reason in result.rejections)
        assert any(correction in reason for reason in result.rejections)


def test_validate_rejects_unsupported_format_type_in_foreign_yaml():
    bad_yaml = TRANSITIVE_YAML.replace(
        "'on': dim_customer.nation_id = dim_nation.nation_id",
        "'on': source.nation_id = dim_nation.nation_id",
    ).replace(
        "    expr: SUM(source.net_revenue)\n",
        "    expr: SUM(source.net_revenue)\n    format:\n      type: integer\n",
    )

    report = validate(bad_yaml)

    assert not report.ok
    assert any("integer" in error and "number" in error for error in report.errors)


# ── Shapes ───────────────────────────────────────────────────────────────


def test_pct_of_total_uses_a_fixed_lod_dimension_never_measure_over_measure():
    shape = ShapeMatch(
        kind=SHAPE_PCT_OF_TOTAL,
        canonical_expr="sum(net_revenue)",
        fingerprint="fp-pct",
        guidance=SHAPE_GUIDANCE[SHAPE_PCT_OF_TOTAL],
        components=(("measure", "SUM(net_revenue)"),),
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        shapes=[shape],
    )

    assert result.ok
    definition = yaml.safe_load(result.yaml_text)

    lod = next(
        d for d in definition["dimensions"] if d["expr"].endswith("OVER ()")
    )
    assert lod["expr"] == "SUM(source.net_revenue) OVER ()"

    share = next(m for m in definition["measures"] if m["name"] == "revenue_pct_of_total")
    assert "ANY_VALUE(" in share["expr"]
    assert share["expr"].count("MEASURE(") == 1
    assert share["format"]["type"] == "percentage"

    for measure in definition["measures"]:
        assert measure["expr"].count("MEASURE(") <= 1, measure["name"]


def test_ratio_shape_emits_atomic_measures_plus_a_composed_one():
    shape = ShapeMatch(
        kind=SHAPE_RATIO,
        canonical_expr="sum(a)/count(1)",
        fingerprint="fp-ratio",
        guidance=SHAPE_GUIDANCE[SHAPE_RATIO],
        components=(("numerator", "SUM(net_revenue)"), ("denominator", "COUNT(1)")),
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        shapes=[shape],
    )

    assert result.ok
    definition = yaml.safe_load(result.yaml_text)
    names = [m["name"] for m in definition["measures"]]
    assert "revenue_rate_numerator" in names
    assert "revenue_rate_denominator" in names

    composed = next(m for m in definition["measures"] if m["name"] == "revenue_rate")
    assert composed["expr"] == (
        "MEASURE(`revenue_rate_numerator`) / MEASURE(`revenue_rate_denominator`)"
    )


def test_two_same_kind_shapes_get_unique_names_and_validate():
    """Two ratios in one bundle once rendered identical ``<concept>_rate*`` names,
    so UC rejected the create for duplicate measure names
    (METRIC_VIEW_INVALID_VIEW_DEFINITION — deployed review). The second shape's
    base is now de-duplicated, so every measure/dimension name is unique and the
    body validates."""
    ratio_a = ShapeMatch(
        kind=SHAPE_RATIO,
        canonical_expr="sum(a)/count(1)",
        fingerprint="fp-ratio-a",
        guidance=SHAPE_GUIDANCE[SHAPE_RATIO],
        components=(("numerator", "SUM(net_revenue)"), ("denominator", "COUNT(1)")),
    )
    ratio_b = ShapeMatch(
        kind=SHAPE_RATIO,
        canonical_expr="sum(b)/count(1)",
        fingerprint="fp-ratio-b",
        guidance=SHAPE_GUIDANCE[SHAPE_RATIO],
        components=(("numerator", "SUM(net_revenue)"), ("denominator", "COUNT(1)")),
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        shapes=[ratio_a, ratio_b],
    )

    assert result.ok, result.rejections
    definition = yaml.safe_load(result.yaml_text)
    names = [m["name"] for m in definition["measures"]] + [
        d["name"] for d in definition["dimensions"]
    ]
    assert len(names) == len(set(names)), names
    assert "revenue_rate" in names
    assert "revenue_rate_2" in names
    assert "revenue_rate_2_numerator" in names


def test_shape_reading_a_foreign_column_is_dropped():
    """A recurring shape is mined from the whole corpus, so it can reference a
    column this source lacks. Deployed review: that produced a kitchen-sink view
    over foreign columns. A shape whose expression reads a column not in the
    source (and not read by any of the view's measures) is now dropped."""
    foreign = ShapeMatch(
        kind=SHAPE_RATIO,
        canonical_expr="sum(a)/sum(b)",
        fingerprint="fp-foreign",
        guidance=SHAPE_GUIDANCE[SHAPE_RATIO],
        # total_amount / property_id are NOT columns of fact_orders.
        components=(("numerator", "SUM(total_amount)"), ("denominator", "COUNT(DISTINCT property_id)")),
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        shapes=[foreign],
    )

    assert result.ok, result.rejections
    definition = yaml.safe_load(result.yaml_text)
    names = [m["name"] for m in definition["measures"]]
    # Only the primary measure survives; the foreign-column ratio is gone.
    assert names == ["total_revenue"], names


def test_pct_of_total_with_a_non_aggregate_base_is_dropped_not_broken():
    """The fixed-LOD grand total is ``<agg> OVER ()`` — valid only for a bare
    aggregate base. A base with trailing arithmetic once rendered
    ``COUNT(*) * 100.0 OVER ()`` and failed at UC with PARSE_SYNTAX_ERROR
    (deployed review). Such a shape is now dropped rather than rendered."""
    scaled = ShapeMatch(
        kind=SHAPE_PCT_OF_TOTAL,
        canonical_expr="count(*) * ?n",
        fingerprint="fp-scaled",
        guidance=SHAPE_GUIDANCE[SHAPE_PCT_OF_TOTAL],
        components=(("measure", "COUNT(*) * 100.0"),),
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        shapes=[scaled],
    )

    assert result.ok, result.rejections
    definition = yaml.safe_load(result.yaml_text)
    names = [m["name"] for m in definition["measures"]] + [
        d["name"] for d in definition["dimensions"]
    ]
    assert not any("pct_of_total" in n for n in names), names
    # And nothing malformed slipped through: every expr parses.
    assert validate(result.yaml_text).ok


def test_validate_rejects_an_unparseable_expression():
    """The gate SQL-parses every expr, so a malformed ``... OVER ()`` (the
    deployed-review parse error) is rejected locally, not at UC create."""
    bad_yaml = (
        "version: '1.1'\n"
        "comment: |\n"
        "  DOMAIN: sales\n"
        "  GRAIN: order\n"
        "  BEST FOR: totals\n"
        "  NOT FOR: forecasts\n"
        "source: finance.sales.orders\n"
        "dimensions:\n"
        "  - name: grand_total\n"
        "    expr: COUNT(*) * 100.0 OVER ()\n"
        "measures:\n"
        "  - name: total\n"
        "    expr: SUM(net_revenue)\n"
    )
    report = validate(bad_yaml)
    assert not report.ok
    assert any("not parseable" in e and "grand_total" in e for e in report.errors), report.errors


def test_validate_rejects_duplicate_measure_or_dimension_names():
    """The local validator is the gate: a body with a repeated name is rejected
    here rather than at UC create time, and the offending name is named."""
    dup_yaml = (
        "version: '1.1'\n"
        "comment: |\n"
        "  DOMAIN: sales\n"
        "  GRAIN: order\n"
        "  BEST FOR: totals\n"
        "  NOT FOR: forecasts\n"
        "source: finance.sales.orders\n"
        "dimensions:\n"
        "  - name: region\n"
        "    expr: region\n"
        "measures:\n"
        "  - name: region\n"
        "    expr: SUM(net_revenue)\n"
    )
    report = validate(dup_yaml)
    assert not report.ok
    assert any("unique" in e and "region" in e for e in report.errors), report.errors


def test_conditional_count_uses_filter_not_case():
    shape = ShapeMatch(
        kind=SHAPE_CONDITIONAL_COUNT,
        canonical_expr="count(case when status = ? then 1 end)",
        fingerprint="fp-cond",
        guidance=SHAPE_GUIDANCE[SHAPE_CONDITIONAL_COUNT],
        components=(("condition", "status = 'F'"), ("name", "fulfilled_orders")),
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        shapes=[shape],
    )

    assert result.ok
    definition = yaml.safe_load(result.yaml_text)
    measure = next(m for m in definition["measures"] if m["name"] == "fulfilled_orders")
    assert measure["expr"] == "COUNT(1) FILTER (WHERE source.status = 'F')"
    assert "CASE" not in measure["expr"].upper()


# ── Additive measures ────────────────────────────────────────────────────


def test_measure_over_a_joined_dimension_column_is_a_conflict():
    """Aggregating a dimension column across a join inflates it silently."""
    profiling = _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,))
    profiling = MvProfiling(
        source_table=profiling.source_table,
        table_columns=profiling.table_columns,
        hops=profiling.hops,
        attributes=profiling.attributes,
        measures=(MeasureRequest(name="total_balance", expr="SUM(customer_balance)"),),
        domain=profiling.domain,
    )

    result = generate(_candidate(measure_expr=""), profiling)

    assert result.verdict == VERDICT_CONFLICT
    assert not result.yaml_text
    assert result.conflicts[0]["joined_table"] == DIM_CUSTOMER
    assert result.conflicts[0]["column"] == "customer_balance"


# ── Comment ──────────────────────────────────────────────────────────────


def test_comment_carries_all_eight_sections_and_cross_references_the_adjacent_view():
    dedup = DedupOutcome(
        verdict=VERDICT_PROPOSE,
        alternatives=({"pointer": "main.sales.mv_revenue.total_revenue"},),
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        dedup=dedup,
    )

    comment = yaml.safe_load(result.yaml_text)["comment"]
    for section in MV_COMMENT_SECTIONS:
        assert f"{section}:" in comment
    assert "main.sales.mv_revenue" in comment


def test_benchmark_verbatim_best_for_line_is_rejected():
    """A BEST FOR line that echoes a benchmark question never ships."""
    corpus = BenchmarkCorpus.from_benchmarks(
        [
            {
                "id": "rev_001",
                "question": "What is the total revenue by market segment?",
                "expected_sql": "SELECT 1",
            }
        ]
    )
    oracle = LeakageOracle(corpus)

    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        oracle=oracle,
    )

    assert not result.ok
    assert any("BEST FOR" in reason for reason in result.rejections)


def test_a_line_at_exactly_the_threshold_is_rejected():
    """The echo comparison is inclusive, and this test is what pins it.

    ``leakage.contains_question`` compares ``score >= threshold``, so a line
    landing on exactly 0.90 is a rejection. MV-D8 originally read ">0.9" and was
    corrected to the code rather than the reverse; an operator flipped back to
    ``>`` fails here.
    """
    shared = "revenue margin freight discount segment nation quarter shipment supplier"
    oracle = LeakageOracle(
        BenchmarkCorpus.from_benchmarks(
            [{"id": "b1", "question": shared, "expected_sql": "SELECT 1"}]
        )
    )
    # Nine shared content tokens over a ten-token union is 0.90 exactly — the
    # threshold itself. Dropping one shared token instead gives 8/10 = 0.80.
    at_threshold = f"{shared} returns"
    below_threshold = "revenue margin freight discount segment nation quarter shipment returns"

    rejected_at, compared_at = mv_yaml_module._comment_echoes([at_threshold], oracle=oracle)
    rejected_below, compared_below = mv_yaml_module._comment_echoes(
        [below_threshold], oracle=oracle
    )

    assert rejected_at and compared_at
    assert not rejected_below and compared_below


def test_an_unconfigured_echo_check_reports_that_it_compared_nothing():
    """A firewall that cannot run must not read like a firewall that found nothing.

    The oracle is optional input, so the vacuous case is the *common* one, and a
    caller reading only ``ok`` would record a clean pass the check never earned.
    """
    profiling = _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN)

    without = generate(_candidate(), profiling)
    assert without.ok
    assert without.echo_check == MV_ECHO_CHECK_NOT_COMPARED
    assert not without.echo_checked
    assert without.evidence["echo_check"] == MV_ECHO_CHECK_NOT_COMPARED

    with_oracle = generate(
        _candidate(),
        profiling,
        oracle=LeakageOracle(
            BenchmarkCorpus.from_benchmarks(
                [{"id": "x", "question": "How many suppliers ship from Peru?",
                  "expected_sql": "SELECT 1"}]
            )
        ),
    )
    assert with_oracle.ok
    assert with_oracle.echo_check == MV_ECHO_CHECK_COMPARED
    assert with_oracle.echo_checked


def test_validate_reports_the_vacuous_echo_check_as_a_warning():
    """``validate`` carries the same distinction, for YAML it did not render."""
    yaml_text = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
    ).yaml_text

    bare = validate(yaml_text)
    assert bare.ok
    assert bare.echo_check == MV_ECHO_CHECK_NOT_COMPARED
    assert not bare.echo_checked
    assert any("did not run" in w for w in bare.warnings)

    checked = validate(
        yaml_text,
        oracle=LeakageOracle(
            BenchmarkCorpus.from_benchmarks(
                [{"id": "x", "question": "How many suppliers ship from Peru?",
                  "expected_sql": "SELECT 1"}]
            )
        ),
    )
    assert checked.ok
    assert checked.echo_check == MV_ECHO_CHECK_COMPARED
    assert not any("did not run" in w for w in checked.warnings)


def test_validate_catches_an_echoed_best_for_line_in_foreign_yaml():
    """The recovered-intent path is what protects LLM-authored YAML.

    ``generate`` checks the lines it just built; YAML arriving from elsewhere has
    no such provenance, so the intents are parsed back out of the comment.
    """
    yaml_text = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
    ).yaml_text
    best_for = mv_yaml_module._best_for_from_comment(yaml.safe_load(yaml_text))
    assert best_for, "the comment must expose its BEST FOR intents"

    report = validate(
        yaml_text,
        oracle=LeakageOracle(
            BenchmarkCorpus.from_benchmarks(
                [{"id": "leak", "question": best_for[0], "expected_sql": "SELECT 1"}]
            )
        ),
    )

    assert not report.ok
    assert any("BEST FOR" in e for e in report.errors)
    assert report.echo_check == MV_ECHO_CHECK_COMPARED


def test_unrelated_benchmarks_do_not_block_generation():
    corpus = BenchmarkCorpus.from_benchmarks(
        [{"id": "x", "question": "How many suppliers ship from Peru?", "expected_sql": "SELECT 1"}]
    )
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
        oracle=LeakageOracle(corpus),
    )

    assert result.ok


# ── Unsupported fields and synonyms ──────────────────────────────────────


def test_generated_yaml_never_contains_an_unsupported_field():
    result = generate(
        _candidate(),
        _profiling(
            hops=(CUSTOMER_HOP, NATION_HOP),
            attributes=(ORDER_DATE_ATTR, SEGMENT_ATTR, NATION_ATTR),
            uniqueness=PROVEN,
            capabilities={MV_CAPABILITY_NESTED_JOINS: "GRANTED"},
        ),
    )
    definition = yaml.safe_load(result.yaml_text)

    assert "name" not in definition
    assert "time_dimension" not in definition
    assert "window_measures" not in definition
    assert "fields" not in definition

    def _check(joins):
        for join in joins:
            assert "join_type" not in join
            assert "table" not in join
            _check(join.get("joins") or [])

    _check(definition["joins"])


def test_validate_flags_unsupported_top_level_and_join_fields():
    report = validate(
        TRANSITIVE_YAML.replace(
            "'on': dim_customer.nation_id = dim_nation.nation_id",
            "'on': source.nation_id = dim_nation.nation_id",
        ).replace(
            "source: main.sales.fact_orders\n",
            "source: main.sales.fact_orders\nname: should_not_be_here\ntime_dimension: order_date\n",
        ).replace(
            "    source: main.sales.dim_nation\n",
            "    source: main.sales.dim_nation\n    join_type: inner\n",
        )
    )

    assert not report.ok
    joined = " ".join(report.errors)
    assert "unsupported top-level field 'name'" in joined
    assert "unsupported top-level field 'time_dimension'" in joined
    assert "unsupported field 'join_type'" in joined


# A valid nested-join document, used to prove the unsupported-field rules are
# bound to a *path* rather than to a bare key name. Nested so the same key can be
# planted at the top level, at a first-level join and at a deeper one.
PATH_BASE_YAML = """
version: '1.1'
comment: |
  PURPOSE: p

  BEST FOR: a

  NOT FOR: b

  DIMENSIONS: c

  MEASURES: d

  SOURCE: e

  JOINS: f

  NOTE: g
source: main.sales.fact_orders
joins:
  - name: dim_customer
    source: main.sales.dim_customer
    'on': source.customer_id = dim_customer.customer_id
    joins:
      - name: dim_nation
        source: main.sales.dim_nation
        'on': dim_customer.nation_id = dim_nation.nation_id
dimensions:
  - name: nation_name
    expr: dim_customer.dim_nation.nation_name
    synonyms: [nation, country, nation name]
measures:
  - name: total_revenue
    expr: SUM(source.net_revenue)
    synonyms: [revenue, sales, total revenue]
"""

_NESTED_GRANTED = {MV_CAPABILITY_NESTED_JOINS: "GRANTED"}


def test_the_path_base_document_is_valid_before_anything_is_planted():
    """Positive control. Without it, a rejection below proves nothing."""
    report = validate(PATH_BASE_YAML, capabilities=_NESTED_GRANTED)

    assert report.ok, report.errors


def test_validate_rejects_a_numeric_placeholder_in_an_emitted_expr():
    """MV-D29 / MV-D8 gate: the exact defect Scenario D's BYO leg hit live. A
    body rendered from the erased canonical form carries ``?n``, parses as the
    identifier ``n - l_discount`` inside a CREATE VIEW, and fails with
    INVALID_IDENTIFIER. ``validate`` must catch it statically, before any
    warehouse round-trip."""
    planted = PATH_BASE_YAML.replace(
        "expr: SUM(source.net_revenue)",
        "expr: SUM(source.l_extendedprice * (?n - source.l_discount))",
    )
    report = validate(planted, capabilities=_NESTED_GRANTED)

    assert not report.ok
    joined = " ".join(report.errors)
    assert "placeholder" in joined
    assert "?n" in joined
    assert "MV-D29" in joined


def test_validate_rejects_a_string_placeholder_in_an_emitted_expr():
    planted = PATH_BASE_YAML.replace(
        "expr: SUM(source.net_revenue)",
        "expr: SUM(CASE WHEN source.status = ?s THEN 1 END)",
    )
    report = validate(planted, capabilities=_NESTED_GRANTED)

    assert not report.ok
    assert any("?s" in e for e in report.errors)


def test_validate_accepts_a_literal_bearing_measure_expr():
    """The complement: a real numeric literal (the POV's ``1 - l_discount``) is
    not a placeholder and must pass. The gate rejects ``?n``/``?s`` tokens, not
    honest constants — otherwise MV-D29's fix would trade one false gate for
    another."""
    planted = PATH_BASE_YAML.replace(
        "expr: SUM(source.net_revenue)",
        "expr: SUM(source.l_extendedprice * (1 - source.l_discount))",
    )
    report = validate(planted, capabilities=_NESTED_GRANTED)

    assert report.ok, report.errors


def test_window_measures_is_rejected_at_the_top_level_and_ignored_below_it():
    """The unsupported *array* form is a top-level key; ``window:`` per measure is not.

    Two rules share the word: the top-level ``window_measures`` array fails to
    create, while a per-measure ``window`` property is supported. A check keyed on
    the bare name could not tell them apart, so this pins that the top-level rule
    is applied only to top-level keys.
    """
    planted_at_top = PATH_BASE_YAML.replace(
        "source: main.sales.fact_orders\n",
        "source: main.sales.fact_orders\nwindow_measures:\n  - name: rolling_revenue\n",
    )
    report = validate(planted_at_top, capabilities=_NESTED_GRANTED)
    assert not report.ok
    assert any("unsupported top-level field 'window_measures'" in e for e in report.errors)

    # The supported per-measure form, one level down, must survive untouched.
    per_measure_window = PATH_BASE_YAML.replace(
        "    expr: SUM(source.net_revenue)\n",
        "    expr: SUM(source.net_revenue)\n    window: [order_date]\n",
    )
    report = validate(per_measure_window, capabilities=_NESTED_GRANTED)
    assert report.ok, report.errors
    assert not any("window" in e for e in report.errors)


def test_table_is_rejected_inside_any_join_and_allowed_at_legal_positions():
    """``joins[].table`` is the wrong relation key; the word itself is not banned."""
    at_first_level = PATH_BASE_YAML.replace(
        "    source: main.sales.dim_customer\n",
        "    source: main.sales.dim_customer\n    table: main.sales.dim_customer\n",
    )
    report = validate(at_first_level, capabilities=_NESTED_GRANTED)
    assert not report.ok
    assert any(
        "join 'dim_customer': unsupported field 'table'" in e
        and "the relation key is 'source'" in e
        for e in report.errors
    )

    # Depth matters: the walk recurses, so a nested join is checked identically.
    at_nested_level = PATH_BASE_YAML.replace(
        "        source: main.sales.dim_nation\n",
        "        source: main.sales.dim_nation\n        table: main.sales.dim_nation\n",
    )
    report = validate(at_nested_level, capabilities=_NESTED_GRANTED)
    assert not report.ok
    assert any("join 'dim_nation': unsupported field 'table'" in e for e in report.errors)

    # Legal positions for the same word: a field name, a synonym, and a relation
    # name. None of these is the join key, so none may be rejected.
    legal = (
        PATH_BASE_YAML.replace("main.sales.fact_orders", "main.sales.fact_order_table")
        .replace("  - name: nation_name\n", "  - name: table\n")
        .replace("synonyms: [nation, country, nation name]", "synonyms: [table, tables, source table]")
    )
    report = validate(legal, capabilities=_NESTED_GRANTED)
    assert report.ok, report.errors


def test_every_generated_field_has_synonyms_within_bounds():
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(ORDER_DATE_ATTR, SEGMENT_ATTR), uniqueness=PROVEN),
    )
    definition = yaml.safe_load(result.yaml_text)

    for entry in list(definition["dimensions"]) + list(definition["measures"]):
        synonyms = entry["synonyms"]
        assert MV_SYNONYMS_MIN <= len(synonyms) <= MV_SYNONYMS_MAX, entry["name"]
        assert len(set(synonyms)) == len(synonyms), entry["name"]


def test_validate_rejects_out_of_bounds_synonyms():
    report = validate(
        TRANSITIVE_YAML.replace(
            "'on': dim_customer.nation_id = dim_nation.nation_id",
            "'on': source.nation_id = dim_nation.nation_id",
        ).replace("synonyms: [nation, country, nation name]", "synonyms: [nation]")
    )

    assert not report.ok
    assert any("expected between" in error for error in report.errors)


# ── Validation of malformed input ────────────────────────────────────────


def test_validate_rejects_unparseable_and_empty_input():
    assert not validate("").ok
    assert not validate("version: '1.1'\n  bad indent: [").ok
    assert not validate("- just\n- a\n- list\n").ok


def test_validate_requires_the_quoted_version_string():
    report = validate(TRANSITIVE_YAML.replace("version: '1.1'", "version: 1.1"))

    assert not report.ok
    assert any("version must be the quoted string" in error for error in report.errors)


# ── DDL wrapper ──────────────────────────────────────────────────────────


def test_create_ddl_round_trips_the_yaml_body_through_sqlglot():
    """sqlglot has no grammar for WITH METRICS, so this pins body survival.

    The statement parses as an opaque ``Command``; what matters is that
    re-rendering it does not mangle or drop the YAML payload.
    """
    result = generate(
        _candidate(),
        _profiling(hops=(CUSTOMER_HOP,), attributes=(SEGMENT_ATTR,), uniqueness=PROVEN),
    )
    ddl = create_ddl("main.sales.mv_revenue", result.yaml_text, comment="revenue metrics")

    assert "CREATE VIEW main.sales.mv_revenue" in ddl
    assert "WITH METRICS" in ddl
    assert "LANGUAGE YAML" in ddl

    parsed = sqlglot.parse_one(ddl, read="databricks")
    rendered = parsed.sql(dialect="databricks")
    assert "SUM(source.net_revenue)" in rendered
    assert "dim_customer.market_segment" in rendered

    body = ddl.split("AS $$\n", 1)[1].rsplit("$$", 1)[0]
    assert yaml.safe_load(body) == yaml.safe_load(result.yaml_text)


def test_create_ddl_escapes_single_quotes_in_the_comment():
    ddl = create_ddl("c.s.v", "version: '1.1'\n", comment="it's fine")

    assert "COMMENT 'it''s fine'" in ddl


# ── The sole-renderer property ───────────────────────────────────────────


def test_mv_yaml_is_the_only_module_that_renders_yaml():
    """Pins the claim in this module's docstring instead of trusting it.

    "One renderer" is only worth stating if it is enforced. A second
    ``yaml.dump`` anywhere in the package is a second set of quoting and field
    ordering rules, which is how the emitted schema starts to drift from the
    schema the validator checks.
    """
    package_root = Path(mv_yaml_module.__file__).resolve().parents[1]
    offenders: list[str] = []

    for path in sorted(package_root.rglob("*.py")):
        if path.name == "mv_yaml.py":
            continue
        source = path.read_text(encoding="utf-8")
        if re.search(r"\byaml\s*\.\s*(dump|dump_all|safe_dump|safe_dump_all)\s*\(", source):
            offenders.append(str(path.relative_to(package_root)))

    assert offenders == [], f"YAML is rendered outside mv_yaml.py: {offenders}"


# The sanctioned non-mv_yaml occurrences of metric-view DDL text, pinned by exact
# location. Not a substring exemption: the phrase is allowed at *these lines* and
# nowhere else, so a new assembly site fails even if it copies this wording. If an
# edit shifts these lines, re-pin deliberately — that is the same discipline
# MV-D9 applies to quoted anchors.
SANCTIONED_DDL_TEXT_SITES: dict[tuple[str, int], str] = {
    ("optimization/ddl.py", 266): (
        "created_by          STRING        NOT NULL COMMENT 'Identity that executed "
        "CREATE VIEW ... WITH METRICS. Always the consenting user under OBO — never "
        "the service principal',"
    ),
}


def _executable_string_lines(source: str) -> set[int]:
    """Physical line numbers covered by string literals that are not docstrings.

    Docstrings and ``#`` comments describe DDL; an executable string *is* DDL, or
    becomes it. Only the latter can put a statement on a warehouse, so only the
    latter needs pinning. f-string segments count — assembling the statement with
    an f-string is the evasion this distinction has to catch.
    """
    tree = ast.parse(source)
    docstring_ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstring_ids.add(id(body[0].value))
        # A bare string expression statement is a field/constant docstring.
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                docstring_ids.add(id(node.value))

    covered: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in docstring_ids:
                continue
            covered.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return covered


def test_mv_yaml_is_the_only_module_that_builds_metric_view_ddl():
    """The YAML guard above stops a second renderer; this stops a second *statement*.

    ``create_ddl`` is the only sanctioned assembler of
    ``CREATE VIEW ... WITH METRICS LANGUAGE YAML``. An f-string in any other module
    would bypass every check in this file — the validator never sees text that was
    never rendered here — so the phrase is forbidden in executable strings package
    wide, with the known documentation sites pinned by location.
    """
    package_root = Path(mv_yaml_module.__file__).resolve().parents[1]
    phrases = ("WITH METRICS", "CREATE VIEW")
    unpinned: list[str] = []
    stale_pins = dict(SANCTIONED_DDL_TEXT_SITES)

    for path in sorted(package_root.rglob("*.py")):
        if path.name == "mv_yaml.py":
            continue
        source = path.read_text(encoding="utf-8")
        if not any(phrase in source for phrase in phrases):
            continue
        executable = _executable_string_lines(source)
        rel = str(path.relative_to(package_root))
        for lineno, line in enumerate(source.splitlines(), start=1):
            if not any(phrase in line for phrase in phrases):
                continue
            if lineno not in executable:
                continue  # a docstring or comment describing DDL, not building it
            pinned = stale_pins.pop((rel, lineno), None)
            if pinned is None:
                unpinned.append(f"{rel}:{lineno}: {line.strip()[:100]}")
            elif pinned != line.strip():
                unpinned.append(
                    f"{rel}:{lineno}: pinned text no longer matches — re-pin.\n"
                    f"  pinned: {pinned}\n  actual: {line.strip()}"
                )

    assert unpinned == [], "metric-view DDL is assembled outside mv_yaml.py:\n" + "\n".join(unpinned)
    assert stale_pins == {}, f"pinned DDL sites no longer exist — remove the pin: {sorted(stale_pins)}"
