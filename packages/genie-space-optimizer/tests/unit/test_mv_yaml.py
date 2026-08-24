"""Tests for the single metric view YAML renderer and its static validator.

Golden coverage is organised around the thing that varies: the multi-hop ladder
rung. One rung-by-rung golden pins the ``source`` / ``joins`` region each rung
produces (the only region a rung decides), and one whole-document golden pins
everything else — comment structure, field ordering, quoting of ``version`` and
``'on'``, format blocks. Splitting it that way keeps a rung change readable in a
diff instead of re-baselining four near-identical documents.
"""

from __future__ import annotations

import re
from pathlib import Path

import sqlglot
import yaml

from genie_space_optimizer.common.config import (
    MV_CAPABILITY_NESTED_JOINS,
    MV_COMMENT_SECTIONS,
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
