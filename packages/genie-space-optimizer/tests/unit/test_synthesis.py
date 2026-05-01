"""Structural synthesis tests (Bug #4, Phase 3).

Covers:

* Archetype matcher is deterministic and respects required schema traits.
* The 5-gate validator rejects malformed / leaky / wrong-shape proposals
  and accepts clean ones; each gate is exercised in isolation.
* Caps are enforced per cluster / per archetype / per-run headroom.
* Consecutive-failure budget triggers the deterministic instruction-only
  fallback.
* Synthesized proposal with an arbiter verdict carries provenance.
* End-to-end: every proposal produced by ``synthesize_example_sqls``
  passes the firewall across a 100-benchmark corpus.
"""

from __future__ import annotations

from typing import Any

import pytest

from genie_space_optimizer.optimization.archetypes import (
    ARCHETYPES,
    Archetype,
    pick_archetype,
    schema_traits,
)
from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
from genie_space_optimizer.optimization.synthesis import (
    MAX_SYNTHESIZED_PER_ARCHETYPE,
    MAX_SYNTHESIZED_PER_CLUSTER,
    SynthesisBudget,
    _extract_json_proposal,
    _gate_firewall,
    _gate_parse,
    _gate_structural,
    instruction_only_fallback,
    synthesize_example_sqls,
    validate_synthesis_proposal,
)


# ── Fixtures ───────────────────────────────────────────────────────────


_BENCHMARKS = [
    {
        "id": f"q{i}",
        "question": f"Benchmark question number {i}",
        "expected_sql": f"SELECT col_{i} FROM t WHERE k = {i}",
    }
    for i in range(5)
]


_SCHEMA_SNAPSHOT = {
    "tables": [
        {
            "name": "sales",
            "column_configs": [
                {"name": "category", "type_text": "string"},
                {"name": "revenue", "type_text": "double"},
                {"name": "order_date", "type_text": "date"},
            ],
        },
        {
            "name": "orders",
            "column_configs": [
                {"name": "customer_id", "type_text": "long"},
                {"name": "amount", "type_text": "double"},
            ],
        },
    ],
}


@pytest.fixture
def corpus() -> BenchmarkCorpus:
    return BenchmarkCorpus.from_benchmarks(_BENCHMARKS)


# ── Archetype matcher ──────────────────────────────────────────────────


def test_schema_traits_extracts_all_kinds() -> None:
    traits = schema_traits(_SCHEMA_SNAPSHOT)
    assert "has_numeric" in traits
    assert "has_date" in traits
    assert "has_categorical" in traits
    assert "has_joinable" in traits


def test_schema_traits_reads_production_data_sources_shape() -> None:
    """Regression guard: the Genie ``serialized_space`` shape nests tables
    under ``data_sources.tables``. ``schema_traits`` must read that path,
    not only the top-level ``tables`` key. Historically it didn't, and
    the preflight planner collapsed to a single ``filter_compose``
    archetype in production."""
    production_shape = {
        "data_sources": {
            "tables": _SCHEMA_SNAPSHOT["tables"],
            "metric_views": [{"identifier": "cat.sch.mv_sales"}],
        },
    }
    traits = schema_traits(production_shape)
    assert "has_numeric" in traits
    assert "has_date" in traits
    assert "has_categorical" in traits
    assert "has_joinable" in traits
    assert "has_metric_view" in traits


def test_pick_archetype_deterministic() -> None:
    afs = {"failure_type": "wrong_aggregation", "blame_set": ["sales.revenue"]}
    first = pick_archetype(afs, _SCHEMA_SNAPSHOT)
    second = pick_archetype(afs, _SCHEMA_SNAPSHOT)
    assert first is not None
    assert first is second


def test_pick_archetype_respects_schema_traits() -> None:
    # Schema with no numeric columns — archetypes that require numeric
    # must be skipped.
    skinny = {"tables": [{"name": "t", "column_configs": [{"name": "s", "type_text": "string"}]}]}
    afs = {"failure_type": "wrong_aggregation"}
    chosen = pick_archetype(afs, skinny)
    if chosen is not None:
        assert "has_numeric" not in chosen.required_schema_traits


def test_pick_archetype_falls_back_to_simple_enumerate_for_unknown() -> None:
    """Phase 1.R4: unknown root causes are caught by the
    ``simple_enumerate`` safety net rather than leaving the caller
    without an archetype. The previous contract (return None) no
    longer holds — the planner always has a fallback to stop empty
    synthesis batches."""
    afs = {"failure_type": "totally_unknown_root_cause_xyz"}
    picked = pick_archetype(afs, _SCHEMA_SNAPSHOT)
    assert picked is not None
    assert picked.name == "simple_enumerate"


def test_archetype_catalog_covers_common_root_causes() -> None:
    covered: set[str] = set()
    for a in ARCHETYPES:
        covered |= a.applicable_root_causes
    for must in (
        "missing_aggregation", "wrong_aggregation", "missing_filter",
        "wrong_join", "missing_limit",
        # Cluster vocabulary aliases (added in the SQL-shape patterns +
        # archetype reconciliation pass).
        "wrong_filter_condition", "wrong_join_spec",
        # P1 pattern labels emitted by ``_detect_failure_pattern``.
        "plural_top_n_collapse", "time_window_pivot",
        "value_format_mismatch", "column_disambiguation",
        "granularity_drop",
    ):
        assert must in covered, f"No archetype covers root cause {must!r}"


@pytest.mark.parametrize(
    "failure_type, expected_archetype",
    [
        ("plural_top_n_collapse", "ordered_list_by_metric"),
        ("time_window_pivot", "period_over_period"),
        ("value_format_mismatch", "filter_compose"),
        ("column_disambiguation", "disambiguate_column"),
        ("granularity_drop", "group_by_all_projected_keys"),
    ],
)
def test_pick_archetype_for_pattern_labels(
    failure_type: str, expected_archetype: str,
) -> None:
    """The five P1 pattern labels must each route to a tailored archetype
    (not the ``simple_enumerate`` safety net) when the schema snapshot
    has the standard numeric+date+categorical traits.
    """
    afs = {"failure_type": failure_type}
    picked = pick_archetype(afs, _SCHEMA_SNAPSHOT)
    assert picked is not None
    assert picked.name == expected_archetype, (
        f"{failure_type} routed to {picked.name}, expected {expected_archetype}"
    )


@pytest.mark.parametrize(
    "failure_type",
    ["wrong_join_spec", "missing_join_spec", "wrong_join", "wrong_join_type"],
)
def test_join_spec_causes_route_to_correct_join_spec(failure_type: str) -> None:
    """Join-spec failures must route to ``correct_join_spec``, not
    ``cohort_retention``.

    Before this change, ``_ROOT_CAUSES_JOIN`` was added to
    ``cohort_retention``, which would synthesize an unrelated cohort-by-
    first-activity-month example for any join-spec failure. The dedicated
    ``correct_join_spec`` archetype now owns the join causes.
    """
    afs = {"failure_type": failure_type}
    picked = pick_archetype(afs, _SCHEMA_SNAPSHOT)
    assert picked is not None
    assert picked.name == "correct_join_spec", (
        f"{failure_type} routed to {picked.name}, expected correct_join_spec"
    )


def test_cohort_retention_no_longer_claims_join_causes() -> None:
    """cohort_retention's applicable_root_causes must not include any of
    the join-spec labels — those now belong to correct_join_spec."""
    cohort = next(a for a in ARCHETYPES if a.name == "cohort_retention")
    for rc in (
        "wrong_join_spec",
        "missing_join_spec",
        "wrong_join",
        "wrong_join_type",
    ):
        assert rc not in cohort.applicable_root_causes, (
            f"cohort_retention should not claim {rc}"
        )


# ── _extract_json_proposal ────────────────────────────────────────────


def test_extract_json_handles_fenced() -> None:
    raw = 'preamble\n```json\n{"example_question":"q","example_sql":"select 1"}\n```\ntail'
    p = _extract_json_proposal(raw)
    assert p == {"example_question": "q", "example_sql": "select 1"}


def test_extract_json_handles_inline() -> None:
    raw = 'The proposal is: {"example_question":"q","example_sql":"select 1"} ok'
    p = _extract_json_proposal(raw)
    assert p is not None
    assert p["example_sql"] == "select 1"


def test_extract_json_handles_garbage() -> None:
    assert _extract_json_proposal("no json here") is None
    assert _extract_json_proposal("") is None


# ── 5-gate validator, per gate ─────────────────────────────────────────


@pytest.fixture
def _top_n_archetype() -> Archetype:
    return next(a for a in ARCHETYPES if a.name == "top_n_by_metric")


def test_parse_gate_rejects_empty_fields() -> None:
    result = _gate_parse({"example_question": "", "example_sql": ""})
    assert not result.passed
    assert result.gate == "parse"


def test_parse_gate_rejects_unparseable_sql() -> None:
    result = _gate_parse({"example_question": "q", "example_sql": "THIS IS NOT SQL SELECT FROM"})
    assert not result.passed


def test_parse_gate_passes_valid() -> None:
    result = _gate_parse({
        "example_question": "What are top 10 products?",
        "example_sql": "SELECT category, SUM(revenue) FROM sales GROUP BY category ORDER BY SUM(revenue) DESC LIMIT 10",
    })
    assert result.passed


def test_structural_gate_rejects_missing_construct(_top_n_archetype: Archetype) -> None:
    # top_n requires GROUP_BY, ORDER_BY, LIMIT — this proposal has none.
    result = _gate_structural(
        {"example_sql": "SELECT * FROM sales"},
        _top_n_archetype,
    )
    assert not result.passed


def test_structural_gate_passes_matching_shape(_top_n_archetype: Archetype) -> None:
    result = _gate_structural(
        {
            "example_sql": (
                "SELECT category, SUM(revenue) FROM sales "
                "GROUP BY category ORDER BY SUM(revenue) DESC LIMIT 10"
            )
        },
        _top_n_archetype,
    )
    assert result.passed


def test_firewall_gate_rejects_leaky_proposal(corpus: BenchmarkCorpus) -> None:
    # Directly embeds benchmark expected_sql.
    proposal = {
        "example_question": "any",
        "example_sql": _BENCHMARKS[0]["expected_sql"],
    }
    result = _gate_firewall(proposal, corpus)
    assert not result.passed
    assert "firewall" in result.gate


def test_firewall_gate_passes_clean(corpus: BenchmarkCorpus) -> None:
    proposal = {
        "example_question": "Completely original question about margins",
        "example_sql": "SELECT 1 FROM margins LIMIT 1",
    }
    result = _gate_firewall(proposal, corpus)
    assert result.passed


# ── Full validate_synthesis_proposal ───────────────────────────────────


def test_validate_synthesis_short_circuits_on_parse_fail(corpus: BenchmarkCorpus) -> None:
    arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
    ok, results = validate_synthesis_proposal(
        {"example_question": "", "example_sql": ""},
        archetype=arch,
        benchmark_corpus=corpus,
    )
    assert not ok
    assert results[0].gate == "parse"
    # Other gates must not run after parse fails.
    assert len(results) == 1


def test_validate_synthesis_accepts_clean_proposal(corpus: BenchmarkCorpus) -> None:
    arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
    proposal = {
        "example_question": "What are top 10 categories by total margin?",
        "example_sql": (
            "SELECT category, SUM(margin) AS total_margin FROM sales "
            "GROUP BY category ORDER BY total_margin DESC LIMIT 10"
        ),
    }
    ok, results = validate_synthesis_proposal(
        proposal,
        archetype=arch,
        benchmark_corpus=corpus,
    )
    assert ok, [r.__dict__ for r in results]


# ── Caps / fallback (P3.4) ─────────────────────────────────────────────


def test_synthesis_budget_enforces_cluster_cap() -> None:
    budget = SynthesisBudget.new()
    for _ in range(MAX_SYNTHESIZED_PER_CLUSTER):
        budget.record_success("C1", "top_n_by_metric")
    ok, reason = budget.may_synthesize("C1", "another_archetype", 0)
    assert not ok
    assert reason == "cluster_cap"


def test_synthesis_budget_enforces_archetype_cap() -> None:
    budget = SynthesisBudget.new()
    for i in range(MAX_SYNTHESIZED_PER_ARCHETYPE):
        budget.record_success(f"C{i}", "top_n_by_metric")
    ok, reason = budget.may_synthesize("C_new", "top_n_by_metric", 0)
    assert not ok
    assert reason == "archetype_cap"


def test_synthesis_budget_fallback_after_repeated_failures() -> None:
    budget = SynthesisBudget.new()
    for _ in range(3):
        budget.record_failure()
    assert budget.should_fallback()


def test_instruction_fallback_declines_sql_shape_counterfactuals() -> None:
    afs = {
        "cluster_id": "C_fallback",
        "failure_type": "wrong_aggregation",
        "blame_set": ["PAYMENT_CURRENCY_CD", "PAYMENT_AMT"],
        "counterfactual_fixes": [
            "Add an instruction in the Genie Space metadata clarifying that "
            "PAYMENT_AMT is already in USD and does not require filtering by "
            "PAYMENT_CURRENCY_CD = USD.",
        ],
        "suggested_fix_summary": (
            "Root cause: wrong_aggregation; Blamed: PAYMENT_CURRENCY_CD; "
            "1 question(s) affected"
        ),
    }

    proposal = instruction_only_fallback(afs)

    assert proposal is None


def test_instruction_fallback_keeps_diagnostics_in_provenance_only() -> None:
    afs = {
        "cluster_id": "C_non_structural",
        "failure_type": "missing_instruction",
        "blame_set": ["PAYMENT_AMT", "PAYMENT_CURRENCY_CD"],
        "suggested_fix_summary": (
            "Root cause: missing_instruction; Blamed: PAYMENT_AMT; "
            "1 question(s) affected"
        ),
        "publishable_instruction_candidates": [
            {
                "section_name": "DATA QUALITY NOTES",
                "text": (
                    "PAYMENT_AMT is USD-denominated; avoid adding "
                    "PAYMENT_CURRENCY_CD filters unless the user explicitly "
                    "asks to compare source currencies."
                ),
                "assets": ["PAYMENT_AMT", "PAYMENT_CURRENCY_CD"],
            }
        ],
    }

    proposal = instruction_only_fallback(afs)

    assert proposal is not None
    assert "Root cause" not in proposal["new_text"]
    assert "Blamed" not in proposal["new_text"]
    assert "question(s) affected" not in proposal["new_text"]
    assert proposal["provenance"]["failure_type"] == "missing_instruction"
    assert proposal["provenance"]["cluster_id"] == "C_non_structural"


def test_instruction_fallback_publishes_only_explicit_instruction_candidate() -> None:
    afs = {
        "cluster_id": "C_fallback",
        "failure_type": "missing_instruction",
        "blame_set": ["PAYMENT_AMT", "PAYMENT_CURRENCY_CD"],
        "counterfactual_fixes": [
            "Add an instruction in the Genie Space metadata clarifying that "
            "PAYMENT_AMT is already in USD.",
        ],
        "publishable_instruction_candidates": [
            {
                "section_name": "DATA QUALITY NOTES",
                "text": (
                    "PAYMENT_AMT is USD-denominated; do not infer a "
                    "PAYMENT_CURRENCY_CD = 'USD' filter from a request for "
                    "total payment amount in USD."
                ),
                "assets": ["PAYMENT_AMT", "PAYMENT_CURRENCY_CD"],
            }
        ],
    }

    proposal = instruction_only_fallback(afs)

    assert proposal is not None
    assert proposal["patch_type"] == "update_instruction_section"
    assert proposal["section_name"] == "DATA QUALITY NOTES"
    assert "PAYMENT_AMT is USD-denominated" in proposal["new_text"]
    assert "Guidance for" not in proposal["new_text"]
    assert "Root cause" not in proposal["new_text"]
    assert "Affected:" not in proposal["new_text"]


# ── End-to-end: synthesize + firewall ──────────────────────────────────


def test_synthesize_attaches_archetype_provenance(corpus: BenchmarkCorpus) -> None:
    """Happy path with a mocked LLM — the returned proposal carries the
    archetype name and the cluster id in its provenance."""
    afs = {
        "failure_type": "wrong_aggregation",
        "cluster_id": "C_p",
        "affected_judge": "schema_accuracy",
        "question_count": 3,
        "blame_set": ["sales.revenue"],
        "structural_diff": {"missing_constructs": ["GROUP_BY"]},
    }
    cluster = {
        "cluster_id": afs["cluster_id"],
        "root_cause": afs["failure_type"],
        "question_ids": ["q1", "q2", "q3"],
        "asi_blame_set": afs["blame_set"],
    }

    def fake_llm(prompt: str) -> str:
        # Emit a structurally-correct top-N with none of the benchmark text.
        return (
            '{"example_question":"What are top 5 categories by total revenue?",'
            '"example_sql":"SELECT category, SUM(revenue) AS total_rev FROM sales '
            'GROUP BY category ORDER BY total_rev DESC LIMIT 5",'
            '"usage_guidance":"Ranking by aggregate",'
            '"rationale":"Address missing aggregation"}'
        )

    budget = SynthesisBudget.new()
    proposal = synthesize_example_sqls(
        cluster, _SCHEMA_SNAPSHOT, corpus,
        budget=budget, existing_example_sql_count=0,
        llm_caller=fake_llm,
    )
    assert proposal is not None, "Synthesis must succeed with a clean LLM output"
    assert proposal.get("provenance", {}).get("source") == "structural_synthesis"
    assert proposal.get("provenance", {}).get("cluster_id") == "C_p"
    # Budget bumped.
    assert budget.total == 1


def test_synthesize_retries_on_firewall_rejection(corpus: BenchmarkCorpus) -> None:
    """First LLM response is leaky; retry produces a clean one; final
    proposal must be accepted."""
    cluster = {
        "cluster_id": "C_retry",
        "root_cause": "wrong_aggregation",
        "question_ids": ["q1", "q2"],
        "asi_blame_set": ["sales.revenue"],
    }

    attempts = {"n": 0}

    def fake_llm(prompt: str) -> str:
        attempts["n"] += 1
        if attempts["n"] == 1:
            return (
                '{"example_question":"Benchmark question number 0",'
                '"example_sql":"' + _BENCHMARKS[0]["expected_sql"] + '"}'
            )
        return (
            '{"example_question":"Which categories generate the most profit?",'
            '"example_sql":"SELECT category, SUM(revenue) AS sr FROM sales '
            'GROUP BY category ORDER BY sr DESC LIMIT 5",'
            '"usage_guidance":"Top categories"}'
        )

    budget = SynthesisBudget.new()
    proposal = synthesize_example_sqls(
        cluster, _SCHEMA_SNAPSHOT, corpus,
        budget=budget, llm_caller=fake_llm,
    )
    assert proposal is not None
    assert attempts["n"] == 2  # exactly one retry


def test_synthesize_returns_none_when_both_attempts_fail(corpus: BenchmarkCorpus) -> None:
    cluster = {
        "cluster_id": "C_fail",
        "root_cause": "wrong_aggregation",
        "question_ids": ["q1"],
        "asi_blame_set": ["sales.revenue"],
    }

    def fake_llm(prompt: str) -> str:
        return '{"example_question":"","example_sql":""}'

    budget = SynthesisBudget.new()
    result = synthesize_example_sqls(
        cluster, _SCHEMA_SNAPSHOT, corpus,
        budget=budget, llm_caller=fake_llm,
    )
    assert result is None
    assert budget.consecutive_failures >= 1


def test_full_pipeline_no_leak_across_large_corpus() -> None:
    """Even when the cluster embeds a benchmark verbatim in sql_contexts,
    the synthesis path + firewall guarantee no benchmark text appears in
    the returned proposal."""
    big_benchmarks = [
        {
            "id": f"b{i}",
            "question": f"Some business question {i} about sales",
            "expected_sql": f"SELECT x{i} FROM sales WHERE y = {i}",
        }
        for i in range(100)
    ]
    corpus = BenchmarkCorpus.from_benchmarks(big_benchmarks)
    cluster = {
        "cluster_id": "C_big",
        "root_cause": "wrong_aggregation",
        "question_ids": [b["id"] for b in big_benchmarks[:5]],
        "asi_blame_set": ["sales.revenue"],
        "sql_contexts": [
            {
                "question": b["question"],
                "expected_sql": b["expected_sql"],
                "generated_sql": b["expected_sql"].replace("SELECT", "select"),
            }
            for b in big_benchmarks[:5]
        ],
    }

    def fake_llm(prompt: str) -> str:
        # Should never echo any benchmark text in prompts — the prompt
        # itself is AFS-scrubbed, so we assert on it as well.
        for b in big_benchmarks:
            assert b["question"] not in prompt, (
                f"Benchmark question leaked into prompt: {b['id']}"
            )
            assert b["expected_sql"] not in prompt, (
                f"Benchmark SQL leaked into prompt: {b['id']}"
            )
        return (
            '{"example_question":"Top categories contribution to revenue",'
            '"example_sql":"SELECT category, SUM(revenue) AS sr FROM sales '
            'GROUP BY category ORDER BY sr DESC LIMIT 5",'
            '"usage_guidance":"Ranking"}'
        )

    proposal = synthesize_example_sqls(
        cluster, _SCHEMA_SNAPSHOT, corpus, llm_caller=fake_llm,
    )
    if proposal is not None:
        # Final proposal must not reproduce any benchmark.
        flat = proposal["example_sql"] + " " + proposal["example_question"]
        for b in big_benchmarks:
            assert b["expected_sql"] not in flat


# ─────────────────────────────────────────────────────────────────────
# Fix 3a — schema rendering signals struct-vs-table distinction.
#
# ``_build_schema_data`` in :mod:`optimization.optimizer` is the single
# source of truth for the schema brief shipped to every Lever 6 /
# synthesis prompt. The lever loop run that triggered the
# ``UNRESOLVED_COLUMN: dim_date`` cluster of 34 candidates landed
# because the brief failed to differentiate:
#
#   * ``dim_location`` — a ``struct<region:string, …>`` column on the
#     metric view; ``dim_location.region`` is a valid nested ref.
#   * ``mv_esr_dim_date`` — a separate metric view reachable via JOIN;
#     ``dim_date.year`` is an ``UNRESOLVED_COLUMN`` unless joined.
#
# The fix attaches ``kind: "struct"`` + ``fields`` to struct columns
# and ``related_tables`` derived from ``join_specs`` to each table
# entry so the LLM can tell them apart.
# ─────────────────────────────────────────────────────────────────────


class TestBuildSchemaDataStructAndRelated:
    def _snapshot_with_struct_and_join(self) -> dict:
        # MV ``mv_sales`` has a real struct column ``dim_location``
        # (nested fields) and a join_spec to ``mv_dim_date``. The two
        # signals together let the LLM resolve ``dim_location.region``
        # as a nested field but ``dim_date.year`` only when joined.
        return {
            "data_sources": {
                "tables": [],
                "metric_views": [
                    {
                        "identifier": "cat.sch.mv_sales",
                        "name": "mv_sales",
                        "description": "Sales metric view",
                        "column_configs": [
                            {
                                "column_name": "cy_sales",
                                "data_type": "decimal(18,2)",
                            },
                            {
                                "column_name": "dim_location",
                                "data_type": (
                                    "struct<region:string,"
                                    "city:string,"
                                    "store_no:int>"
                                ),
                            },
                        ],
                    },
                    {
                        "identifier": "cat.sch.mv_dim_date",
                        "name": "mv_dim_date",
                        "column_configs": [
                            {"column_name": "year", "data_type": "int"},
                            {"column_name": "month", "data_type": "int"},
                        ],
                    },
                ],
            },
            "instructions": {
                "join_specs": [
                    {
                        "left": {
                            "identifier": "cat.sch.mv_sales",
                            "column": "date_id",
                        },
                        "right": {
                            "identifier": "cat.sch.mv_dim_date",
                            "column": "date_id",
                        },
                    },
                ],
            },
        }

    def test_struct_column_carries_kind_and_fields(self) -> None:
        from genie_space_optimizer.optimization.optimizer import _build_schema_data

        schema = _build_schema_data(self._snapshot_with_struct_and_join())
        sales_entry = next(e for e in schema if e["table"] == "cat.sch.mv_sales")
        cols_by_name = {c["name"]: c for c in sales_entry["columns"]}

        loc = cols_by_name["dim_location"]
        assert loc.get("kind") == "struct", (
            f"struct column missing kind=struct marker: {loc!r}"
        )
        # Top-level field names only — nested struct fields are NOT
        # flattened (parser-correctness regression coverage).
        assert loc.get("fields") == ["region", "city", "store_no"]

        # Plain decimal column gets neither marker — kind/fields are
        # struct-only signals.
        cy = cols_by_name["cy_sales"]
        assert "kind" not in cy
        assert "fields" not in cy

    def test_related_tables_attached_from_join_specs(self) -> None:
        from genie_space_optimizer.optimization.optimizer import _build_schema_data

        schema = _build_schema_data(self._snapshot_with_struct_and_join())

        # Both sides of the join_spec see each other in related_tables —
        # the index is bidirectional so the LLM gets the same signal
        # whichever table is the FROM table.
        sales_entry = next(e for e in schema if e["table"] == "cat.sch.mv_sales")
        related = sales_entry.get("related_tables") or []
        assert any(
            r.get("table") == "cat.sch.mv_dim_date" for r in related
        ), f"sales entry missing related dim_date: {related!r}"
        # join_on is rendered with the leaf-only ``L.col = R.col`` form.
        assert any(
            "mv_sales.date_id" in (r.get("join_on") or "")
            and "mv_dim_date.date_id" in (r.get("join_on") or "")
            for r in related
        ), f"join_on not rendered as 'L.col = R.col': {related!r}"

        date_entry = next(
            e for e in schema if e["table"] == "cat.sch.mv_dim_date"
        )
        date_related = date_entry.get("related_tables") or []
        assert any(
            r.get("table") == "cat.sch.mv_sales" for r in date_related
        )

    def test_table_with_no_join_spec_omits_related_tables(self) -> None:
        from genie_space_optimizer.optimization.optimizer import _build_schema_data

        snapshot = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t_lonely",
                        "name": "t_lonely",
                        "column_configs": [
                            {"column_name": "id", "data_type": "long"},
                        ],
                    },
                ],
                "metric_views": [],
            },
            "instructions": {"join_specs": []},
        }
        schema = _build_schema_data(snapshot)
        entry = next(e for e in schema if e["table"] == "cat.sch.t_lonely")
        assert "related_tables" not in entry

    def test_non_struct_data_types_do_not_get_marked(self) -> None:
        from genie_space_optimizer.optimization.optimizer import _build_schema_data

        snapshot = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "name": "t",
                        "column_configs": [
                            {"column_name": "tags", "data_type": "array<string>"},
                            {"column_name": "props", "data_type": "map<string,int>"},
                            {"column_name": "name", "data_type": "string"},
                        ],
                    },
                ],
                "metric_views": [],
            },
            "instructions": {"join_specs": []},
        }
        schema = _build_schema_data(snapshot)
        entry = next(e for e in schema if e["table"] == "cat.sch.t")
        for col in entry["columns"]:
            assert "kind" not in col, (
                f"non-struct {col['name']} ({col.get('type')}) "
                f"wrongly flagged: {col!r}"
            )

    def test_parse_struct_field_names_skips_nested_fields(self) -> None:
        """Nested struct fields are NOT surfaced — only top-level names."""
        from genie_space_optimizer.optimization.optimizer import (
            _parse_struct_field_names,
        )

        out = _parse_struct_field_names(
            "struct<a:int,b:struct<c:int,d:int>,e:array<int>>",
        )
        assert out == ["a", "b", "e"], (
            f"top-level only expected, got {out!r}"
        )

    def test_parse_struct_field_names_returns_empty_for_non_struct(self) -> None:
        from genie_space_optimizer.optimization.optimizer import (
            _parse_struct_field_names,
        )

        assert _parse_struct_field_names("") == []
        assert _parse_struct_field_names("string") == []
        assert _parse_struct_field_names("array<string>") == []
        assert _parse_struct_field_names("decimal(18,2)") == []


def test_plural_top_n_archetype_routes_to_ordered_list_without_limit() -> None:
    from genie_space_optimizer.optimization.archetypes import (
        ARCHETYPES,
        pick_archetype,
    )

    archetype = next(a for a in ARCHETYPES if a.name == "ordered_list_by_metric")

    picked = pick_archetype(
        {"failure_type": "plural_top_n_collapse"},
        {
            "data_sources": {
                "tables": [
                    {
                        "column_configs": [
                            {"name": "zone_vp_name", "data_type": "STRING"},
                            {"name": "cy_sales", "data_type": "DOUBLE"},
                        ]
                    }
                ]
            }
        },
    )

    assert picked is not None
    assert picked.name == "ordered_list_by_metric"
    assert "plural" in archetype.prompt_template.lower()
    assert "do not filter to rank = 1" in archetype.prompt_template.lower()
    assert "LIMIT" not in archetype.output_shape["requires_constructs"]


def test_instruction_fallback_declines_payment_currency_pollution_regression() -> None:
    afs = {
        "cluster_id": "C_payment_currency",
        "failure_type": "wrong_aggregation",
        "blame_set": [
            "PAYMENT_CURRENCY_CD",
            "PAYMENT_CURRENCY_CD = USD filter",
            [
                "PAYMENT_CURRENCY_CD",
                "CREDIT_CARD_PAYMENT_VENDOR_CD",
                "FORM_OF_PAYMENT_CD",
                "VCR_CREATE_DT",
            ],
        ],
        "counterfactual_fixes": [
            (
                "Remove the PAYMENT_CURRENCY_CD = USD filter since the user "
                "asked for total payment amount in USD which likely refers to "
                "the label/alias of the amount column rather than filtering by "
                "currency code."
            ),
            (
                "Add an instruction in the Genie Space metadata clarifying "
                "that PAYMENT_AMT is already in USD and does not require "
                "filtering by PAYMENT_CURRENCY_CD = USD."
            ),
        ],
        "suggested_fix_summary": (
            "Root cause: wrong_aggregation; Blamed: PAYMENT_CURRENCY_CD, "
            "PAYMENT_CURRENCY_CD = USD filter; 1 question(s) affected"
        ),
    }

    assert instruction_only_fallback(afs) is None
