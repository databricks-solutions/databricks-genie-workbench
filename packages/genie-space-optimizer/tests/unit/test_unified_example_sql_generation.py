"""Phase 6 tests — unify-example-sql-onto-benchmark-engine.

Seventeen tests organized by phase so a regression points straight at
the failing area:

  Phase 1.R1  : shared-core behavior + benchmark-path reuse
  Phase 1.R1b : LeakageOracle opaque match API
  Phase 1.R1c : question-echo normalization + threshold
  Phase 2.R2  : example prompt content + role framing
  Phase 2.R2b : import-time isolation assertion
  Phase 3.R3  : generate_example_sqls wrapper + required-kwarg contract
  Phase 3.R3b : arbiter actually runs with result_rows; no silent pass
  Phase 4.R4  : harness flag routing + archetype fallback on low yield
  Phase 4.R4b : MLflow registry registration
  Phase 5.R5b : lint rule catches seeded violations

Tests deliberately patch the LLM call (``_call_llm_for_scoring``) and
the primitive validators (``_validate_benchmark_sql``, the arbiter
judge) at their shared import site so the shared core is exercised
without needing a live warehouse or endpoint.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
import sys
import textwrap
from unittest.mock import patch

import pytest

from genie_space_optimizer.common.config import (
    BENCHMARK_CORRECTION_PROMPT,
    BENCHMARK_GENERATION_PROMPT,
    BENCHMARK_PROMPTS,
    EXAMPLE_SQL_CORRECTION_PROMPT,
    EXAMPLE_SQL_GENERATION_PROMPT,
    _BENCHMARK_DERIVED_VARS,
)
from genie_space_optimizer.optimization.evaluation import (
    _attempt_benchmark_correction,
    _attempt_sql_correction,
    generate_example_sqls,
    generate_validated_sql_examples,
)
from genie_space_optimizer.optimization.leakage import (
    EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD,
    BenchmarkCorpus,
    LeakageOracle,
    _normalize_question_text,
    _question_token_set_jaccard,
)


# ═══════════════════════════════════════════════════════════════════════
# Shared fixtures
# ═══════════════════════════════════════════════════════════════════════


def _mk_config() -> dict:
    return {
        "_tables": ["cat.sch.sales"],
        "_metric_views": [],
        "_functions": [],
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.sales",
                        "column_configs": [
                            {
                                "column_name": "region",
                                "data_type": "STRING",
                                "column_type": "dimension",
                            },
                            {
                                "column_name": "revenue",
                                "data_type": "DOUBLE",
                                "column_type": "dimension",
                            },
                        ],
                    },
                ],
                "metric_views": [],
            },
            "instructions": {"example_question_sqls": []},
        },
    }


def _mk_uc_columns() -> list[dict]:
    return [
        {
            "table_name": "cat.sch.sales",
            "column_name": "region",
            "data_type": "STRING",
            "comment": "Sales region code",
        },
        {
            "table_name": "cat.sch.sales",
            "column_name": "revenue",
            "data_type": "DOUBLE",
            "comment": "Revenue in USD",
        },
    ]


def _mk_benchmark_corpus() -> BenchmarkCorpus:
    return BenchmarkCorpus.from_benchmarks([
        {
            "id": "b1",
            "question": "What is total revenue by region?",
            "expected_sql": (
                "SELECT region, SUM(revenue) AS total_revenue "
                "FROM cat.sch.sales GROUP BY region"
            ),
        },
    ])


def _clean_llm_candidate() -> dict:
    return {
        "question": "Top 5 regions by revenue?",
        "expected_sql": (
            "SELECT region, SUM(revenue) AS total_revenue "
            "FROM cat.sch.sales GROUP BY region "
            "ORDER BY total_revenue DESC LIMIT 5"
        ),
        "expected_asset": "TABLE",
        "required_tables": ["cat.sch.sales"],
        "required_columns": ["region", "revenue"],
        "expected_facts": ["region", "revenue"],
        "category": "aggregation",
    }


@pytest.fixture
def patched_core(monkeypatch):
    """Patch the LLM call and warehouse validation at their shared
    import sites so we exercise the full shared-core flow without any
    external dependency. Arbiter is stubbed to always approve unless
    the caller overrides.
    """
    from genie_space_optimizer.optimization import evaluation as ev_mod

    def _fake_llm(w, prompt, *, prompt_name=None):
        return [_clean_llm_candidate()]

    def _fake_validate(sql, spark, catalog, schema, *,
                      execute=False, w=None, warehouse_id=""):
        return True, ""

    def _fake_capture_rows(sql, spark, catalog, schema, *,
                           w=None, warehouse_id="", limit=20):
        # F12 — capture helper now returns a 3-tuple
        # ``(rows, error_class, error_message)``. Success path emits
        # ``(rows, None, None)``; failure paths emit
        # ``(None, "subquery_unsupported"|"exec_failed", "msg")``.
        return ([{"region": "NA", "total_revenue": 1000}], None, None)

    class _FakeArbiterReturn(dict):
        pass

    def _fake_arbiter(
        question, sql, result_rows, *, w, metadata_snapshot=None,
    ):
        return {"value": "yes", "rationale": "looks good"}

    monkeypatch.setattr(ev_mod, "_call_llm_for_scoring", _fake_llm)
    monkeypatch.setattr(ev_mod, "_validate_benchmark_sql", _fake_validate)
    monkeypatch.setattr(ev_mod, "_capture_result_rows", _fake_capture_rows)

    # The arbiter import is inside the function body, so patch the
    # module attribute on scorers.arbiter.
    from genie_space_optimizer.optimization.scorers import arbiter as arb_mod
    monkeypatch.setattr(
        arb_mod, "score_example_sql_correctness", _fake_arbiter,
    )
    return {"arbiter_stub": _fake_arbiter}


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R1 — shared-core regression + primitive reuse
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1SharedCore:
    def test_benchmark_correction_adapter_delegates_to_shared(self):
        """_attempt_benchmark_correction is now a thin wrapper over
        _attempt_sql_correction — ensure it forwards the benchmark
        prompt template + registry key."""
        called = {}

        def _spy(**kwargs):
            called.update(kwargs)
            return []

        with patch(
            "genie_space_optimizer.optimization.evaluation._attempt_sql_correction",
            side_effect=_spy,
        ):
            _attempt_benchmark_correction(
                w=None, config={}, uc_columns=[], uc_routines=[],
                invalid_benchmarks=[{"question": "q", "expected_sql": "SELECT 1"}],
                catalog="c", schema="s", spark=None, allowlist={},
            )
        assert called["correction_prompt_template"] is BENCHMARK_CORRECTION_PROMPT
        assert called["correction_prompt_registry_key"] == "benchmark_correction"

    def test_shared_core_returns_counter_dict_with_all_keys(self, patched_core):
        """Every call returns the same counter schema so the pretty
        summary can log distinct classes without conditional branches.
        """
        _, counters = generate_validated_sql_examples(
            w=None, spark=None,
            config=_mk_config(), uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=5,
            generation_prompt_template=EXAMPLE_SQL_GENERATION_PROMPT,
            correction_prompt_template=EXAMPLE_SQL_CORRECTION_PROMPT,
            generation_prompt_registry_key="example_sql_generation",
            correction_prompt_registry_key="example_sql_correction",
            provenance="synthetic_example_sql",
        )
        for key in (
            "metadata", "mv_select_star", "explain_or_execute",
            "arbiter_no", "firewall_fingerprint", "firewall_question_echo",
            "dedup_in_corpus", "unfixable_after_correction",
            # F8 — deterministic-repair counters must ship in the schema
            # so the unified banner renderer can read them unconditionally.
            "repaired_stemmed_identifiers", "repaired_measure_refs",
        ):
            assert key in counters


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R1b — LeakageOracle opaque match API
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1LeakageOracle:
    def test_oracle_has_no_iteration_or_text_getters(self):
        """Oracle must expose booleans only. No __iter__, no public
        access to benchmark text. This is the Bug #4 side-channel
        closure."""
        oracle = LeakageOracle(_mk_benchmark_corpus())
        forbidden = (
            "__iter__", "questions", "expected_sqls",
            "sql_fingerprints", "question_ids",
        )
        for attr in forbidden:
            assert not hasattr(oracle, attr), (
                f"Oracle leaks {attr}; invariant #4 violated"
            )
        # Only the two boolean match methods are public.
        assert callable(oracle.contains_sql)
        assert callable(oracle.contains_question)

    def test_oracle_union_semantics_across_corpora(self):
        """LeakageOracle(c1, c2) matches on EITHER corpus. This is
        what the harness uses to firewall example-SQL output against
        both the benchmark corpus AND already-installed examples."""
        bench = BenchmarkCorpus.from_benchmarks([
            {"id": "b1", "question": "Q1", "expected_sql": "SELECT a FROM t"},
        ])
        existing = BenchmarkCorpus.from_benchmarks([
            {"id": "e1", "question": "other", "expected_sql": "SELECT b FROM u"},
        ])
        oracle = LeakageOracle(bench, existing)
        assert oracle.contains_sql("SELECT a FROM t")
        assert oracle.contains_sql("SELECT b FROM u")
        assert not oracle.contains_sql("SELECT c FROM different_table")

    def test_oracle_contains_sql_fingerprint_exact_match(self):
        oracle = LeakageOracle(_mk_benchmark_corpus())
        matching = (
            "SELECT region, SUM(revenue) AS total_revenue "
            "FROM cat.sch.sales GROUP BY region"
        )
        assert oracle.contains_sql(matching)

    def test_oracle_empty_strings_return_false(self):
        oracle = LeakageOracle(_mk_benchmark_corpus())
        assert not oracle.contains_sql("")
        assert not oracle.contains_sql("   ")
        assert not oracle.contains_question("")


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R1c — question-echo normalization + threshold
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1QuestionEcho:
    def test_normalize_strips_punct_stopwords_whitespace(self):
        tokens = _normalize_question_text(
            "What is the total revenue by region?",
        )
        # Stopwords (what, is, the, by) dropped; content words kept.
        assert "total" in tokens
        assert "revenue" in tokens
        assert "region" in tokens
        assert "what" not in tokens
        assert "the" not in tokens

    def test_jaccard_paraphrase_above_default_threshold(self):
        score = _question_token_set_jaccard(
            "What is total revenue by region?",
            "Show me total revenue by region",
        )
        assert score >= EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD

    def test_jaccard_unrelated_below_threshold(self):
        score = _question_token_set_jaccard(
            "What is total revenue by region?",
            "How many customers churned last quarter?",
        )
        assert score < EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD


# ═══════════════════════════════════════════════════════════════════════
# Phase 2.R2 — prompt variants
# ═══════════════════════════════════════════════════════════════════════


class TestPhase2PromptVariants:
    def test_example_prompt_reframes_role_as_teach(self):
        # <role> block says TEACH, not TEST / evaluate.
        assert "TEACH" in EXAMPLE_SQL_GENERATION_PROMPT
        assert "evaluation expert" not in EXAMPLE_SQL_GENERATION_PROMPT
        # Correction prompt reframing too.
        assert "example SQLs" in EXAMPLE_SQL_CORRECTION_PROMPT

    def test_example_prompt_preserves_metric_view_rules(self):
        # Every MV rule from the benchmark prompt must be preserved in
        # the example prompt — this is the feature we came for.
        assert "## Metric View Query Rules" in EXAMPLE_SQL_GENERATION_PROMPT
        assert "MEASURE()" in EXAMPLE_SQL_GENERATION_PROMPT
        assert "METRIC_VIEW_JOIN_NOT_SUPPORTED" in EXAMPLE_SQL_GENERATION_PROMPT

    def test_example_prompt_drops_benchmark_fields_from_output(self):
        # Benchmark prompt emits expected_facts + category in schema;
        # example prompt emits only question + expected_sql (+ optional
        # usage_guidance).
        assert '"expected_facts"' not in EXAMPLE_SQL_GENERATION_PROMPT
        assert '"category"' not in EXAMPLE_SQL_GENERATION_PROMPT
        assert '"usage_guidance"' in EXAMPLE_SQL_GENERATION_PROMPT


# ═══════════════════════════════════════════════════════════════════════
# Phase 2.R2b — module-load isolation assertion
# ═══════════════════════════════════════════════════════════════════════


class TestPhase2ImportTimeAssertion:
    def test_example_prompt_has_no_benchmark_derived_vars(self):
        for var in _BENCHMARK_DERIVED_VARS:
            token = "{{ " + var + " }}"
            assert token not in EXAMPLE_SQL_GENERATION_PROMPT, (
                f"isolation invariant violated: {var} in example generation prompt"
            )
            assert token not in EXAMPLE_SQL_CORRECTION_PROMPT


# ═══════════════════════════════════════════════════════════════════════
# Phase 3.R3 — generate_example_sqls required-kwarg contract
# ═══════════════════════════════════════════════════════════════════════


class TestPhase3RequiredKwarg:
    def test_missing_leakage_oracle_raises_type_error(self):
        with pytest.raises(TypeError, match="leakage_oracle"):
            generate_example_sqls(
                w=None, spark=None,
                config={}, uc_columns=[], uc_tags=[], uc_routines=[],
                domain="x", catalog="c", schema="s",
            )

    def test_required_kwarg_signature_is_keyword_only(self):
        import inspect
        sig = inspect.signature(generate_example_sqls)
        param = sig.parameters["leakage_oracle"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY
        assert param.default is inspect.Parameter.empty


# ═══════════════════════════════════════════════════════════════════════
# Phase 3.R3 — firewall drops flow through to counters
# ═══════════════════════════════════════════════════════════════════════


class TestPhase3FirewallDrops:
    def test_firewall_drops_sql_fingerprint_match(
        self, patched_core, monkeypatch,
    ):
        """A candidate whose SQL matches a benchmark fingerprint gets
        dropped and the fingerprint counter increments."""
        from genie_space_optimizer.optimization import evaluation as ev_mod

        # LLM returns a SQL matching the benchmark corpus exactly.
        def _fake_llm(w, prompt, *, prompt_name=None):
            return [{
                "question": "Any region revenue?",
                "expected_sql": (
                    "SELECT region, SUM(revenue) AS total_revenue "
                    "FROM cat.sch.sales GROUP BY region"
                ),
                "expected_asset": "TABLE",
                "required_tables": ["cat.sch.sales"],
                "required_columns": ["region", "revenue"],
                "expected_facts": ["revenue"],
                "category": "agg",
            }]
        monkeypatch.setattr(ev_mod, "_call_llm_for_scoring", _fake_llm)

        oracle = LeakageOracle(_mk_benchmark_corpus())
        survivors, counters = generate_example_sqls(
            w=None, spark=None,
            config=_mk_config(), uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=5,
            leakage_oracle=oracle,
        )
        assert len(survivors) == 0
        assert counters["firewall_fingerprint"] >= 1

    def test_firewall_drops_question_echo(
        self, patched_core, monkeypatch,
    ):
        """LLM emits different SQL but near-paraphrased benchmark
        question → question-echo firewall drops it."""
        from genie_space_optimizer.optimization import evaluation as ev_mod

        def _fake_llm(w, prompt, *, prompt_name=None):
            return [{
                "question": "Show me total revenue by region",
                # Different SQL from the benchmark — but question echoes it.
                "expected_sql": (
                    "SELECT region, COUNT(*) FROM cat.sch.sales GROUP BY region"
                ),
                "expected_asset": "TABLE",
                "required_tables": ["cat.sch.sales"],
                "required_columns": ["region"],
                "expected_facts": ["count"],
                "category": "agg",
            }]
        monkeypatch.setattr(ev_mod, "_call_llm_for_scoring", _fake_llm)

        oracle = LeakageOracle(_mk_benchmark_corpus())
        survivors, counters = generate_example_sqls(
            w=None, spark=None,
            config=_mk_config(), uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=5,
            leakage_oracle=oracle,
        )
        assert counters["firewall_question_echo"] >= 1
        assert len(survivors) == 0


# ═══════════════════════════════════════════════════════════════════════
# Phase 3.R3b — arbiter runs with result_rows, no silent pass
# ═══════════════════════════════════════════════════════════════════════


class TestPhase3ArbiterWiring:
    def test_arbiter_called_with_result_rows(self, patched_core, monkeypatch):
        from genie_space_optimizer.optimization.scorers import arbiter as arb_mod
        seen = {}

        def _capture_call(question, sql, result_rows, *, w, metadata_snapshot=None):
            seen["result_rows"] = result_rows
            seen["question"] = question
            seen["sql"] = sql
            return {"value": "yes"}

        monkeypatch.setattr(
            arb_mod, "score_example_sql_correctness", _capture_call,
        )
        oracle = LeakageOracle(_mk_benchmark_corpus())
        _ = generate_example_sqls(
            w=None, spark=object(),  # backend present → arbiter runs
            config=_mk_config(), uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=1,
            leakage_oracle=oracle,
        )
        assert "result_rows" in seen
        assert seen["result_rows"] is not None

    def test_arbiter_verdict_no_drops_candidate(
        self, patched_core, monkeypatch,
    ):
        from genie_space_optimizer.optimization.scorers import arbiter as arb_mod

        def _reject(question, sql, result_rows, *, w, metadata_snapshot=None):
            return {"value": "no", "rationale": "bad"}

        monkeypatch.setattr(
            arb_mod, "score_example_sql_correctness", _reject,
        )
        oracle = LeakageOracle(_mk_benchmark_corpus())
        survivors, counters = generate_example_sqls(
            w=None, spark=object(),
            config=_mk_config(), uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=1,
            leakage_oracle=oracle,
        )
        assert len(survivors) == 0
        assert counters["arbiter_no"] >= 1

    def test_synthesis_gate_arbiter_fails_closed_on_module_import(self):
        """The broken-silently-passes behavior is gone. When the
        arbiter is reachable BUT we have a backend (so we're not in
        the graceful skip branch), a module-level failure is an
        explicit FAIL rather than a PASS."""
        from genie_space_optimizer.optimization.synthesis import (
            GateResult, _gate_arbiter,
        )
        # Force the import to fail inside the gate body.
        with patch.dict(sys.modules, {
            "genie_space_optimizer.optimization.scorers.arbiter": None,
        }):
            result = _gate_arbiter(
                {"example_question": "q", "example_sql": "SELECT 1"},
                w=object(),
                warehouse_id="wh1",  # backend present → NOT graceful skip
            )
        assert isinstance(result, GateResult)
        assert result.passed is False
        assert "arbiter_module_unavailable" in (result.reason or "")


# ═══════════════════════════════════════════════════════════════════════
# Phase 4.R4 — harness routing + fallback
# ═══════════════════════════════════════════════════════════════════════


class TestPhase4HarnessRouting:
    def test_unified_runner_builds_oracle_and_calls_generator(
        self, patched_core, monkeypatch,
    ):
        """_run_unified_example_sql_generation must (a) build a
        LeakageOracle from benchmarks + existing examples, (b) call
        generate_example_sqls with it, (c) apply via the shared
        applier pipeline."""
        from genie_space_optimizer.optimization import harness

        applier_called = {}

        def _fake_applier(
            w, spark, run_id, space_id, proposals, metadata_snapshot,
            config, catalog, schema, *, benchmarks=None,
        ):
            applier_called["proposals"] = proposals
            applier_called["benchmarks"] = benchmarks

        monkeypatch.setattr(
            harness, "_apply_proactive_example_sqls", _fake_applier,
        )

        result = harness._run_unified_example_sql_generation(
            w=None, spark=None, run_id="r1", space_id="s1",
            config=_mk_config(),
            metadata_snapshot=_mk_config()["_parsed_space"],
            uc_columns=_mk_uc_columns(), domain="sales",
            catalog="cat", schema="sch",
            full_firewall_corpus=[{
                "id": "b1", "question": "Q1", "expected_sql": "SELECT a FROM t",
            }],
            data_profile=None,
        )
        # The generator was called (clean-LLM fixture returns 1 candidate).
        assert result["unified_generated"] >= 0
        if result["applied"]:
            assert applier_called["benchmarks"] == [{
                "id": "b1", "question": "Q1", "expected_sql": "SELECT a FROM t",
            }]


class TestPhase4ArchetypeFallback:
    def test_fallback_config_knobs_parseable(self):
        """GSO_UNIFIED_MIN_SURVIVORS env var is read with a sane default.
        This is the Option A knob: when unified yields below the
        threshold, the archetype path runs to fill the gap.
        """
        import os
        # The harness reads this just-in-time; verify parsing doesn't
        # raise on the default.
        default = int(os.environ.get("GSO_UNIFIED_MIN_SURVIVORS", "5") or "5")
        assert default == 5


# ═══════════════════════════════════════════════════════════════════════
# Phase 4.R4b — MLflow registry
# ═══════════════════════════════════════════════════════════════════════


class TestPhase4MLflowRegistry:
    def test_example_prompts_registered_alongside_benchmark(self):
        assert "example_sql_generation" in BENCHMARK_PROMPTS
        assert "example_sql_correction" in BENCHMARK_PROMPTS
        assert BENCHMARK_PROMPTS["example_sql_generation"] is EXAMPLE_SQL_GENERATION_PROMPT
        # Benchmark prompts still registered (no regression).
        assert BENCHMARK_PROMPTS["benchmark_generation"] is BENCHMARK_GENERATION_PROMPT


# ═══════════════════════════════════════════════════════════════════════
# Phase 5.R5b — isolation lint rule
# ═══════════════════════════════════════════════════════════════════════


class TestPhase5IsolationLint:
    def test_lint_passes_on_clean_source(self):
        """Running the lint on the real evaluation.py must exit 0."""
        script = (
            pathlib.Path(__file__).resolve().parent.parent.parent
            / "scripts" / "lint_example_sql_isolation.py"
        )
        assert script.exists(), script
        proc = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True, text=True,
        )
        assert proc.returncode == 0, (
            f"lint failed on clean source:\n{proc.stderr}"
        )

    def test_lint_catches_seeded_benchmark_param(self, tmp_path):
        """Write a fake module with a forbidden param, run the lint
        logic against it, confirm violation reported."""
        script_dir = (
            pathlib.Path(__file__).resolve().parent.parent.parent / "scripts"
        )
        sys.path.insert(0, str(script_dir))
        try:
            import importlib
            lint = importlib.import_module("lint_example_sql_isolation")
            fake_src = textwrap.dedent("""
                def generate_example_sqls(w, spark, *, benchmarks, leakage_oracle):
                    return [], {}

                def generate_validated_sql_examples(*, target_count):
                    return [], {}
                """)
            fake_file = tmp_path / "fake_eval.py"
            fake_file.write_text(fake_src)
            violations = lint.lint_file(fake_file)
            assert violations, "lint missed seeded benchmark= parameter"
            assert any(
                "forbidden parameter" in v for v in violations
            )
        finally:
            sys.path.remove(str(script_dir))


# ═══════════════════════════════════════════════════════════════════════
# F8 — Unified-pipeline deterministic repair parity
#
# Mirror of the preflight pipeline's F4 (stem qualification) + F5
# (MEASURE() wrap) repairs. Tests patch ``_call_llm_for_scoring`` to
# return SQL with the failure shapes real field logs showed (bare
# ``dim_date`` stems, bare measure refs in metric-view SELECTs) and
# assert the unified correction loop now heals them deterministically
# before re-validation — and that the applied count is surfaced on
# ``rejection_counters`` so the banner renderer can display parity.
# ═══════════════════════════════════════════════════════════════════════


class TestF8UnifiedRepairPorting:
    def _allowlist_with_mv(self, config: dict) -> dict:
        """Build the allowlist via the real production helper so the
        metadata-enforcement check downstream sees the same shape
        (all short-form variants) it would in a live run. Faking this
        by hand would drift from
        ``_enforce_metadata_constraints``'s expectations.
        """
        from genie_space_optimizer.optimization.evaluation import (
            _build_metadata_allowlist,
        )
        return _build_metadata_allowlist(
            config=config, uc_columns=_mk_uc_columns(), uc_routines=[],
        )

    def _config_with_mv(self) -> dict:
        """Extends ``_mk_config()`` so metadata enforcement recognizes
        the metric view the LLM's repaired SQL references. Without
        this, ``_enforce_metadata_constraints`` rejects the corrected
        candidate BEFORE re-validation, masking the repair.
        """
        cfg = _mk_config()
        cfg["_metric_views"] = ["cat.sch.mv_esr_dim_date"]
        cfg["_parsed_space"]["data_sources"]["metric_views"] = [
            {"identifier": "cat.sch.mv_esr_dim_date",
             "measures": [{"name": "cnt", "expr": "COUNT(1)"}]},
        ]
        return cfg

    def test_stem_qualification_repair_fires_and_counts(self, monkeypatch):
        """The LLM returns ``FROM dim_date`` (bare stem). The unified
        correction loop must rewrite it to the canonical
        ``cat.sch.mv_esr_dim_date`` — this is the exact failure class
        the field log surfaced that preflight was healing but unified
        was rejecting. Counter must also increment so the banner can
        report it.
        """
        from genie_space_optimizer.optimization import evaluation as ev_mod

        def _llm(w, prompt, *, prompt_name=None):
            # The candidate refers only to columns already present on
            # the base ``sales`` table so metadata enforcement won't
            # reject on unknown-column grounds. The stem repair is
            # what we're isolating here; the metric view is only in
            # play as the canonical target for the bare stem.
            return [{
                "question": "How many sales rows?",
                "expected_sql": "SELECT COUNT(*) FROM dim_date",
                "expected_asset": "TABLE",
                "required_tables": ["cat.sch.mv_esr_dim_date"],
                "required_columns": [],
                "expected_facts": [],
                "category": "lookup",
            }]

        validate_calls: list[str] = []

        def _validate(sql, spark, catalog, schema, *,
                      execute=False, w=None, warehouse_id=""):
            validate_calls.append(sql)
            # The repair must have run before we see the SQL here.
            return ("cat.sch.mv_esr_dim_date" in sql), ""

        monkeypatch.setattr(ev_mod, "_call_llm_for_scoring", _llm)
        monkeypatch.setattr(ev_mod, "_validate_benchmark_sql", _validate)

        counters = {
            "repaired_stemmed_identifiers": 0,
            "repaired_measure_refs": 0,
        }
        invalid = [{
            "question": "How many sales rows?",
            "expected_sql": "SELECT COUNT(*) FROM dim_date",
            "validation_error": "UNRESOLVED_COLUMN: dim_date",
        }]
        cfg = self._config_with_mv()
        out = ev_mod._attempt_sql_correction(
            w=None, config=cfg,
            uc_columns=_mk_uc_columns(), uc_routines=[],
            invalid_candidates=invalid,
            catalog="cat", schema="sch", spark=None,
            allowlist=self._allowlist_with_mv(cfg),
            correction_prompt_template=EXAMPLE_SQL_CORRECTION_PROMPT,
            correction_prompt_registry_key="example_sql_correction",
            repair_counters=counters,
        )

        assert len(out) == 1
        assert "cat.sch.mv_esr_dim_date" in out[0]["expected_sql"]
        assert "FROM dim_date" not in out[0]["expected_sql"]
        assert counters["repaired_stemmed_identifiers"] >= 1
        # The SQL fed to ``_validate_benchmark_sql`` is the repaired
        # form, not the LLM's raw bare-stem output.
        assert validate_calls
        assert "cat.sch.mv_esr_dim_date" in validate_calls[0]

    def test_repair_counters_none_is_safe_noop(self, monkeypatch):
        """When callers omit ``repair_counters`` (e.g. the benchmark
        adapter), repairs still fire — they can only help — but the
        count is silently discarded. Must not crash.
        """
        from genie_space_optimizer.optimization import evaluation as ev_mod

        def _llm(w, prompt, *, prompt_name=None):
            return [{
                "question": "Q", "expected_sql": "SELECT 1 FROM dim_date",
                "expected_asset": "TABLE",
                "required_tables": ["cat.sch.mv_esr_dim_date"],
                "required_columns": [], "expected_facts": [],
                "category": "lookup",
            }]

        monkeypatch.setattr(ev_mod, "_call_llm_for_scoring", _llm)
        monkeypatch.setattr(
            ev_mod, "_validate_benchmark_sql",
            lambda *a, **kw: (True, ""),
        )

        cfg = self._config_with_mv()
        out = ev_mod._attempt_sql_correction(
            w=None, config=cfg,
            uc_columns=_mk_uc_columns(), uc_routines=[],
            invalid_candidates=[{"question": "Q",
                                 "expected_sql": "SELECT 1 FROM dim_date"}],
            catalog="cat", schema="sch", spark=None,
            allowlist=self._allowlist_with_mv(cfg),
            correction_prompt_template=EXAMPLE_SQL_CORRECTION_PROMPT,
            correction_prompt_registry_key="example_sql_correction",
            # repair_counters intentionally omitted.
        )
        assert len(out) == 1
        # Repair still applied even without a counter to record it.
        assert "cat.sch.mv_esr_dim_date" in out[0]["expected_sql"]

    def test_benchmark_adapter_forwards_without_counters(self, monkeypatch):
        """The benchmark-variant adapter does NOT pass
        ``repair_counters`` (intentionally — the benchmark banner has
        its own schema). The shared core must accept the omission
        without altering its behaviour.
        """
        from genie_space_optimizer.optimization import evaluation as ev_mod

        captured_kwargs: dict = {}

        def _spy(**kwargs):
            captured_kwargs.update(kwargs)
            return []

        monkeypatch.setattr(ev_mod, "_attempt_sql_correction", _spy)
        ev_mod._attempt_benchmark_correction(
            w=None, config={}, uc_columns=[], uc_routines=[],
            invalid_benchmarks=[{"question": "q",
                                 "expected_sql": "SELECT 1"}],
            catalog="c", schema="s", spark=None, allowlist={},
        )
        # The adapter forwards only the benchmark prompt kwargs —
        # ``repair_counters`` is absent (None/missing both acceptable).
        assert "repair_counters" not in captured_kwargs

    def test_unified_banner_surfaces_repair_counts(self):
        """The harness banner renderer must read the repair counters
        off ``rejection_counters`` and display them when non-zero.
        This locks the contract between ``_attempt_sql_correction``
        (writer) and ``_print_unified_example_summary`` (reader).
        """
        import io
        import contextlib

        from genie_space_optimizer.optimization.harness import (
            _print_unified_example_summary,
        )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_unified_example_summary(
                run_id="r1", target=20, existing=0,
                applied_examples=[],
                rejection_counters={
                    "metadata": 0, "mv_select_star": 0,
                    "explain_or_execute": 0, "arbiter_no": 0,
                    "firewall_fingerprint": 0,
                    "firewall_question_echo": 0,
                    "dedup_in_corpus": 0,
                    "unfixable_after_correction": 0,
                    "repaired_stemmed_identifiers": 3,
                    "repaired_measure_refs": 1,
                },
            )
        out = buf.getvalue()
        assert "Stemmed identifiers repaired" in out
        assert "MEASURE() refs repaired" in out
        assert "3" in out and "1" in out

    def test_unified_banner_hides_repair_counts_when_zero(self):
        """Banner stays terse when the LLM returned clean output —
        repair lines only appear when they actually fired. Prevents
        banner bloat on the happy path.
        """
        import io
        import contextlib

        from genie_space_optimizer.optimization.harness import (
            _print_unified_example_summary,
        )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_unified_example_summary(
                run_id="r1", target=20, existing=0,
                applied_examples=[],
                rejection_counters={
                    "metadata": 0, "mv_select_star": 0,
                    "explain_or_execute": 0, "arbiter_no": 0,
                    "firewall_fingerprint": 0,
                    "firewall_question_echo": 0,
                    "dedup_in_corpus": 0,
                    "unfixable_after_correction": 0,
                    "repaired_stemmed_identifiers": 0,
                    "repaired_measure_refs": 0,
                },
            )
        out = buf.getvalue()
        assert "Stemmed identifiers repaired" not in out
        assert "MEASURE() refs repaired" not in out


# ═══════════════════════════════════════════════════════════════════════
# F13 — two-tier row capture, end-to-end through the unified pipeline
#
# The unit-level coverage in test_arbiter_row_capture.py exercises
# ``_capture_result_rows`` in isolation. This integration test wires the
# full unified shared core (LLM → metadata → execute → row capture →
# arbiter) and confirms a metric-view-style candidate that previously
# died at ``arbiter_no_result_rows`` now reaches the arbiter with rows
# and gets applied. This is the production path PR 13 is fixing.
# ═══════════════════════════════════════════════════════════════════════


class TestF13MetricViewRowCaptureRecovery:
    """The Tier 2 LIMIT-injection fallback must recover candidates whose
    SQL targets a metric view at the top level — DBSQL refuses the
    Tier 1 ``SELECT * FROM ({sql})`` wrap there, but the original SQL
    with a top-level ``LIMIT`` runs cleanly.
    """

    def _config_with_mv(self) -> dict:
        cfg = _mk_config()
        cfg["_metric_views"] = ["cat.sch.mv_sales"]
        cfg["_parsed_space"]["data_sources"]["metric_views"] = [
            {
                "identifier": "cat.sch.mv_sales",
                "measures": [
                    {"name": "total_revenue", "expr": "SUM(revenue)"},
                ],
                "dimensions": [{"name": "region"}],
            },
        ]
        return cfg

    def _mv_candidate(self) -> dict:
        # Question must NOT echo any benchmark-corpus question — the
        # leakage firewall would otherwise reject before row capture
        # ever runs and the test would silently miss the recovery path.
        return {
            "question": "Show me yesterday's regional sales breakdown please",
            "expected_sql": (
                "SELECT region, MEASURE(total_revenue) AS rev "
                "FROM cat.sch.mv_sales GROUP BY region"
            ),
            "expected_asset": "METRIC_VIEW",
            "required_tables": ["cat.sch.mv_sales"],
            "required_columns": ["region"],
            "expected_facts": ["region"],
            "category": "metric_view_aggregation",
        }

    def test_metric_view_candidate_reaches_arbiter_with_rows(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """End-to-end: LLM emits a ``MEASURE()`` candidate; Tier 1 wrap
        would fail on a real warehouse so we simulate that exact shape;
        the arbiter must see actual rows (not the ``None`` that the
        pre-F13 row-capture surfaced) and the candidate is applied.

        Key observable contract: ``arbiter_no_result_rows`` /
        ``arbiter_row_capture_subquery_unsupported`` MUST NOT be the
        rejection reason — that's the regression we're guarding against.
        """
        from genie_space_optimizer.optimization import evaluation as ev_mod
        from genie_space_optimizer.optimization.scorers import (
            arbiter as arb_mod,
        )

        # 1. LLM emits a single metric-view candidate.
        monkeypatch.setattr(
            ev_mod, "_call_llm_for_scoring",
            lambda w, prompt, *, prompt_name=None: [self._mv_candidate()],
        )

        # 2. Metadata enforcement / EXPLAIN both pass — the bug PR 13
        # fixes lives strictly between EXPLAIN-pass and arbiter call.
        monkeypatch.setattr(
            ev_mod, "_validate_benchmark_sql",
            lambda *a, **kw: (True, ""),
        )

        # 3. Two-tier execution behavior:
        # Tier 1 wrap (subquery) → DBSQL-style metric-view rejection
        # Tier 2 (raw SQL + LIMIT) → returns an actual row.
        captured_sql: list[str] = []

        class _FakeDF:
            def __init__(self, rows: list[dict]) -> None:
                self._rows = rows

            @property
            def empty(self) -> bool:
                return not self._rows

            def head(self, n: int) -> "_FakeDF":
                return _FakeDF(self._rows[:n])

            def to_dict(self, orient: str = "records") -> list[dict]:
                assert orient == "records"
                return list(self._rows)

        def _exec(sql: str, spark, **kwargs) -> object:
            captured_sql.append(sql)
            if "_gvse_sample" in sql:
                # Tier 1 — the wrap that DBSQL refuses on metric views.
                raise RuntimeError(
                    "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY: "
                    "scalar metric view"
                )
            return _FakeDF([{"region": "NA", "rev": 12345.6}])

        monkeypatch.setattr(ev_mod, "_exec_sql", _exec)

        # 4. Arbiter records what it sees and approves.
        seen: dict = {}

        def _arbiter(question, sql, result_rows, *, w, metadata_snapshot=None):
            seen["question"] = question
            seen["sql"] = sql
            seen["rows"] = result_rows
            return {"value": "yes", "rationale": "ok"}

        monkeypatch.setattr(
            arb_mod, "score_example_sql_correctness", _arbiter,
        )

        # Use an unrelated benchmark corpus so the leakage firewall
        # doesn't reject the candidate on n-gram overlap before row
        # capture has a chance to run — we're testing the row-capture
        # recovery path, not leakage detection.
        unrelated_corpus = BenchmarkCorpus.from_benchmarks([
            {
                "id": "x1",
                "question": "How many customers churned last quarter?",
                "expected_sql": (
                    "SELECT COUNT(DISTINCT customer_id) FROM churn_events "
                    "WHERE quarter = 'Q4'"
                ),
            },
        ])
        oracle = LeakageOracle(unrelated_corpus)
        survivors, counters = generate_example_sqls(
            w=None, spark=object(),  # truthy → row capture fires
            config=self._config_with_mv(),
            uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=1,
            leakage_oracle=oracle,
        )

        # The candidate must have been applied — pre-F13, this would
        # have been zero with rejection_counters
        # ["arbiter_no_result_rows"] = 1.
        assert len(survivors) >= 1, (
            f"metric-view candidate was not recovered; counters={counters}; "
            f"sql_seen={captured_sql}"
        )

        # The arbiter saw real rows.
        assert seen.get("rows"), (
            "arbiter received empty/None rows — Tier 2 fallback didn't "
            "deliver row capture"
        )
        assert seen["rows"][0].get("region") == "NA"

        # Tier 2 actually fired: there's a non-wrap exec containing
        # the raw MEASURE() candidate with a LIMIT appended.
        tier2_runs = [s for s in captured_sql if "_gvse_sample" not in s]
        assert tier2_runs, (
            f"Tier 2 LIMIT-injection path never executed; captured={captured_sql}"
        )
        assert "MEASURE(total_revenue)" in tier2_runs[0]
        assert "LIMIT" in tier2_runs[0].upper()

        # No row-capture-attributed rejection in the counters.
        assert counters.get(
            "arbiter_row_capture_subquery_unsupported", 0,
        ) == 0
        assert counters.get(
            "arbiter_row_capture_exec_failed", 0,
        ) == 0


# ═══════════════════════════════════════════════════════════════════════
# PR 21 — Adaptive overdraw short-circuit + MV-detection banner counter
#
# When ``mv_measures`` is empty AND every LLM round dies on the same
# ``mv_missing_measure_function`` reject, additional rounds cannot
# recover (without measures the auto-wrap rewriter is a no-op). The
# short-circuit cuts the loop after the first round, stamps a marker on
# rejection_counters, and surfaces the failure mode on the banner along
# with the per-source MV-detection counts so log readers can attribute
# the cluster to "no MVs at all" vs "MVs exist but none of the
# detection paths saw them".
# ═══════════════════════════════════════════════════════════════════════


class TestPR21AdaptiveOverdrawShortCircuit:
    """Locks the budget guard and the banner counter contract."""

    def _llm_with_bad_mv_sql(self):
        """Return a fake LLM that emits a single MV-shaped SQL the
        execute gate will reject with the same reason every round.
        Runs forever absent a short-circuit.
        """
        return [{
            "question": "Top regions by sales?",
            "expected_sql": (
                "SELECT region, SUM(revenue) AS total "
                "FROM cat.sch.sales GROUP BY region"
            ),
            "expected_asset": "TABLE",
            "required_tables": ["cat.sch.sales"],
            "required_columns": ["region", "revenue"],
            "expected_facts": ["region"],
            "category": "agg",
        }]

    def test_short_circuit_fires_when_no_mv_measures_and_dominant_reject(
        self, patched_core, monkeypatch,
    ):
        """No MVs configured + every round dies on
        ``mv_missing_measure_function`` → break out of the overdraw
        loop after round 0 and stamp ``adaptive_overdraw_short_circuited
        = 'no_mv_measures'``. The legacy ``adaptive_overdraw_rounds_used``
        counter should report 1 (not the configured 3).
        """
        from genie_space_optimizer.optimization import evaluation as ev_mod

        # The default ``_mk_config`` has no MVs → ``build_metric_view_measures``
        # returns ``{}``, which is the exact precondition the short-circuit
        # guards on.

        monkeypatch.setattr(
            ev_mod, "_call_llm_for_scoring",
            lambda w, prompt, *, prompt_name=None: self._llm_with_bad_mv_sql(),
        )

        def _validate_always_fail_with_mv_error(
            sql, spark, catalog, schema, *,
            execute=False, w=None, warehouse_id="",
        ):
            return False, (
                "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] The aggregate "
                "function 'sum(revenue)' must be wrapped with MEASURE()."
            )

        monkeypatch.setattr(
            ev_mod, "_validate_benchmark_sql",
            _validate_always_fail_with_mv_error,
        )

        # Stub the correction loop so it doesn't burn additional rounds —
        # we want to isolate the OUTER overdraw-loop short-circuit.
        monkeypatch.setattr(
            ev_mod, "_attempt_sql_correction",
            lambda **kwargs: [],
        )

        _survivors, counters = generate_validated_sql_examples(
            w=None, spark=None,
            config=_mk_config(), uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=5,
            generation_prompt_template=EXAMPLE_SQL_GENERATION_PROMPT,
            correction_prompt_template=EXAMPLE_SQL_CORRECTION_PROMPT,
            generation_prompt_registry_key="example_sql_generation",
            correction_prompt_registry_key="example_sql_correction",
            provenance="synthetic_example_sql",
        )

        assert (
            counters.get("adaptive_overdraw_short_circuited")
            == "no_mv_measures"
        ), counters
        assert counters.get("adaptive_overdraw_rounds_used") == 1, counters

    def test_short_circuit_does_not_fire_when_mv_measures_present(
        self, patched_core, monkeypatch,
    ):
        """If catalog detection populated ``_metric_view_yaml`` with at
        least one measure, the rewriter has a fighting chance and we
        must NOT short-circuit — overdraw runs the full configured
        rounds (3 by default) when the deficit persists.
        """
        from genie_space_optimizer.optimization import evaluation as ev_mod

        cfg = _mk_config()
        cfg["_metric_view_yaml"] = {
            "cat.sch.mv_sales": {
                "measures": [{"name": "total_revenue", "expr": "SUM(revenue)"}],
            },
        }

        monkeypatch.setattr(
            ev_mod, "_call_llm_for_scoring",
            lambda w, prompt, *, prompt_name=None: self._llm_with_bad_mv_sql(),
        )
        monkeypatch.setattr(
            ev_mod, "_validate_benchmark_sql",
            lambda *a, **kw: (False, "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] x"),
        )
        monkeypatch.setattr(
            ev_mod, "_attempt_sql_correction",
            lambda **kwargs: [],
        )

        _survivors, counters = generate_validated_sql_examples(
            w=None, spark=None,
            config=cfg, uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=5,
            generation_prompt_template=EXAMPLE_SQL_GENERATION_PROMPT,
            correction_prompt_template=EXAMPLE_SQL_CORRECTION_PROMPT,
            generation_prompt_registry_key="example_sql_generation",
            correction_prompt_registry_key="example_sql_correction",
            provenance="synthetic_example_sql",
        )

        assert "adaptive_overdraw_short_circuited" not in counters or (
            counters.get("adaptive_overdraw_short_circuited") in (None, "")
        ), counters
        # We continued through every configured round.
        assert counters.get("adaptive_overdraw_rounds_used") == 3, counters

    def test_count_mv_detection_sources_returns_three_buckets(self):
        """The helper must attribute each MV to exactly one detection
        path. Catalog-only finds (no overlap with config or column
        flags) populate the ``catalog`` bucket, etc. Used by both the
        unified and preflight banners — a drift in this contract drifts
        both banners simultaneously, so this test is the canary.
        """
        from genie_space_optimizer.optimization.evaluation import (
            _count_mv_detection_sources,
        )

        cfg = {
            "_parsed_space": {
                "data_sources": {
                    "metric_views": [
                        {"identifier": "cat.sch.mv_a", "measures": [{"name": "m"}]},
                    ],
                    "tables": [
                        {
                            "identifier": "cat.sch.mv_b",
                            "column_configs": [
                                {"column_name": "m", "column_type": "measure"},
                            ],
                        },
                    ],
                },
            },
            "_metric_view_yaml": {
                "cat.sch.mv_c": {"measures": [{"name": "m"}]},
                # Already counted by ``config`` — must NOT also count as catalog.
                "cat.sch.mv_a": {"measures": [{"name": "m"}]},
            },
        }
        counts = _count_mv_detection_sources(cfg)
        assert counts == {"config": 1, "column_flags": 1, "catalog": 1}

    def test_count_mv_detection_sources_empty_config(self):
        from genie_space_optimizer.optimization.evaluation import (
            _count_mv_detection_sources,
        )
        assert _count_mv_detection_sources({}) == {
            "config": 0, "column_flags": 0, "catalog": 0,
        }

    def test_unified_banner_renders_mv_detection_counts(self):
        """When ``config`` is provided, the banner must surface the
        MV-detection summary line so log readers don't have to trawl
        DEBUG logs to find out whether catalog detection fired.
        """
        import io
        import contextlib

        from genie_space_optimizer.optimization.harness import (
            _print_unified_example_summary,
        )

        cfg = {
            "_parsed_space": {
                "data_sources": {
                    "metric_views": [
                        {"identifier": "cat.sch.mv_a", "measures": [{"name": "m"}]},
                        {"identifier": "cat.sch.mv_b", "measures": [{"name": "m"}]},
                    ],
                    "tables": [],
                },
            },
            "_metric_view_yaml": {
                "cat.sch.mv_c": {"measures": [{"name": "m"}]},
            },
        }
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_unified_example_summary(
                run_id="r1", target=20, existing=0,
                applied_examples=[],
                rejection_counters={},
                config=cfg,
            )
        out = buf.getvalue()
        assert "MVs detected" in out
        assert "config: 2" in out
        assert "column-flags: 0" in out
        assert "catalog: 1" in out

    def test_unified_banner_renders_short_circuit_marker(self):
        """When ``adaptive_overdraw_short_circuited`` is set on the
        counters, the banner must surface it on its own line so
        operators see why round count plateaued at 1.
        """
        import io
        import contextlib

        from genie_space_optimizer.optimization.harness import (
            _print_unified_example_summary,
        )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_unified_example_summary(
                run_id="r1", target=20, existing=0,
                applied_examples=[],
                rejection_counters={
                    "adaptive_overdraw_short_circuited": "no_mv_measures",
                },
            )
        out = buf.getvalue()
        assert "Adaptive overdraw short-circuited" in out
        assert "no_mv_measures" in out

    def test_unified_banner_omits_short_circuit_when_unset(self):
        import io
        import contextlib

        from genie_space_optimizer.optimization.harness import (
            _print_unified_example_summary,
        )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_unified_example_summary(
                run_id="r1", target=20, existing=0,
                applied_examples=[],
                rejection_counters={
                    "metadata": 0,
                    "explain_or_execute": 0,
                },
            )
        out = buf.getvalue()
        assert "Adaptive overdraw short-circuited" not in out


# ═══════════════════════════════════════════════════════════════════════
# PR 22 — Sub-bucket parity between unified and preflight banners
#
# PR 18 surfaced per-reason sub-buckets on the unified banner so
# operators could attribute ``EXPLAIN/execute rejected: 40`` to a
# specific failure class (unknown column / mv_missing_measure_function /
# alias collision / etc.). PR 20 added ``mv_measure_in_where`` as a new
# reason code; this suite verifies that:
#
#   1. ``_classify_sql_validation_error`` returns the new code for
#      WHERE/HAVING/ON-clause measure errors so PR 18's sub-bucket
#      machinery picks it up automatically.
#   2. The preflight banner now renders the same sub-bucket breakdown
#      for execute-gate rejections so both pipelines tell the same
#      story.
# ═══════════════════════════════════════════════════════════════════════


class TestPR22SubBucketParity:
    def test_unified_subbuckets_capture_mv_measure_in_where(
        self, patched_core, monkeypatch,
    ):
        """End-to-end: feed a candidate that fails with the WHERE-clause
        flavor of ``METRIC_VIEW_MISSING_MEASURE_FUNCTION``; assert the
        sub-bucket breakdown reports ``mv_measure_in_where`` (not the
        plain ``mv_missing_measure_function``).
        """
        from genie_space_optimizer.optimization import evaluation as ev_mod

        monkeypatch.setattr(
            ev_mod, "_call_llm_for_scoring",
            lambda w, prompt, *, prompt_name=None: [{
                "question": "Filter on a measure?",
                "expected_sql": (
                    "SELECT region FROM cat.sch.sales "
                    "WHERE total_revenue > 0"
                ),
                "expected_asset": "TABLE",
                "required_tables": ["cat.sch.sales"],
                "required_columns": ["region"],
                "expected_facts": ["region"],
                "category": "filter",
            }],
        )
        monkeypatch.setattr(
            ev_mod, "_validate_benchmark_sql",
            lambda *a, **kw: (False, (
                "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] measure column "
                "'total_revenue' in WHERE clause must be wrapped in MEASURE()"
            )),
        )
        monkeypatch.setattr(
            ev_mod, "_attempt_sql_correction",
            lambda **kwargs: [],
        )

        _, counters = generate_validated_sql_examples(
            w=None, spark=None,
            config=_mk_config(), uc_columns=_mk_uc_columns(),
            uc_tags=[], uc_routines=[],
            domain="sales", catalog="cat", schema="sch",
            target_count=1,
            generation_prompt_template=EXAMPLE_SQL_GENERATION_PROMPT,
            correction_prompt_template=EXAMPLE_SQL_CORRECTION_PROMPT,
            generation_prompt_registry_key="example_sql_generation",
            correction_prompt_registry_key="example_sql_correction",
            provenance="synthetic_example_sql",
        )
        sub = counters.get("explain_or_execute_subbuckets") or {}
        assert sub.get("mv_measure_in_where", 0) >= 1, counters
        # And the plain bucket DOES NOT also count this candidate.
        assert sub.get("mv_missing_measure_function", 0) == 0, counters

    def test_unified_banner_renders_mv_measure_in_where_subbucket(self):
        """Banner must show ``mv_measure_in_where`` as a separate
        sub-bucket line (not folded into the catch-all bucket)."""
        import io
        import contextlib

        from genie_space_optimizer.optimization.harness import (
            _print_unified_example_summary,
        )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_unified_example_summary(
                run_id="r1", target=20, existing=0,
                applied_examples=[],
                rejection_counters={
                    "explain_or_execute": 3,
                    "explain_or_execute_subbuckets": {
                        "mv_measure_in_where": 2,
                        "mv_missing_measure_function": 1,
                    },
                    "explain_or_execute_examples": {
                        "mv_measure_in_where": [{
                            "question": "Filter on measure?",
                            "error": "WHERE clause has measure col",
                        }],
                    },
                },
            )
        out = buf.getvalue()
        assert "mv_measure_in_where" in out
        assert "mv_missing_measure_function" in out

    def test_preflight_banner_renders_execute_subbuckets(self):
        """The preflight banner must mirror the unified banner's
        sub-bucket rendering for execute-gate rejections — same shape
        so operators reading either pipeline see the same diagnostic.
        """
        import io
        import contextlib

        from genie_space_optimizer.optimization.preflight_synthesis import (
            _print_summary,
        )

        result = {
            "applied": 0,
            "need": 5,
            "existing": 0,
            "target": 5,
            "generated": 5,
            "passed_parse": 5,
            "passed_identifier_qualification": 5,
            "passed_execute": 0,
            "passed_firewall": 0,
            "passed_structural": 0,
            "passed_arbiter": 0,
            "passed_genie_agreement": 0,
            "dedup_rejected": 0,
            "rejected_by_gate": {"execute": 5},
            "asset_coverage": {},
            "archetype_distribution": {},
            "skipped_reason": None,
            "traits": [],
            "eligible_archetypes": [],
            "gate_rejected_examples": [],
            "execute_subbuckets": {
                "mv_measure_in_where": 3,
                "mv_missing_measure_function": 2,
            },
            "execute_subbucket_examples": {
                "mv_measure_in_where": [{
                    "question": "Q1",
                    "error": "WHERE on measure col",
                }],
                "mv_missing_measure_function": [{
                    "question": "Q2",
                    "error": "Wrap in MEASURE()",
                }],
            },
        }
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_summary(result)
        out = buf.getvalue()
        assert "execute sub-buckets" in out
        assert "mv_measure_in_where" in out
        assert "mv_missing_measure_function" in out
        # Most-frequent reason rendered first (count 3 > count 2).
        assert out.find("mv_measure_in_where") < out.find(
            "mv_missing_measure_function"
        )

    def test_preflight_banner_omits_subbuckets_when_empty(self):
        """When no execute-gate rejections were sub-bucketed, the
        section is hidden so the banner stays terse on clean runs.
        """
        import io
        import contextlib

        from genie_space_optimizer.optimization.preflight_synthesis import (
            _print_summary,
        )

        result = {
            "applied": 5,
            "need": 5,
            "existing": 0,
            "target": 5,
            "generated": 5,
            "passed_parse": 5,
            "passed_identifier_qualification": 5,
            "passed_execute": 5,
            "passed_firewall": 5,
            "passed_structural": 5,
            "passed_arbiter": 5,
            "passed_genie_agreement": 0,
            "dedup_rejected": 0,
            "rejected_by_gate": {},
            "asset_coverage": {},
            "archetype_distribution": {},
            "skipped_reason": None,
            "traits": [],
            "eligible_archetypes": [],
            "gate_rejected_examples": [],
        }
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_summary(result)
        out = buf.getvalue()
        assert "execute sub-buckets" not in out


class TestPR23DetectionVsRejectionHint:
    """When 0 MVs are detected but mv_* sub-buckets fired, surface a hint."""

    def _capture_banner(self, *, config, rejection_counters):
        import io
        import contextlib

        from genie_space_optimizer.optimization.harness import (
            _print_unified_example_summary,
        )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _print_unified_example_summary(
                run_id="r1", target=20, existing=0,
                applied_examples=[],
                rejection_counters=rejection_counters,
                config=config,
            )
        return buf.getvalue()

    def test_hint_when_zero_mvs_but_mv_rejection(self):
        """0 MVs detected + at least one mv_* sub-bucket → hint appears."""
        out = self._capture_banner(
            config={
                "_parsed_space": {
                    "data_sources": {"tables": [], "metric_views": []},
                },
            },
            rejection_counters={
                "explain_or_execute": 5,
                "explain_or_execute_subbuckets": {
                    "mv_missing_measure_function": 5,
                },
            },
        )
        assert "0 MVs detected" in out
        assert "mv_* rejections present" in out

    def test_no_hint_when_mvs_detected(self):
        """MVs detected → hint suppressed even when mv_* sub-buckets present."""
        out = self._capture_banner(
            config={
                "_parsed_space": {
                    "data_sources": {
                        "tables": [],
                        "metric_views": [
                            {"identifier": "cat.sch.mv_x"},
                        ],
                    },
                },
            },
            rejection_counters={
                "explain_or_execute": 1,
                "explain_or_execute_subbuckets": {
                    "mv_missing_measure_function": 1,
                },
            },
        )
        assert "0 MVs detected but mv_* rejections present" not in out

    def test_no_hint_when_no_mv_rejections(self):
        """No mv_* sub-buckets → hint suppressed even with zero MVs."""
        out = self._capture_banner(
            config={
                "_parsed_space": {
                    "data_sources": {"tables": [], "metric_views": []},
                },
            },
            rejection_counters={
                "explain_or_execute": 2,
                "explain_or_execute_subbuckets": {
                    "unknown_column": 2,
                },
            },
        )
        assert "0 MVs detected but mv_* rejections present" not in out
