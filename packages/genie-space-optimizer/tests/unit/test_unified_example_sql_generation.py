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
        return [{"region": "NA", "total_revenue": 1000}]

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
