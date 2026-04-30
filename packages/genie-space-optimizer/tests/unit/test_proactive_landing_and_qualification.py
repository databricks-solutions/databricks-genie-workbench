"""Phase 5 tests for the fix-proactive-enrichment-landing-and-qualification plan.

Covers:
  - Phase 1 — runtime-key contract: validator and stripper both defer
    to ``common.config.is_runtime_key`` so ``_data_profile`` no longer
    blocks the PATCH path.
  - Phase 2 — LLM qualification defect: prompt carries a concrete
    worked example; the new ``_gate_identifier_qualification`` runs
    before ``_gate_execute`` and catches unqualified FROM/JOIN targets
    with the slice's allowlist; the R6 retry engages on qualification
    failures with exact-identifier feedback.
  - Phase 3 — hardening: SQL-expression miner rebinds benchmark-local
    aliases to real identifiers; ``_extract_json`` returns ``{}`` on
    empty input instead of raising.
"""

from __future__ import annotations

import copy
from unittest.mock import patch

from genie_space_optimizer.common.config import (
    INTERNAL_RUNTIME_KEYS_PREFIX,
    KNOWN_INTERNAL_RUNTIME_KEYS,
    is_runtime_key,
)
from genie_space_optimizer.common.genie_client import strip_non_exportable_fields
from genie_space_optimizer.common.genie_schema import validate_serialized_space
from genie_space_optimizer.optimization.archetypes import ARCHETYPES
from genie_space_optimizer.optimization.evaluation import _extract_json
from genie_space_optimizer.optimization.optimizer import (
    _extract_alias_bindings,
    _mine_sql_expression_candidates,
    _rebind_expression_aliases,
)
from genie_space_optimizer.optimization.preflight_synthesis import (
    AssetSlice,
    _build_qualification_feedback,
    _is_qualification_failure,
    render_preflight_prompt,
)
from genie_space_optimizer.optimization.synthesis import (
    GateResult,
    _extract_from_join_targets,
    _gate_identifier_qualification,
    validate_synthesis_proposal,
)


# ═══════════════════════════════════════════════════════════════════════
# Shared fixtures
# ═══════════════════════════════════════════════════════════════════════


def _mk_slice() -> AssetSlice:
    return AssetSlice(
        tables=[
            {
                "identifier": "cat.sch.mv_esr_dim_date",
                "name": "mv_esr_dim_date",
                "column_configs": [
                    {"column_name": "day_of_week", "data_type": "STRING"},
                    {"column_name": "full_date", "data_type": "DATE"},
                ],
            },
        ],
        columns=[
            ("cat.sch.mv_esr_dim_date", "day_of_week"),
            ("cat.sch.mv_esr_dim_date", "full_date"),
        ],
    )


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R1 — runtime-key convention in the validator
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1RuntimeKeyConvention:
    def test_strict_validate_tolerates_underscore_prefixed_keys(self):
        config = {
            "version": 2,
            "data_sources": {"tables": [], "metric_views": []},
            "_data_profile": {"cat.sch.t": {}},
            "_failure_clusters": [{"cluster_id": "C1"}],
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok, f"runtime keys must not error: {errors}"

    def test_strict_validate_rejects_unknown_non_underscore_key(self):
        config = {
            "version": 2,
            "data_sources": {"tables": [], "metric_views": []},
            "garbage_key": {"stuff": True},
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert not ok
        joined = "\n".join(errors)
        assert "garbage_key" in joined

    def test_is_runtime_key_convention(self):
        assert is_runtime_key("_data_profile")
        assert is_runtime_key("_anything")
        assert not is_runtime_key("data_sources")
        assert not is_runtime_key("")
        assert not is_runtime_key(42)  # type: ignore[arg-type]
        assert INTERNAL_RUNTIME_KEYS_PREFIX == "_"
        assert "_data_profile" in KNOWN_INTERNAL_RUNTIME_KEYS


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R2 — applier validates the stripped payload
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1AppliesStripBeforeValidate:
    def test_strip_removes_runtime_before_validate(self):
        """Structural check of the Phase 1.R2 invariant: the post-patch
        validator must operate on a payload that has been through
        ``strip_non_exportable_fields`` first. We verify that the
        stripper does indeed drop underscore-prefixed runtime keys so
        the validator sees a clean payload, which is exactly what the
        applier now does at ``applier.py:apply_patches``.
        """
        config_with_runtime = {
            "version": 2,
            "data_sources": {"tables": [], "metric_views": []},
            "instructions": {"example_question_sqls": []},
            "_data_profile": {"cat.sch.t": {"columns": {}}},
            "_failure_clusters": [],
            "_cluster_synthesis_count": 3,
        }
        stripped = strip_non_exportable_fields(copy.deepcopy(config_with_runtime))
        assert "_data_profile" not in stripped
        assert "_failure_clusters" not in stripped
        assert "_cluster_synthesis_count" not in stripped
        assert "data_sources" in stripped
        # Validating the stripped payload must succeed.
        ok, errors = validate_serialized_space(stripped, strict=True)
        assert ok, f"stripped payload should validate, got: {errors}"

    def test_applier_call_site_uses_stripped_payload(self):
        """Source-level guard — the applier's post-patch validate call
        must be preceded by ``strip_non_exportable_fields`` on a deep
        copy. Pins the R2 wiring so a future edit cannot silently
        revert to validating the runtime config."""
        import inspect

        from genie_space_optimizer.optimization import applier as applier_mod

        source = inspect.getsource(applier_mod.apply_patch_set)
        assert "strip_non_exportable_fields(copy.deepcopy(config))" in source
        assert "validate_serialized_space" in source
        # The strip call must appear BEFORE the validator call.
        strip_idx = source.index("strip_non_exportable_fields(copy.deepcopy(config))")
        validate_idx = source.index("validate_serialized_space(")
        assert strip_idx < validate_idx


# ═══════════════════════════════════════════════════════════════════════
# Phase 2.R4 — prompt qualification constraint
# ═══════════════════════════════════════════════════════════════════════


class TestPhase2PromptQualification:
    def test_preflight_prompt_includes_qualification_constraint_with_example(self):
        arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
        slice_ = _mk_slice()
        prompt = render_preflight_prompt(arch, slice_, [])
        assert "## Constraint: identifier qualification" in prompt
        # The worked example renders the first slice identifier verbatim.
        assert "cat.sch.mv_esr_dim_date" in prompt
        assert "BAD" in prompt and "GOOD" in prompt


# ═══════════════════════════════════════════════════════════════════════
# Phase 2.R5 — identifier-qualification structural gate
# ═══════════════════════════════════════════════════════════════════════


class TestPhase2IdentifierQualificationGate:
    def test_extract_from_join_targets_handles_backticks_and_case(self):
        sql = (
            "SELECT d.day_of_week FROM `Cat`.`Sch`.`Mv_Esr_Dim_Date` d "
            "JOIN cat.sch.mv_esr_fact_sales f ON f.calendar_sk = d.calendar_sk"
        )
        targets = _extract_from_join_targets(sql)
        assert "cat.sch.mv_esr_dim_date" in targets
        assert "cat.sch.mv_esr_fact_sales" in targets

    def test_extract_from_join_targets_ignores_string_literals(self):
        sql = "SELECT note FROM cat.sch.t WHERE note = 'from other table'"
        targets = _extract_from_join_targets(sql)
        assert targets == ["cat.sch.t"]

    def test_gate_rejects_unqualified_from(self):
        proposal = {
            "example_sql": "SELECT day_of_week FROM dim_date LIMIT 10",
        }
        result = _gate_identifier_qualification(
            proposal, {"cat.sch.mv_esr_dim_date"},
        )
        assert result.passed is False
        assert result.gate == "identifier_qualification"
        assert "UNQUALIFIED_TABLE" in result.reason

    def test_gate_accepts_qualified(self):
        proposal = {
            "example_sql": (
                "SELECT day_of_week FROM cat.sch.mv_esr_dim_date LIMIT 10"
            ),
        }
        result = _gate_identifier_qualification(
            proposal, {"cat.sch.mv_esr_dim_date"},
        )
        assert result.passed is True

    def test_gate_skips_when_no_allowlist(self):
        proposal = {"example_sql": "SELECT x FROM whatever"}
        result = _gate_identifier_qualification(proposal, None)
        assert result.passed is True
        assert "skipped" in (result.reason or "")

    def test_gate_runs_before_execute_in_validator(self):
        """Ordering guarantee: qualification failure short-circuits the
        execute gate so the warehouse is never touched for an obviously
        unqualified proposal that the deterministic repair pipeline
        cannot fix.

        PR 31 introduced a shared pre-execute repair pass that runs
        BEFORE qualification — when the bare stem has a unique match in
        the allowlist the repair promotes it (``FROM dim_date`` →
        ``FROM cat.sch.mv_esr_dim_date``) and qualification passes. To
        keep the original ordering invariant testable we use a stem
        with NO match in the allowlist; the repair becomes a no-op
        and qualification rejects before execute runs.
        """
        proposal = {
            "example_question": "show days",
            "example_sql": "SELECT x FROM totally_unknown_table",
        }
        archetype = next(a for a in ARCHETYPES if a.name == "simple_enumerate")

        execute_called = {"count": 0}

        def _fake_execute(proposal, **_kw):
            execute_called["count"] += 1
            return GateResult(True, "execute")

        with patch(
            "genie_space_optimizer.optimization.synthesis._gate_execute",
            side_effect=_fake_execute,
        ):
            passed, results = validate_synthesis_proposal(
                proposal,
                archetype=archetype, benchmark_corpus=None,
                identifier_allowlist={"cat.sch.mv_esr_dim_date"},
            )
        assert passed is False
        assert execute_called["count"] == 0
        assert any(
            r.gate == "identifier_qualification" and not r.passed for r in results
        )

    def test_pre_execute_repair_promotes_unique_stem_then_qualifies(self):
        """PR 31 contract: when the proposal SQL has a bare stem with
        exactly one canonical match in the allowlist, the shared
        pre-execute repair promotes the stem to its canonical form,
        qualification then passes, and execute runs on the repaired
        SQL. This avoids burning an LLM retry round on a class of
        failures the deterministic repair can fix outright.
        """
        proposal = {
            "example_question": "show days",
            "example_sql": "SELECT x FROM dim_date",
        }
        archetype = next(a for a in ARCHETYPES if a.name == "simple_enumerate")

        execute_called = {"count": 0, "sql": None}

        def _fake_execute(proposal, **_kw):
            execute_called["count"] += 1
            execute_called["sql"] = proposal.get("example_sql")
            return GateResult(True, "execute")

        with patch(
            "genie_space_optimizer.optimization.synthesis._gate_execute",
            side_effect=_fake_execute,
        ):
            passed, results = validate_synthesis_proposal(
                proposal,
                archetype=archetype, benchmark_corpus=None,
                identifier_allowlist={"cat.sch.mv_esr_dim_date"},
            )
        # Execute must have been called on the REPAIRED SQL — the
        # deterministic stem-repair fixes ``FROM dim_date`` →
        # ``FROM cat.sch.mv_esr_dim_date`` before qualification runs.
        assert execute_called["count"] == 1
        assert execute_called["sql"] is not None
        assert "cat.sch.mv_esr_dim_date" in execute_called["sql"].lower()
        assert "from dim_date" not in execute_called["sql"].lower()


# ═══════════════════════════════════════════════════════════════════════
# Phase 2.R6 — retry engages on qualification / unresolved failures
# ═══════════════════════════════════════════════════════════════════════


class TestPhase2QualificationRetry:
    def test_retry_feedback_includes_allowlist_and_failure_reason(self):
        slice_ = _mk_slice()
        proposal = {
            "example_sql": "SELECT day_of_week FROM dim_date LIMIT 5",
        }
        feedback = _build_qualification_feedback(
            proposal, slice_, "UNQUALIFIED_TABLE: dim_date",
        )
        assert "UNQUALIFIED_TABLE" in feedback
        assert "cat.sch.mv_esr_dim_date" in feedback
        assert "SELECT day_of_week FROM dim_date" in feedback

    def test_is_qualification_failure_recognizes_both_sources(self):
        # New gate:
        gr = GateResult(False, "identifier_qualification", "UNQUALIFIED_TABLE: x")
        assert _is_qualification_failure(gr)
        # Spark-side error surfaced through the execute gate:
        gr = GateResult(False, "execute", "UNRESOLVED_COLUMN: foo")
        assert _is_qualification_failure(gr)
        gr = GateResult(False, "execute", "TABLE_OR_VIEW_NOT_FOUND: bar")
        assert _is_qualification_failure(gr)
        # Value-style failure is NOT a qualification failure:
        gr = GateResult(False, "execute", "EMPTY_RESULT: 0 rows")
        assert not _is_qualification_failure(gr)


# ═══════════════════════════════════════════════════════════════════════
# Phase 3.R7 — SQL-expression alias rebinding
# ═══════════════════════════════════════════════════════════════════════


class TestPhase3AliasRebinding:
    def test_extract_alias_bindings_basic(self):
        sql = "SELECT * FROM cat.sch.fact_sales F JOIN cat.sch.dim_location AS L ON F.loc=L.id"
        bindings = _extract_alias_bindings(sql)
        assert bindings["f"] == "cat.sch.fact_sales"
        assert bindings["l"] == "cat.sch.dim_location"
        # Identity binding for the bare short name:
        assert bindings["fact_sales"] == "cat.sch.fact_sales"
        assert bindings["dim_location"] == "cat.sch.dim_location"

    def test_rebind_replaces_alias_with_full_identifier(self):
        bindings = {"f": "cat.sch.fact_sales"}
        out = _rebind_expression_aliases("SUM(F.SALES_AMOUNT_USD)", bindings)
        assert out == "SUM(cat.sch.fact_sales.SALES_AMOUNT_USD)"

    def test_rebind_returns_none_when_alias_unknown(self):
        bindings = {"f": "cat.sch.fact_sales"}
        out = _rebind_expression_aliases("SUM(G.X)", bindings)
        assert out is None

    def test_rebind_ignores_keywords_not_aliases(self):
        bindings = {"f": "cat.sch.t"}
        # Using DISTINCT inside isn't an alias.
        out = _rebind_expression_aliases(
            "COUNT(DISTINCT f.id)", bindings,
        )
        assert out == "COUNT(DISTINCT cat.sch.t.id)"

    def test_miner_rebinds_and_drops_unrebindable(self):
        benchmarks = [
            {
                "id": "b1",
                # Clean, rebindable (x2 so freq-min=2 threshold is met):
                "expected_sql": (
                    "SELECT SUM(F.AMOUNT) FROM cat.sch.fact_sales F"
                    " GROUP BY F.REGION"
                ),
            },
            {
                "id": "b1_dup",
                "expected_sql": (
                    "SELECT SUM(F.AMOUNT) FROM cat.sch.fact_sales F"
                    " GROUP BY F.COUNTRY"
                ),
            },
            {
                "id": "b2",
                # No alias declared for ``Z`` — must be dropped.
                "expected_sql": "SELECT SUM(Z.UNKNOWN) FROM cat.sch.fact_sales F",
            },
        ]
        with patch(
            "genie_space_optimizer.common.config.SQL_EXPRESSION_MIN_FREQUENCY", 2,
        ):
            candidates = _mine_sql_expression_candidates(benchmarks, {})

        rebound = [c["sql"] for c in candidates if c["snippet_type"] == "measure"]
        # The rebound expression appears with the full identifier. The
        # miner uppercases aggregation expressions (the existing
        # normalization contract) so we match against the upper form.
        assert any("CAT.SCH.FACT_SALES.AMOUNT" in s for s in rebound)
        assert not any("Z.UNKNOWN" in s for s in rebound)
        dropped = getattr(
            _mine_sql_expression_candidates, "last_rebind_dropped", 0,
        )
        assert dropped >= 1


# ═══════════════════════════════════════════════════════════════════════
# Phase 3.R8 — `_extract_json` empty-input handling
# ═══════════════════════════════════════════════════════════════════════


class TestPhase3ExtractJsonEmptyHandling:
    def test_empty_input_returns_none(self):
        # Task 8 — ``_extract_json`` now returns ``None`` for empty /
        # whitespace-only input so callers can branch on a typed soft
        # failure instead of catching a JSONDecodeError. Pass ``strict=True``
        # to preserve the legacy raise-on-error behaviour.
        assert _extract_json("") is None
        assert _extract_json("   ") is None
        assert _extract_json("\n\t") is None

    def test_non_empty_invalid_returns_none_or_raises_under_strict(self):
        """Task 8 — non-empty malformed input returns ``None`` by default
        and raises only when ``strict=True``. Callers that want a hard
        failure on garbage input opt in explicitly."""
        import json as _json

        assert _extract_json("not json at all") is None
        try:
            _extract_json("not json at all", strict=True)
        except _json.JSONDecodeError:
            return
        raise AssertionError("expected JSONDecodeError under strict=True")

    def test_extract_json_valid_dict_unchanged(self):
        result = _extract_json('{"changes": [{"x": 1}]}')
        assert result == {"changes": [{"x": 1}]}
