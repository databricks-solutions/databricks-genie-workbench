"""Unit tests for PR 32 — categorical value-domain hints and cast
guardrails.

PR 32 introduces two contract points:

1. :func:`evaluation.check_categorical_cast_violations` — detects
   ``CAST(<col> AS <numeric_type>)`` whose argument is a categorical
   string column with non-numeric sample values (e.g. Y/N flags
   being cast to BIGINT). Returns a list of offending
   ``(column, target_type, samples)`` tuples.

2. ``cast_invalid_input`` reason code in
   :func:`evaluation._classify_sql_validation_error` — distinct
   from generic ``sql_compile_error`` so the LLM correction loop
   can surface a class-specific repair hint.

3. The synthesis ``_gate_categorical_cast`` gate, integrated into
   :func:`synthesis.validate_synthesis_proposal`, hard-fails a
   proposal whose SQL would trigger ``CAST_INVALID_INPUT`` against
   the warehouse — saving a round-trip on a deterministically-
   detectable failure.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.evaluation import (
    _classify_sql_validation_error,
    _is_numeric_value,
    _profile_lookup,
    _repair_hint_for_reason,
    check_categorical_cast_violations,
)
from genie_space_optimizer.optimization.synthesis import (
    GateResult,
    validate_synthesis_proposal,
)


# ════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════


def _profile(table: str, column: str, distinct_values: list[str]) -> dict:
    return {
        table: {
            "row_count": 100,
            "columns": {
                column: {
                    "cardinality": len(distinct_values),
                    "distinct_values": distinct_values,
                },
            },
        },
    }


# ════════════════════════════════════════════════════════════════════
# _is_numeric_value
# ════════════════════════════════════════════════════════════════════


class TestIsNumericValue:
    def test_integers_are_numeric(self):
        assert _is_numeric_value("0")
        assert _is_numeric_value("42")
        assert _is_numeric_value("-7")

    def test_floats_are_numeric(self):
        assert _is_numeric_value("3.14")
        assert _is_numeric_value("-0.001")
        assert _is_numeric_value("1e10")
        assert _is_numeric_value("  123.45  ")

    def test_strings_are_not_numeric(self):
        assert not _is_numeric_value("Y")
        assert not _is_numeric_value("N")
        assert not _is_numeric_value("yes")
        assert not _is_numeric_value("true")
        assert not _is_numeric_value("")
        assert not _is_numeric_value("   ")
        assert not _is_numeric_value(None)


# ════════════════════════════════════════════════════════════════════
# _profile_lookup
# ════════════════════════════════════════════════════════════════════


class TestProfileLookup:
    def test_finds_column_with_table_hint(self):
        profile = _profile("cat.sch.t", "flag", ["Y", "N"])
        cinfo = _profile_lookup(profile, "flag", table_hint="cat.sch.t")
        assert cinfo is not None
        assert cinfo["distinct_values"] == ["Y", "N"]

    def test_finds_column_without_hint_via_scan(self):
        profile = _profile("cat.sch.t", "flag", ["Y", "N"])
        cinfo = _profile_lookup(profile, "FLAG")
        assert cinfo is not None
        assert cinfo["distinct_values"] == ["Y", "N"]

    def test_missing_column_returns_none(self):
        profile = _profile("cat.sch.t", "flag", ["Y", "N"])
        assert _profile_lookup(profile, "other_col") is None

    def test_empty_profile_returns_none(self):
        assert _profile_lookup({}, "flag") is None
        assert _profile_lookup(None, "flag") is None


# ════════════════════════════════════════════════════════════════════
# check_categorical_cast_violations
# ════════════════════════════════════════════════════════════════════


class TestCheckCategoricalCastViolations:
    def test_yn_flag_cast_to_bigint_flagged(self):
        sql = "SELECT CAST(same_store_7now AS BIGINT) FROM cat.sch.t"
        profile = _profile("cat.sch.t", "same_store_7now", ["Y", "N"])
        violations = check_categorical_cast_violations(sql, profile)
        assert len(violations) == 1
        col, target, samples = violations[0]
        assert col.lower() == "same_store_7now"
        assert target.lower() == "bigint"
        assert "Y" in samples and "N" in samples

    def test_numeric_string_cast_passes(self):
        sql = "SELECT CAST(amount_str AS BIGINT) FROM cat.sch.t"
        profile = _profile("cat.sch.t", "amount_str", ["1", "2", "3"])
        violations = check_categorical_cast_violations(sql, profile)
        assert violations == []

    def test_cast_to_date_not_flagged(self):
        sql = "SELECT CAST(flag AS DATE) FROM cat.sch.t"
        profile = _profile("cat.sch.t", "flag", ["Y", "N"])
        violations = check_categorical_cast_violations(sql, profile)
        # Only numeric casts are guarded.
        assert violations == []

    def test_cast_to_double_with_categorical_values_flagged(self):
        sql = "SELECT CAST(status AS DOUBLE) FROM cat.sch.t"
        profile = _profile("cat.sch.t", "status", ["active", "inactive"])
        violations = check_categorical_cast_violations(sql, profile)
        assert len(violations) == 1
        assert violations[0][1].lower() == "double"

    def test_no_profile_returns_empty(self):
        sql = "SELECT CAST(flag AS BIGINT) FROM cat.sch.t"
        assert check_categorical_cast_violations(sql, {}) == []
        assert check_categorical_cast_violations(sql, None) == []

    def test_unknown_column_not_flagged(self):
        sql = "SELECT CAST(unknown_col AS BIGINT) FROM cat.sch.t"
        profile = _profile("cat.sch.t", "flag", ["Y", "N"])
        violations = check_categorical_cast_violations(sql, profile)
        assert violations == []

    def test_no_cast_returns_empty(self):
        sql = "SELECT flag FROM cat.sch.t WHERE flag = 'Y'"
        profile = _profile("cat.sch.t", "flag", ["Y", "N"])
        violations = check_categorical_cast_violations(sql, profile)
        assert violations == []

    def test_dedup_of_repeated_casts(self):
        sql = (
            "SELECT CAST(flag AS BIGINT), CAST(flag AS BIGINT) FROM cat.sch.t"
        )
        profile = _profile("cat.sch.t", "flag", ["Y", "N"])
        violations = check_categorical_cast_violations(sql, profile)
        assert len(violations) == 1


# ════════════════════════════════════════════════════════════════════
# Reason code & repair hint
# ════════════════════════════════════════════════════════════════════


class TestCastInvalidInputClassifier:
    def test_cast_invalid_input_marker(self):
        msg = "[CAST_INVALID_INPUT] The value 'N' cannot be cast to BIGINT"
        assert _classify_sql_validation_error(msg) == "cast_invalid_input"

    def test_cannot_be_cast_to_marker(self):
        msg = "Value 'Y' cannot be cast to type BIGINT"
        assert _classify_sql_validation_error(msg) == "cast_invalid_input"

    def test_repair_hint_present(self):
        hint = _repair_hint_for_reason("cast_invalid_input")
        assert hint
        assert "categorical" in hint.lower()
        assert "case" in hint.lower() or "string" in hint.lower()


# ════════════════════════════════════════════════════════════════════
# Synthesis gate integration
# ════════════════════════════════════════════════════════════════════


class TestSynthesisCategoricalCastGate:
    """The new gate must reject SQL whose CAST would fail given the
    current data profile, and must NOT reject SQL when the profile
    is absent or shows the cast is safe."""

    def test_gate_rejects_on_categorical_cast(self):
        # We test the gate logic in isolation by short-circuiting the
        # earlier gates (parse always passes; we use a permissive
        # allowlist; we never reach execute because the new gate
        # rejects first).
        from unittest.mock import patch

        from genie_space_optimizer.optimization.archetypes import ARCHETYPES

        profile = _profile("cat.sch.fact", "flag", ["Y", "N"])
        snapshot = {"_data_profile": profile}
        proposal = {
            "example_question": "How many rows have flag = Y?",
            "example_sql": (
                "SELECT CAST(flag AS BIGINT) AS f FROM cat.sch.fact"
            ),
        }
        archetype = next(a for a in ARCHETYPES if a.name == "simple_enumerate")
        with patch(
            "genie_space_optimizer.optimization.synthesis._gate_execute",
            return_value=GateResult(True, "execute"),
        ):
            passed, results = validate_synthesis_proposal(
                proposal,
                archetype=archetype, benchmark_corpus=None,
                metadata_snapshot=snapshot,
                identifier_allowlist={"cat.sch.fact"},
            )
        assert not passed
        cast_failure = next(
            (r for r in results if r.gate == "categorical_cast"),
            None,
        )
        assert cast_failure is not None
        assert not cast_failure.passed
        assert "cast_invalid_input" in (cast_failure.reason or "").lower()

    def test_gate_passes_when_no_profile(self):
        from unittest.mock import patch

        from genie_space_optimizer.optimization.archetypes import ARCHETYPES

        snapshot: dict = {}  # no _data_profile
        proposal = {
            "example_question": "How many rows?",
            "example_sql": (
                "SELECT CAST(flag AS BIGINT) AS f FROM cat.sch.fact"
            ),
        }
        archetype = next(a for a in ARCHETYPES if a.name == "simple_enumerate")
        with patch(
            "genie_space_optimizer.optimization.synthesis._gate_execute",
            return_value=GateResult(True, "execute"),
        ), patch(
            "genie_space_optimizer.optimization.synthesis._gate_arbiter",
            return_value=GateResult(True, "arbiter"),
        ), patch(
            "genie_space_optimizer.optimization.synthesis._gate_firewall",
            return_value=GateResult(True, "firewall"),
        ), patch(
            "genie_space_optimizer.optimization.synthesis._gate_structural",
            return_value=GateResult(True, "structural"),
        ):
            _passed, results = validate_synthesis_proposal(
                proposal,
                archetype=archetype, benchmark_corpus=None,
                metadata_snapshot=snapshot,
                identifier_allowlist={"cat.sch.fact"},
            )
        cast_result = next(
            (r for r in results if r.gate == "categorical_cast"),
            None,
        )
        assert cast_result is not None
        assert cast_result.passed
        assert "no_profile" in (cast_result.reason or "")

    def test_gate_passes_when_cast_is_safe(self):
        from unittest.mock import patch

        from genie_space_optimizer.optimization.archetypes import ARCHETYPES

        # Profile says column is numeric strings — cast is safe.
        profile = _profile("cat.sch.fact", "amount_str", ["1", "2", "3"])
        snapshot = {"_data_profile": profile}
        proposal = {
            "example_question": "Sum of amounts?",
            "example_sql": (
                "SELECT CAST(amount_str AS BIGINT) FROM cat.sch.fact"
            ),
        }
        archetype = next(a for a in ARCHETYPES if a.name == "simple_enumerate")
        with patch(
            "genie_space_optimizer.optimization.synthesis._gate_execute",
            return_value=GateResult(True, "execute"),
        ), patch(
            "genie_space_optimizer.optimization.synthesis._gate_arbiter",
            return_value=GateResult(True, "arbiter"),
        ), patch(
            "genie_space_optimizer.optimization.synthesis._gate_firewall",
            return_value=GateResult(True, "firewall"),
        ), patch(
            "genie_space_optimizer.optimization.synthesis._gate_structural",
            return_value=GateResult(True, "structural"),
        ):
            _passed, results = validate_synthesis_proposal(
                proposal,
                archetype=archetype, benchmark_corpus=None,
                metadata_snapshot=snapshot,
                identifier_allowlist={"cat.sch.fact"},
            )
        cast_result = next(
            (r for r in results if r.gate == "categorical_cast"),
            None,
        )
        assert cast_result is not None
        assert cast_result.passed


# ════════════════════════════════════════════════════════════════════
# Prompt-side categorical hint (PR 32 verification)
# ════════════════════════════════════════════════════════════════════


class TestCategoricalValueDomainHints:
    """The plan's PR 32 verification asserts the prompt includes
    sampled categorical values (e.g. ``values: 'Y', 'N'``) for low-
    cardinality flag columns. This already happens via
    :func:`preflight_synthesis._format_slice_data_profile`. We pin
    that contract here so future refactors don't silently drop it.
    """

    def test_low_cardinality_values_render_in_prompt_block(self):
        from genie_space_optimizer.optimization.preflight_synthesis import (
            AssetSlice,
            _format_slice_data_profile,
        )

        slice_ = AssetSlice(
            tables=[{"name": "cat.sch.fact"}],
            columns=[("cat.sch.fact", "same_store_7now")],
        )
        data_profile = _profile(
            "cat.sch.fact", "same_store_7now", ["Y", "N"],
        )
        rendered = _format_slice_data_profile(slice_, data_profile)
        assert "same_store_7now" in rendered
        assert "'Y'" in rendered and "'N'" in rendered
        assert "cardinality=2" in rendered
