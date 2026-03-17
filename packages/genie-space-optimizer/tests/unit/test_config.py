"""Unit tests for genie_space_optimizer.common.config constants."""

from __future__ import annotations

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    BENCHMARK_CATEGORIES,
    CONFLICT_RULES,
    DEFAULT_LEVER_ORDER,
    DEFAULT_THRESHOLDS,
    FAILURE_TAXONOMY,
    LEVER_NAMES,
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    LOW_RISK_PATCHES,
    MAX_ITERATIONS,
    MAX_PATCH_OBJECTS,
    MEDIUM_RISK_PATCHES,
    MLFLOW_THRESHOLDS,
    PATCH_TYPES,
    RATE_LIMIT_SECONDS,
    REGRESSION_THRESHOLD,
    REPEATABILITY_TARGET,
    TABLE_ASI,
    TABLE_ITERATIONS,
    TABLE_PATCHES,
    TABLE_RUNS,
    TABLE_STAGES,
    TEMPLATE_VARIABLES,
    _LEVER_TO_PATCH_TYPE,
)


class TestQualityThresholds:
    def test_default_thresholds_are_dict(self):
        assert isinstance(DEFAULT_THRESHOLDS, dict)

    def test_all_thresholds_are_numeric(self):
        for key, val in DEFAULT_THRESHOLDS.items():
            assert isinstance(val, (int, float)), f"{key} is not numeric"

    def test_thresholds_in_0_100_range(self):
        for key, val in DEFAULT_THRESHOLDS.items():
            assert 0 <= val <= 100, f"{key}={val} out of 0-100 range"

    def test_repeatability_target_is_numeric(self):
        assert isinstance(REPEATABILITY_TARGET, (int, float))
        assert 0 <= REPEATABILITY_TARGET <= 100

    def test_mlflow_thresholds_are_0_1_scale(self):
        for key, val in MLFLOW_THRESHOLDS.items():
            assert 0 <= val <= 1.0, f"{key}={val} out of 0-1 range"
            assert "/mean" in key


class TestTimingConstants:
    def test_rate_limit_is_positive(self):
        assert RATE_LIMIT_SECONDS > 0

    def test_max_iterations_is_positive(self):
        assert MAX_ITERATIONS > 0

    def test_regression_threshold_is_positive(self):
        assert REGRESSION_THRESHOLD > 0


class TestLLMConfig:
    def test_llm_endpoint_is_string(self):
        assert isinstance(LLM_ENDPOINT, str)
        assert len(LLM_ENDPOINT) > 0

    def test_llm_temperature_range(self):
        assert 0 <= LLM_TEMPERATURE <= 2

    def test_llm_max_retries_positive(self):
        assert LLM_MAX_RETRIES > 0


class TestLeverConfig:
    def test_lever_names_covers_1_to_5(self):
        for i in range(1, 6):
            assert i in LEVER_NAMES, f"Lever {i} missing from LEVER_NAMES"

    def test_lever_names_are_strings(self):
        for k, v in LEVER_NAMES.items():
            assert isinstance(k, int)
            assert isinstance(v, str)

    def test_default_lever_order(self):
        assert isinstance(DEFAULT_LEVER_ORDER, list)
        assert len(DEFAULT_LEVER_ORDER) >= 1


class TestPatchTypes:
    def test_patch_types_is_dict(self):
        assert isinstance(PATCH_TYPES, dict)
        assert len(PATCH_TYPES) > 0

    def test_each_patch_type_has_required_fields(self):
        for name, info in PATCH_TYPES.items():
            assert "scope" in info, f"{name} missing 'scope'"
            assert "risk_level" in info, f"{name} missing 'risk_level'"
            assert "affects" in info, f"{name} missing 'affects'"

    def test_risk_patches_are_subsets(self):
        all_types = set(PATCH_TYPES.keys())
        for pt in LOW_RISK_PATCHES:
            assert pt in all_types, f"LOW_RISK '{pt}' not in PATCH_TYPES"
        for pt in MEDIUM_RISK_PATCHES:
            assert pt in all_types, f"MEDIUM_RISK '{pt}' not in PATCH_TYPES"

    def test_max_patch_objects_is_positive(self):
        assert MAX_PATCH_OBJECTS > 0

    def test_lever_to_patch_type_mapping(self):
        assert isinstance(_LEVER_TO_PATCH_TYPE, dict)
        for key, val in _LEVER_TO_PATCH_TYPE.items():
            assert isinstance(key, tuple)
            assert isinstance(val, str)


class TestConflictRules:
    def test_conflict_rules_is_list_of_tuples(self):
        assert isinstance(CONFLICT_RULES, list)
        for rule in CONFLICT_RULES:
            assert isinstance(rule, (list, tuple))
            assert len(rule) == 2


class TestFailureTaxonomy:
    def test_taxonomy_is_collection(self):
        assert isinstance(FAILURE_TAXONOMY, (dict, set, list))
        assert len(FAILURE_TAXONOMY) > 0


class TestDeltaTableNames:
    def test_table_names_are_strings(self):
        for name in [TABLE_RUNS, TABLE_STAGES, TABLE_ITERATIONS, TABLE_PATCHES, TABLE_ASI]:
            assert isinstance(name, str)
            assert len(name) > 0


class TestBenchmarkConfig:
    def test_categories_is_list(self):
        assert isinstance(BENCHMARK_CATEGORIES, list)
        assert len(BENCHMARK_CATEGORIES) > 0

    def test_template_variables(self):
        assert isinstance(TEMPLATE_VARIABLES, dict)
        assert "${catalog}" in TEMPLATE_VARIABLES
        assert "${gold_schema}" in TEMPLATE_VARIABLES


class TestApplyMode:
    def test_apply_mode_valid(self):
        assert APPLY_MODE in ("genie_config", "uc_artifact", "both")
