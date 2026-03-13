"""Tests for maturity config loading and merging logic."""

import pytest
from backend.services.maturity_config import get_default_config, merge_config


@pytest.fixture
def default_config():
    return get_default_config()


class TestDefaultConfig:
    def test_loads_successfully(self, default_config):
        assert default_config is not None

    def test_has_version(self, default_config):
        assert "version" in default_config

    def test_has_five_stages(self, default_config):
        assert len(default_config["stages"]) == 5

    def test_stage_names(self, default_config):
        names = [s["name"] for s in default_config["stages"]]
        assert names == ["Nascent", "Basic", "Developing", "Proficient", "Optimized"]

    def test_stages_cover_0_to_100(self, default_config):
        stages = default_config["stages"]
        assert stages[0]["range"][0] == 0
        assert stages[-1]["range"][1] == 100

    def test_stages_contiguous(self, default_config):
        """Each stage's low bound should be previous stage's high bound + 1."""
        stages = default_config["stages"]
        for i in range(1, len(stages)):
            assert stages[i]["range"][0] == stages[i - 1]["range"][1] + 1, (
                f"Gap between {stages[i-1]['name']} and {stages[i]['name']}"
            )

    def test_has_criteria(self, default_config):
        assert len(default_config["criteria"]) > 0

    def test_criteria_have_required_fields(self, default_config):
        required = {"id", "stage", "type", "points", "enabled", "description"}
        for c in default_config["criteria"]:
            missing = required - set(c.keys())
            assert not missing, f"Criterion {c['id']} missing fields: {missing}"

    def test_criteria_types_valid(self, default_config):
        for c in default_config["criteria"]:
            assert c["type"] in ("boolean", "count"), f"{c['id']} has invalid type: {c['type']}"

    def test_total_points_is_100(self, default_config):
        total = sum(c["points"] for c in default_config["criteria"] if c["enabled"])
        assert total == 100, f"Total points should be 100, got {total}"

    def test_deep_copy_isolation(self):
        """Modifications to one copy shouldn't affect another."""
        a = get_default_config()
        b = get_default_config()
        a["stages"][0]["name"] = "MODIFIED"
        assert b["stages"][0]["name"] == "Nascent"


class TestMergeConfig:
    def test_empty_overrides_returns_base(self, default_config):
        merged = merge_config(default_config, {})
        assert merged == default_config

    def test_override_version(self, default_config):
        merged = merge_config(default_config, {"version": 99})
        assert merged["version"] == 99

    def test_override_stages_replaces_wholesale(self, default_config):
        new_stages = [{"name": "Low", "range": [0, 50]}, {"name": "High", "range": [51, 100]}]
        merged = merge_config(default_config, {"stages": new_stages})
        assert len(merged["stages"]) == 2
        assert merged["stages"][0]["name"] == "Low"

    def test_override_criterion_points(self, default_config):
        merged = merge_config(default_config, {
            "criteria": [{"id": "tables_attached", "points": 20}],
        })
        tables = next(c for c in merged["criteria"] if c["id"] == "tables_attached")
        assert tables["points"] == 20
        # Other fields preserved
        assert tables["type"] == "boolean"
        assert tables["stage"] == "Nascent"

    def test_override_criterion_enabled(self, default_config):
        merged = merge_config(default_config, {
            "criteria": [{"id": "sql_functions", "enabled": False}],
        })
        sf = next(c for c in merged["criteria"] if c["id"] == "sql_functions")
        assert sf["enabled"] is False

    def test_override_preserves_unmodified_criteria(self, default_config):
        original_count = len(default_config["criteria"])
        merged = merge_config(default_config, {
            "criteria": [{"id": "tables_attached", "points": 20}],
        })
        assert len(merged["criteria"]) == original_count

    def test_new_criterion_appended(self, default_config):
        original_count = len(default_config["criteria"])
        merged = merge_config(default_config, {
            "criteria": [{"id": "custom_check", "stage": "Basic", "type": "boolean", "points": 5, "enabled": True, "description": "Custom"}],
        })
        assert len(merged["criteria"]) == original_count + 1
        custom = next(c for c in merged["criteria"] if c["id"] == "custom_check")
        assert custom["points"] == 5

    def test_merge_does_not_mutate_base(self, default_config):
        import copy
        original = copy.deepcopy(default_config)
        merge_config(default_config, {"criteria": [{"id": "tables_attached", "points": 99}]})
        assert default_config == original
