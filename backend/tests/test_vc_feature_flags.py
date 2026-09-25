"""A5: FeatureFlags.from_config (offline)."""

import pytest

from backend.services.version_control.platform.feature_flags import FeatureFlags


def test_from_config_empty_is_all_off():
    flags = FeatureFlags.from_config({})
    assert all(value is False for value in flags.registry.values())


def test_from_config_defaults_missing_switches_to_false():
    flags = FeatureFlags.from_config({"vc_writes_enabled": True, "vc_restore_enabled": True})
    assert flags.enabled("vc_restore_enabled") is True
    assert flags.vc_promotion_enabled is False
    assert flags.vc_optimizer_apply_enabled is False


def test_from_config_ignores_unknown_keys():
    flags = FeatureFlags.from_config({"vc_writes_enabled": True, "unrelated": True})
    assert flags.vc_writes_enabled is True
    assert "unrelated" not in flags.registry


def test_from_config_requires_booleans():
    with pytest.raises(TypeError):
        FeatureFlags.from_config({"vc_writes_enabled": "true"})


def test_from_config_write_switch_still_requires_master():
    flags = FeatureFlags.from_config({"vc_restore_enabled": True})
    # master vc_writes_enabled off => the write switch is not effectively enabled.
    assert flags.enabled("vc_restore_enabled") is False
