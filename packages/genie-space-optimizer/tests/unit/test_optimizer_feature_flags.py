"""Optimizer Control-Plane Hardening Plan — Task 0.

Feature-flag helpers in common.config must default off and parse
common truthy/falsy env-var values consistently.
"""

import pytest

from genie_space_optimizer.common import config as cfg


def test_feature_flag_defaults_off(monkeypatch):
    for env in (
        "GSO_TARGET_AWARE_ACCEPTANCE",
        "GSO_NO_CAUSAL_APPLYABLE_HALT",
        "GSO_BUCKET_DRIVEN_AG_SELECTION",
        "GSO_RCA_AWARE_PATCH_CAP",
        "GSO_LEVER_AWARE_BLAST_RADIUS",
    ):
        monkeypatch.delenv(env, raising=False)
    assert cfg.target_aware_acceptance_enabled() is False
    assert cfg.no_causal_applyable_halt_enabled() is False
    assert cfg.bucket_driven_ag_selection_enabled() is False
    assert cfg.rca_aware_patch_cap_enabled() is False
    assert cfg.lever_aware_blast_radius_enabled() is False


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1", True),
        ("true", True),
        ("yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("", False),
    ],
)
def test_feature_flag_env_parsing(monkeypatch, value, expected):
    monkeypatch.setenv("GSO_TARGET_AWARE_ACCEPTANCE", value)
    assert cfg.target_aware_acceptance_enabled() is expected
