"""Optimizer Control-Plane Hardening Plan — Task 0.

Feature-flag helpers in common.config now default ON for cycle-9
deploy. Each helper returns True unless the env-var is explicitly set
to a falsy value (``0``/``false``/``no``/``off``).
"""

import pytest

from genie_space_optimizer.common import config as cfg


def test_feature_flag_defaults_on(monkeypatch):
    for env in (
        "GSO_TARGET_AWARE_ACCEPTANCE",
        "GSO_NO_CAUSAL_APPLYABLE_HALT",
        "GSO_BUCKET_DRIVEN_AG_SELECTION",
        "GSO_RCA_AWARE_PATCH_CAP",
        "GSO_LEVER_AWARE_BLAST_RADIUS",
    ):
        monkeypatch.delenv(env, raising=False)
    assert cfg.target_aware_acceptance_enabled() is True
    assert cfg.no_causal_applyable_halt_enabled() is True
    assert cfg.bucket_driven_ag_selection_enabled() is True
    assert cfg.rca_aware_patch_cap_enabled() is True
    assert cfg.lever_aware_blast_radius_enabled() is True


@pytest.mark.parametrize(
    "value,expected",
    [
        # Explicit truthy values — flag stays on.
        ("1", True),
        ("true", True),
        ("yes", True),
        ("on", True),
        # Empty / unrecognized values fall through to the default-on.
        ("", True),
        # Explicit falsy values — disable.
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
    ],
)
def test_feature_flag_env_parsing(monkeypatch, value, expected):
    monkeypatch.setenv("GSO_TARGET_AWARE_ACCEPTANCE", value)
    assert cfg.target_aware_acceptance_enabled() is expected


def test_each_flag_can_be_individually_disabled(monkeypatch):
    """Per-flag disable: setting one env-var to ``0`` only turns off
    that helper; the other four remain on at their default."""
    for env in (
        "GSO_TARGET_AWARE_ACCEPTANCE",
        "GSO_NO_CAUSAL_APPLYABLE_HALT",
        "GSO_BUCKET_DRIVEN_AG_SELECTION",
        "GSO_RCA_AWARE_PATCH_CAP",
        "GSO_LEVER_AWARE_BLAST_RADIUS",
    ):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv("GSO_RCA_AWARE_PATCH_CAP", "0")
    assert cfg.rca_aware_patch_cap_enabled() is False
    assert cfg.target_aware_acceptance_enabled() is True
    assert cfg.no_causal_applyable_halt_enabled() is True
    assert cfg.bucket_driven_ag_selection_enabled() is True
    assert cfg.lever_aware_blast_radius_enabled() is True
