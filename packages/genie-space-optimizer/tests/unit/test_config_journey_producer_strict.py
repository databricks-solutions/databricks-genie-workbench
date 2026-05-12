"""Cycle 17 / Defect Plan 3 — ``GSO_JOURNEY_PRODUCER_STRICT`` accessor unit tests.

Defect Plan 3 (2026-05-12) flipped the default from OFF to ON. The
flag is now consulted via ``_flag_default_on`` which returns True
when the env var is unset or set to a non-falsy value, and False
only when explicitly set to one of ``_FALSY_VALUES`` (``0``,
``false``, ``no``, ``off``, ``''``).
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture(autouse=True)
def _reload_config():
    """Reload ``config`` after each test so env-var reads are fresh."""
    yield
    from genie_space_optimizer.common import config as _c
    importlib.reload(_c)


def test_journey_producer_strict_default_on(monkeypatch):
    """Defect Plan 3 — env var unset means default behaviour. With
    ``_flag_default_on`` the accessor returns True.
    """
    monkeypatch.delenv("GSO_JOURNEY_PRODUCER_STRICT", raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    assert config.journey_producer_strict_enabled() is True


def test_journey_producer_strict_truthy_values_keep_on(monkeypatch):
    """Defense: explicit truthy values stay on. Catches a future
    regression that inverts the accessor or swaps it back to
    ``_flag_enabled``.
    """
    from genie_space_optimizer.common import config

    for raw in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", raw)
        importlib.reload(config)
        assert config.journey_producer_strict_enabled() is True, (
            f"value {raw!r} should keep the flag on"
        )


def test_journey_producer_strict_falsy_values_disable(monkeypatch):
    """Legacy escape hatch: explicit falsy values turn the flag off.
    Used by ``test_anchor_setenv_zero_preserves_legacy_violations``
    and ``test_anchor_flag_off_clears_clustered_to_already_passing``
    to pin the pre-Defect-3 producer regime.
    """
    from genie_space_optimizer.common import config

    for raw in ("0", "false", "FALSE", "no", "off"):
        monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", raw)
        importlib.reload(config)
        assert config.journey_producer_strict_enabled() is False, (
            f"value {raw!r} should disable the flag"
        )
