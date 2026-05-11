"""Cycle 17 Task 1 — `GSO_JOURNEY_PRODUCER_STRICT` accessor unit tests."""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture(autouse=True)
def _reload_config():
    """Reload ``config`` after each test so env-var reads are fresh."""
    yield
    from genie_space_optimizer.common import config as _c
    importlib.reload(_c)


def test_journey_producer_strict_default_off(monkeypatch):
    monkeypatch.delenv("GSO_JOURNEY_PRODUCER_STRICT", raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    assert config.journey_producer_strict_enabled() is False


def test_journey_producer_strict_truthy_values_enable(monkeypatch):
    from genie_space_optimizer.common import config

    for raw in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", raw)
        importlib.reload(config)
        assert config.journey_producer_strict_enabled() is True, (
            f"value {raw!r} should enable the flag"
        )


def test_journey_producer_strict_falsy_values_disable(monkeypatch):
    from genie_space_optimizer.common import config

    for raw in ("0", "false", "FALSE", "no", "off", ""):
        monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", raw)
        importlib.reload(config)
        assert config.journey_producer_strict_enabled() is False, (
            f"value {raw!r} should disable the flag"
        )
