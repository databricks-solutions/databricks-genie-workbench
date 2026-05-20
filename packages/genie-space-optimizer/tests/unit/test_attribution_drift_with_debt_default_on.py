"""Plan 9 Task 11.C — attribution_drift_with_debt_enabled defaults ON."""

from genie_space_optimizer.common.config import (
    attribution_drift_with_debt_enabled,
)


def test_attribution_drift_with_debt_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", raising=False)
    assert attribution_drift_with_debt_enabled() is True


def test_attribution_drift_with_debt_explicit_off_disables(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "0")
    assert attribution_drift_with_debt_enabled() is False


def test_attribution_drift_with_debt_explicit_on_enables(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "1")
    assert attribution_drift_with_debt_enabled() is True
