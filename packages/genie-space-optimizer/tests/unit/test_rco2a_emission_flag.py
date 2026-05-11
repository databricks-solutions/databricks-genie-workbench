"""RCO-2a Task 8 — emission flag accessor tests."""
from __future__ import annotations


def test_emission_flag_default_is_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", raising=False)
    from genie_space_optimizer.common.config import (
        gso_contract_health_summary_enabled,
    )
    assert gso_contract_health_summary_enabled() is True


def test_emission_flag_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "0")
    from genie_space_optimizer.common.config import (
        gso_contract_health_summary_enabled,
    )
    assert gso_contract_health_summary_enabled() is False
