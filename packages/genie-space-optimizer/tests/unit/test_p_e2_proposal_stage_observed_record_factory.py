"""P-E2 — proposal-stage forbidden-AG observe-only record + marker."""
from __future__ import annotations


def test_reason_code_registered():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    members = {rc.value for rc in ReasonCode}
    assert "proposal_stage_forbidden_ag_observed" in members


def test_flag_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", raising=False)
    from genie_space_optimizer.common.config import (
        proposal_stage_forbidden_ag_observed_enabled,
    )
    assert proposal_stage_forbidden_ag_observed_enabled() is True


def test_flag_off_when_env_zero(monkeypatch):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "0")
    from genie_space_optimizer.common.config import (
        proposal_stage_forbidden_ag_observed_enabled,
    )
    assert proposal_stage_forbidden_ag_observed_enabled() is False


def test_flag_on_when_env_one(monkeypatch):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    from genie_space_optimizer.common.config import (
        proposal_stage_forbidden_ag_observed_enabled,
    )
    assert proposal_stage_forbidden_ag_observed_enabled() is True
