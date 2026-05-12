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


def test_record_factory_shape_for_cluster_driven_synthesis():
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_stage_forbidden_ag_observed_record,
    )
    rec = proposal_stage_forbidden_ag_observed_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        root_cause="missing_filter",
        call_site="cluster_driven_synthesis",
        match_axis="root_cause",
        cluster_signature="sig_abc123",
        lever_set=(5, 6),
    )
    d = rec.to_dict()
    assert d["reason_code"] == "proposal_stage_forbidden_ag_observed"
    assert d["decision_type"] == "proposal_generated"
    assert d["outcome"] == "unresolved"
    assert d["ag_id"] == "AG_X"
    assert d["cluster_id"] == "H004"
    assert d["root_cause"] == "missing_filter"
    refs = d.get("evidence_refs") or ()
    assert "ag:AG_X" in refs
    assert "cluster:H004" in refs
    assert "signature:sig_abc123" in refs
    assert "call_site:cluster_driven_synthesis" in refs
    assert "match_axis:root_cause" in refs
    metrics = dict(d.get("metrics") or {})
    assert metrics.get("call_site") == "cluster_driven_synthesis"
    assert metrics.get("match_axis") == "root_cause"
    assert sorted(metrics.get("lever_set") or ()) == [5, 6]


def test_record_factory_shape_for_force_lever6():
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_stage_forbidden_ag_observed_record,
    )
    rec = proposal_stage_forbidden_ag_observed_record(
        run_id="r1",
        iteration=3,
        ag_id="AG_Y",
        cluster_id="H007",
        root_cause="missing_join",
        call_site="force_lever6",
        match_axis="cluster_signature",
        cluster_signature="sig_xyz789",
        lever_set=(6,),
    )
    d = rec.to_dict()
    metrics = dict(d.get("metrics") or {})
    assert metrics["call_site"] == "force_lever6"
    assert metrics["match_axis"] == "cluster_signature"


def test_record_factory_rejects_invalid_call_site():
    """Closed-vocabulary guard — ``call_site`` must be one of the two
    known sub-AG sites."""
    import pytest
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_stage_forbidden_ag_observed_record,
    )
    with pytest.raises(ValueError, match="call_site"):
        proposal_stage_forbidden_ag_observed_record(
            run_id="r1", iteration=1, ag_id="AG_X", cluster_id="H004",
            root_cause="rc", call_site="not_a_real_call_site",
            match_axis="root_cause", cluster_signature="", lever_set=(),
        )


def test_record_factory_rejects_invalid_match_axis():
    """Closed-vocabulary guard — ``match_axis`` must be one of
    {root_cause, cluster_signature, both}."""
    import pytest
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_stage_forbidden_ag_observed_record,
    )
    with pytest.raises(ValueError, match="match_axis"):
        proposal_stage_forbidden_ag_observed_record(
            run_id="r1", iteration=1, ag_id="AG_X", cluster_id="H004",
            root_cause="rc", call_site="force_lever6",
            match_axis="invalid_axis", cluster_signature="", lever_set=(),
        )
