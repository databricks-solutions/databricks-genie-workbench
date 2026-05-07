"""Cycle 10 — config-flag accessors for the seven workstreams."""
from __future__ import annotations


def test_rca_ungrounded_records_enabled_default_on(monkeypatch):
    monkeypatch.delenv("GSO_RCA_UNGROUNDED_RECORDS_ENABLED", raising=False)
    from genie_space_optimizer.common.config import (
        rca_ungrounded_records_enabled,
    )
    assert rca_ungrounded_records_enabled() is True


def test_rca_ungrounded_records_enabled_off(monkeypatch):
    monkeypatch.setenv("GSO_RCA_UNGROUNDED_RECORDS_ENABLED", "0")
    from genie_space_optimizer.common.config import (
        rca_ungrounded_records_enabled,
    )
    assert rca_ungrounded_records_enabled() is False


def test_ag_levers_union_recommended_default_on(monkeypatch):
    monkeypatch.delenv("GSO_AG_LEVERS_UNION_RECOMMENDED", raising=False)
    from genie_space_optimizer.common.config import (
        ag_levers_union_recommended_enabled,
    )
    assert ag_levers_union_recommended_enabled() is True


def test_ag_levers_union_recommended_off(monkeypatch):
    monkeypatch.setenv("GSO_AG_LEVERS_UNION_RECOMMENDED", "0")
    from genie_space_optimizer.common.config import (
        ag_levers_union_recommended_enabled,
    )
    assert ag_levers_union_recommended_enabled() is False


def test_lever6_force_typed_outcomes_default_on(monkeypatch):
    monkeypatch.delenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", raising=False)
    from genie_space_optimizer.common.config import (
        lever6_force_typed_outcomes_enabled,
    )
    assert lever6_force_typed_outcomes_enabled() is True


def test_lever6_force_typed_outcomes_off(monkeypatch):
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "0")
    from genie_space_optimizer.common.config import (
        lever6_force_typed_outcomes_enabled,
    )
    assert lever6_force_typed_outcomes_enabled() is False


def test_l6_narrow_replacement_patch_aware_default_on(monkeypatch):
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_PATCH_AWARE", raising=False)
    from genie_space_optimizer.common.config import (
        l6_narrow_replacement_patch_aware_enabled,
    )
    assert l6_narrow_replacement_patch_aware_enabled() is True


def test_l6_narrow_replacement_patch_aware_off(monkeypatch):
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_PATCH_AWARE", "0")
    from genie_space_optimizer.common.config import (
        l6_narrow_replacement_patch_aware_enabled,
    )
    assert l6_narrow_replacement_patch_aware_enabled() is False


def test_doa_fingerprint_patch_body_match_default_on(monkeypatch):
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_PATCH_BODY_MATCH", raising=False)
    from genie_space_optimizer.common.config import (
        doa_fingerprint_patch_body_match_enabled,
    )
    assert doa_fingerprint_patch_body_match_enabled() is True


def test_doa_fingerprint_patch_body_match_off(monkeypatch):
    monkeypatch.setenv("GSO_DOA_FINGERPRINT_PATCH_BODY_MATCH", "0")
    from genie_space_optimizer.common.config import (
        doa_fingerprint_patch_body_match_enabled,
    )
    assert doa_fingerprint_patch_body_match_enabled() is False


def test_plateau_counts_quarantined_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PLATEAU_COUNTS_QUARANTINED", raising=False)
    from genie_space_optimizer.common.config import (
        plateau_counts_quarantined_enabled,
    )
    assert plateau_counts_quarantined_enabled() is True


def test_plateau_counts_quarantined_off(monkeypatch):
    monkeypatch.setenv("GSO_PLATEAU_COUNTS_QUARANTINED", "0")
    from genie_space_optimizer.common.config import (
        plateau_counts_quarantined_enabled,
    )
    assert plateau_counts_quarantined_enabled() is False


def test_proposal_trace_one_source_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PROPOSAL_TRACE_ONE_SOURCE", raising=False)
    from genie_space_optimizer.common.config import (
        proposal_trace_one_source_enabled,
    )
    assert proposal_trace_one_source_enabled() is True


def test_proposal_trace_one_source_off(monkeypatch):
    monkeypatch.setenv("GSO_PROPOSAL_TRACE_ONE_SOURCE", "0")
    from genie_space_optimizer.common.config import (
        proposal_trace_one_source_enabled,
    )
    assert proposal_trace_one_source_enabled() is False


def test_gso_run_manifest_v2_enabled_default_on(monkeypatch):
    monkeypatch.delenv("GSO_RUN_MANIFEST_V2_ENABLED", raising=False)
    from genie_space_optimizer.common.config import (
        gso_run_manifest_v2_enabled,
    )
    assert gso_run_manifest_v2_enabled() is True


def test_gso_run_manifest_v2_enabled_off(monkeypatch):
    monkeypatch.setenv("GSO_RUN_MANIFEST_V2_ENABLED", "0")
    from genie_space_optimizer.common.config import (
        gso_run_manifest_v2_enabled,
    )
    assert gso_run_manifest_v2_enabled() is False
