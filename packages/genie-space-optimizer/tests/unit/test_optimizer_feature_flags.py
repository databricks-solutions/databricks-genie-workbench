"""Optimizer Control-Plane Hardening Plan — Task 0.

Production-locked: every helper returns ``True`` unconditionally,
regardless of env-var. The associated ``GSO_*`` env-vars are inert —
the behaviours are part of the canonical optimizer pipeline. These
tests pin the production lock so a future regression that quietly
restores env-var gating is caught.
"""

import pytest

from genie_space_optimizer.common import config as cfg


_LOCKED_HELPERS_AND_ENV = (
    ("target_aware_acceptance_enabled", "GSO_TARGET_AWARE_ACCEPTANCE"),
    ("regression_debt_invariant_enabled", "GSO_REGRESSION_DEBT_INVARIANT"),
    ("lever_qualified_patch_ids_enabled", "GSO_LEVER_QUALIFIED_PATCH_IDS"),
    ("no_causal_applyable_halt_enabled", "GSO_NO_CAUSAL_APPLYABLE_HALT"),
    ("bucket_driven_ag_selection_enabled", "GSO_BUCKET_DRIVEN_AG_SELECTION"),
    ("rca_aware_patch_cap_enabled", "GSO_RCA_AWARE_PATCH_CAP"),
    ("lever_aware_blast_radius_enabled", "GSO_LEVER_AWARE_BLAST_RADIUS"),
    ("intra_ag_proposal_dedup_enabled", "GSO_INTRA_AG_PROPOSAL_DEDUP"),
    ("shared_cause_blast_radius_enabled", "GSO_SHARED_CAUSE_BLAST_RADIUS"),
    (
        "doa_selected_proposal_signature_enabled",
        "GSO_DOA_SELECTED_PROPOSAL_SIGNATURE",
    ),
    (
        "question_shape_lever_preference_enabled",
        "GSO_QUESTION_SHAPE_LEVER_PREFERENCE",
    ),
    (
        "force_structural_synthesis_on_lever5_drop_enabled",
        "GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP",
    ),
)


@pytest.mark.parametrize("helper_name,env_name", _LOCKED_HELPERS_AND_ENV)
def test_helper_returns_true_with_env_unset(
    monkeypatch, helper_name, env_name,
):
    monkeypatch.delenv(env_name, raising=False)
    helper = getattr(cfg, helper_name)
    assert helper() is True


@pytest.mark.parametrize("helper_name,env_name", _LOCKED_HELPERS_AND_ENV)
@pytest.mark.parametrize("value", ["0", "false", "no", "off", "", "1", "on"])
def test_helper_ignores_env_var(
    monkeypatch, helper_name, env_name, value,
):
    """Production-lock: the env-var is inert. Setting it to any value
    (truthy, falsy, or empty) must not change the helper's return."""
    monkeypatch.setenv(env_name, value)
    helper = getattr(cfg, helper_name)
    assert helper() is True
