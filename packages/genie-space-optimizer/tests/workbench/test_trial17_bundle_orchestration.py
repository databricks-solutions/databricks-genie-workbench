"""Trial 17 Step 5 — multi-lever bundle orchestration workbench.

Pins the bundle contract documented in
``bundle_orchestration.py`` without requiring a live SM run:

1. Proposals with empty ``bundle_id`` are singletons (legacy path
   unchanged).
2. Proposals sharing a non-empty ``bundle_id`` are grouped in
   emission order.
3. Bundle acceptance is gated on the last step's
   ``post_apply_score`` vs the bundle's initial pre-apply score.
4. The mid-bundle abort rule terminates early on
   ``target_unchanged`` / ``apply_failed``.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.bundle_orchestration import (
    Bundle,
    BundleStep,
    bundle_accepted,
    group_proposals_by_bundle,
    iter_bundle_steps,
    should_terminate_bundle_early,
    trial17_bundles_enabled,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def _proposal(
    intent_suffix: str,
    *,
    patch_type: PatchType = PatchType.ADD_INSTRUCTION,
    selected_lever: str = "lever-5",
    bundle_id: str = "",
) -> RepairProposal:
    return RepairProposal(
        intent_id=f"intent_H001_AG3_{intent_suffix}",
        intent_name=f"intent_{intent_suffix}",
        intent_description="x",
        repair_shape=RepairShape.OTHER,
        patch_type=patch_type,
        rationale="x",
        confidence="medium",
        patch_body={"instruction_text": f"Always do {intent_suffix}."},
        blame_set=(),
        selected_lever=selected_lever,
        bundle_id=bundle_id,
    )


def test_empty_bundle_id_proposals_are_singletons():
    """Trial 17 Step 5 — legacy single-proposal path unchanged.

    Proposals carrying ``bundle_id=""`` are returned as singletons;
    the orchestrator must not invent a synthetic bundle for them.
    """
    proposals = [
        _proposal("001"),
        _proposal("002"),
    ]
    bundles, singletons = group_proposals_by_bundle(proposals)
    assert bundles == []
    assert len(singletons) == 2
    assert all(p.bundle_id == "" for p in singletons)


def test_shared_bundle_id_groups_proposals_in_emission_order():
    """Proposals sharing a non-empty bundle_id are grouped, preserving
    LLM-declared emission order."""
    a = _proposal("001", bundle_id="bundle_A")
    b = _proposal("002", bundle_id="bundle_A")
    c = _proposal("003", bundle_id="bundle_A")
    bundles, singletons = group_proposals_by_bundle([a, b, c])
    assert singletons == []
    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.bundle_id == "bundle_A"
    assert bundle.size == 3
    assert [s.order for s in bundle.steps] == [0, 1, 2]
    assert [s.proposal.intent_id for s in bundle.steps] == [
        "intent_H001_AG3_001",
        "intent_H001_AG3_002",
        "intent_H001_AG3_003",
    ]


def test_mixed_batch_partitions_correctly():
    """Mixed batch: some singletons, two bundles — partition stable."""
    s1 = _proposal("001")
    a1 = _proposal("002", bundle_id="bundle_A")
    s2 = _proposal("003")
    a2 = _proposal("004", bundle_id="bundle_A")
    b1 = _proposal("005", bundle_id="bundle_B")
    bundles, singletons = group_proposals_by_bundle([s1, a1, s2, a2, b1])
    assert [p.intent_id for p in singletons] == [
        "intent_H001_AG3_001",
        "intent_H001_AG3_003",
    ]
    assert [b.bundle_id for b in bundles] == ["bundle_A", "bundle_B"]
    assert bundles[0].size == 2
    assert bundles[1].size == 1
    assert iter_bundle_steps(bundles[0]).__iter__ is not None


def test_bundle_acceptance_requires_strict_improvement_on_last_step():
    """Bundle accepted iff last-step post_apply > initial_pre_apply.

    Equal or worse scores are rejected (consistent with the single-
    proposal ``target_unchanged = post <= pre`` contract).
    """
    assert bundle_accepted(
        initial_pre_apply_score=0.40, last_post_apply_score=0.55,
    )
    assert not bundle_accepted(
        initial_pre_apply_score=0.40, last_post_apply_score=0.40,
    )
    assert not bundle_accepted(
        initial_pre_apply_score=0.40, last_post_apply_score=0.30,
    )


def test_bundle_acceptance_supports_epsilon_threshold():
    """``epsilon`` lets the caller require a minimum delta (matches
    the existing single-proposal acceptance gate idiom)."""
    assert not bundle_accepted(
        initial_pre_apply_score=0.40,
        last_post_apply_score=0.405,
        epsilon=0.01,
    )
    assert bundle_accepted(
        initial_pre_apply_score=0.40,
        last_post_apply_score=0.42,
        epsilon=0.01,
    )


def test_should_terminate_bundle_early_on_target_unchanged():
    assert should_terminate_bundle_early(
        step_target_unchanged=True, step_apply_failed=False,
    )


def test_should_terminate_bundle_early_on_apply_failure():
    assert should_terminate_bundle_early(
        step_target_unchanged=False, step_apply_failed=True,
    )


def test_should_not_terminate_bundle_when_step_accepted():
    """When neither failure mode trips, the bundle continues to the
    next step."""
    assert not should_terminate_bundle_early(
        step_target_unchanged=False, step_apply_failed=False,
    )


def test_trial17_bundles_flag_default_on(monkeypatch):
    """Default is ON (Trial 17.1) — env unset means bundles active.

    Legacy single-proposal callers still see no behavior change because
    proposals with ``bundle_id == ""`` are never grouped by
    ``group_proposals_by_bundle``; the flag only matters for clusters
    that emit a bundle.
    """
    monkeypatch.delenv("GSO_TRIAL17_BUNDLES", raising=False)
    assert trial17_bundles_enabled() is True


@pytest.mark.parametrize("on_value", ["1", "true", "yes", "on", "TRUE", ""])
def test_trial17_bundles_flag_on_via_env(monkeypatch, on_value):
    monkeypatch.setenv("GSO_TRIAL17_BUNDLES", on_value)
    assert trial17_bundles_enabled() is True


@pytest.mark.parametrize("off_value", ["0", "false", "no", "off", "OFF", "False"])
def test_trial17_bundles_flag_off_via_env(monkeypatch, off_value):
    """Explicit opt-out keeps the legacy single-proposal path intact."""
    monkeypatch.setenv("GSO_TRIAL17_BUNDLES", off_value)
    assert trial17_bundles_enabled() is False


def test_bundle_step_carries_proposal_and_order():
    """Sanity check the typed carrier."""
    p = _proposal("001", bundle_id="bundle_A")
    step = BundleStep(bundle_id="bundle_A", order=0, proposal=p)
    assert step.bundle_id == "bundle_A"
    assert step.order == 0
    assert step.proposal is p


def test_bundle_last_step_returns_last_emitted_step():
    a = _proposal("001", bundle_id="bundle_A")
    b = _proposal("002", bundle_id="bundle_A")
    bundle = Bundle(
        bundle_id="bundle_A",
        steps=(
            BundleStep("bundle_A", 0, a),
            BundleStep("bundle_A", 1, b),
        ),
    )
    assert bundle.last_step().proposal is b
