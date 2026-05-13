"""Tests for kit_level_gate (Phase 2 Action 2.2)."""

from __future__ import annotations

from genie_space_optimizer.optimization.kit_safety import (
    KitSafetyPolicy,
    KitSafetySummary,
)
from genie_space_optimizer.optimization.repair_kit import RepairKit


def _kit() -> RepairKit:
    return RepairKit(
        kit_id="kit_test",
        repair_archetype="plural_top_n_collapse",
        target_qids=("gs_026",),
        expected_causal_effect="ORDER BY metric DESC",
        patches=(),
    )


def _summary(
    *,
    union_passing_dependents: tuple[str, ...] = (),
    risk_class: str = "low",
    scoped_alternative_available: bool = False,
    expected_causal_effect: str = "ORDER BY metric DESC",
    target_qids: tuple[str, ...] = ("gs_026",),
    co_beneficiary_qids: tuple[str, ...] = (),
) -> KitSafetySummary:
    return KitSafetySummary(
        kit_id="kit_test",
        repair_archetype="plural_top_n_collapse",
        union_target_objects=(),
        union_passing_dependents=union_passing_dependents,
        required_companions=(),
        expected_causal_effect=expected_causal_effect,
        target_qids=target_qids,
        risk_class=risk_class,
        co_beneficiary_qids=co_beneficiary_qids,
        scoped_alternative_available=scoped_alternative_available,
    )


def test_gate_accepts_low_risk_kit_within_dependents_threshold() -> None:
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(union_passing_dependents=("gs_a",), risk_class="low"),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is True
    assert decision.reason == "kit_safe"


def test_gate_rejects_when_union_passing_dependents_exceeds_threshold() -> None:
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            union_passing_dependents=tuple(f"gs_{i}" for i in range(20)),
            risk_class="low",
        ),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is False
    assert decision.reason == "union_passing_dependents_exceeds_threshold"


def test_gate_rejects_high_risk_when_no_scoped_alternative() -> None:
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(risk_class="high", scoped_alternative_available=False),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is False
    assert decision.reason == "high_risk_no_scoped_alternative"


def test_gate_accepts_high_risk_when_scoped_alternative_available() -> None:
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(risk_class="high", scoped_alternative_available=True),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is True
    assert decision.reason == "kit_safe"
    # Risk should be effectively downgraded for telemetry; the gate
    # records the downgrade in ``effective_risk_class``.
    assert decision.effective_risk_class == "medium"


def test_gate_rejects_when_expected_effect_does_not_target_any_cluster_qid() -> None:
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    kit = RepairKit(
        kit_id="kit_test",
        repair_archetype="plural_top_n_collapse",
        target_qids=("gs_999",),  # No overlap with cluster_target_qids below
        expected_causal_effect="ORDER BY metric DESC",
        patches=(),
    )
    decision = kit_level_gate(
        kit=kit,
        summary=_summary(),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is False
    assert decision.reason == "expected_effect_misses_target_qids"


def test_gate_threshold_is_inclusive_at_boundary() -> None:
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            union_passing_dependents=tuple(f"gs_{i}" for i in range(15)),
            risk_class="low",
        ),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
        cluster_target_qids=("gs_026",),
    )
    # 15 dependents exactly at threshold → accept (strict >).
    assert decision.accepted is True


def test_gate_co_beneficiary_downgrade_high_to_medium_when_threshold_met() -> None:
    """Phase 2 Action 2.2 — when ``co_beneficiary_qids`` count meets the
    policy threshold (default 5), the gate's ``effective_risk_class``
    drops one tier. A high-risk kit with 5+ co-beneficiaries effectively
    becomes medium-risk and clears the gate without needing a scoped
    alternative."""
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            risk_class="high",
            co_beneficiary_qids=tuple(f"gs_co_{i}" for i in range(5)),
            scoped_alternative_available=False,
        ),
        policy=KitSafetyPolicy(
            passing_dependents_threshold=15,
            co_beneficiary_downgrade_threshold=5,
        ),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is True
    assert decision.reason == "kit_safe"
    assert decision.effective_risk_class == "medium"
    assert decision.co_beneficiary_count == 5


def test_gate_co_beneficiary_downgrade_does_not_fire_below_threshold() -> None:
    """Below the threshold (default 5), no downgrade. A high-risk kit
    with only 4 co-beneficiaries still needs a scoped alternative."""
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            risk_class="high",
            co_beneficiary_qids=tuple(f"gs_co_{i}" for i in range(4)),
            scoped_alternative_available=False,
        ),
        policy=KitSafetyPolicy(
            passing_dependents_threshold=15,
            co_beneficiary_downgrade_threshold=5,
        ),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is False
    assert decision.reason == "high_risk_no_scoped_alternative"


def test_gate_co_beneficiary_and_scoped_downgrades_compose() -> None:
    """Both downgrades apply independently and floor at ``low``. A
    high-risk kit with both 5+ co-beneficiaries AND a scoped variant
    drops two tiers (high → medium → low)."""
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            risk_class="high",
            co_beneficiary_qids=tuple(f"gs_co_{i}" for i in range(8)),
            scoped_alternative_available=True,
        ),
        policy=KitSafetyPolicy(
            passing_dependents_threshold=15,
            co_beneficiary_downgrade_threshold=5,
        ),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is True
    assert decision.effective_risk_class == "low"


def test_gate_co_beneficiary_downgrade_floors_at_low() -> None:
    """A medium-risk kit with strong co-beneficiary support drops to
    low; a low-risk kit stays low (floor enforced)."""
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    medium_decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            risk_class="medium",
            co_beneficiary_qids=tuple(f"gs_co_{i}" for i in range(10)),
        ),
        policy=KitSafetyPolicy(
            passing_dependents_threshold=15,
            co_beneficiary_downgrade_threshold=5,
        ),
        cluster_target_qids=("gs_026",),
    )
    assert medium_decision.effective_risk_class == "low"

    low_decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            risk_class="low",
            co_beneficiary_qids=tuple(f"gs_co_{i}" for i in range(10)),
        ),
        policy=KitSafetyPolicy(
            passing_dependents_threshold=15,
            co_beneficiary_downgrade_threshold=5,
        ),
        cluster_target_qids=("gs_026",),
    )
    assert low_decision.effective_risk_class == "low"


def test_gate_records_co_beneficiary_count_for_postmortem() -> None:
    """``co_beneficiary_count`` is informational and is populated on
    every decision (accept or reject) so postmortems can aggregate
    soft-evidence breadth alongside gate outcomes."""
    from genie_space_optimizer.optimization.kit_safety import kit_level_gate

    decision = kit_level_gate(
        kit=_kit(),
        summary=_summary(
            risk_class="low",
            co_beneficiary_qids=("gs_co_1", "gs_co_2", "gs_co_3"),
        ),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
        cluster_target_qids=("gs_026",),
    )
    assert decision.accepted is True
    assert decision.co_beneficiary_count == 3
