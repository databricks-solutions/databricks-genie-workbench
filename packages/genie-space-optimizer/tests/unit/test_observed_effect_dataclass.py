"""Phase C Task 3 — ObservedEffect frozen dataclass + helper.

ObservedEffect closes the loop between intended fix
(``RcaExecutionPlan`` / ``ExpectedFix``) and post-eval delta. This
test pins:

* ``ObservedEffect`` is frozen and carries the canonical post-eval
  signal: pre/post passing, IQ delta, arbiter-verdict change, judge
  failures.
* ``build_observed_effects`` produces one ObservedEffect per applied
  patch from an ``apply_log`` + pre/post passing qid sets.
* Empty input yields an empty list.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 3.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest


def test_observed_effect_is_frozen() -> None:
    from genie_space_optimizer.optimization.rca_execution import ObservedEffect

    eff = ObservedEffect(
        iteration=1,
        ag_id="AG1",
        proposal_id="P001",
        pre_passing_qids=("q1", "q2"),
        post_passing_qids=("q1", "q2", "q3"),
        iq_delta=0.05,
        arbiter_verdict_change="fail->pass",
        judge_failure_delta=-1,
    )
    with pytest.raises(FrozenInstanceError):
        eff.iq_delta = 0.10  # type: ignore[misc]


def test_build_observed_effects_one_per_applied_patch() -> None:
    from genie_space_optimizer.optimization.rca_execution import (
        ObservedEffect,
        build_observed_effects,
    )

    apply_log = {
        "applied": [
            {
                "patch": {
                    "proposal_id": "P001",
                    "ag_id": "AG1",
                },
            },
            {
                "patch": {
                    "proposal_id": "P002",
                    "ag_id": "AG1",
                },
            },
        ]
    }
    effects = build_observed_effects(
        iteration=2,
        ag_id="AG1",
        apply_log=apply_log,
        pre_passing_qids=("q1",),
        post_passing_qids=("q1", "q2"),
        pre_iq=0.50,
        post_iq=0.60,
        arbiter_verdict_change="hold",
        pre_judge_failures=3,
        post_judge_failures=2,
    )
    assert len(effects) == 2
    assert all(isinstance(e, ObservedEffect) for e in effects)
    assert effects[0].proposal_id == "P001"
    assert effects[1].proposal_id == "P002"
    # Both effects share the same iteration-level signal (all patches in
    # one AG see the same pre/post snapshot).
    for e in effects:
        assert e.iteration == 2
        assert e.ag_id == "AG1"
        assert e.pre_passing_qids == ("q1",)
        assert e.post_passing_qids == ("q1", "q2")
        assert e.iq_delta == pytest.approx(0.10)
        assert e.arbiter_verdict_change == "hold"
        assert e.judge_failure_delta == -1


def test_build_observed_effects_empty_apply_log_yields_empty_list() -> None:
    from genie_space_optimizer.optimization.rca_execution import (
        build_observed_effects,
    )

    effects = build_observed_effects(
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        apply_log={"applied": []},
        pre_passing_qids=(),
        post_passing_qids=(),
        pre_iq=0.0,
        post_iq=0.0,
        arbiter_verdict_change="",
        pre_judge_failures=0,
        post_judge_failures=0,
    )
    assert effects == []


def test_build_observed_effects_handles_missing_proposal_id() -> None:
    """Defensive: applier rows occasionally omit ``proposal_id`` (e.g.
    legacy fixture rows). Producer must skip those without crashing
    so the ``ObservedEffect`` list stays a faithful index of the
    proposals that actually applied with attribution."""
    from genie_space_optimizer.optimization.rca_execution import (
        build_observed_effects,
    )

    effects = build_observed_effects(
        iteration=3,
        ag_id="AG1",
        apply_log={
            "applied": [
                {"patch": {"proposal_id": "P001", "ag_id": "AG1"}},
                {"patch": {"ag_id": "AG1"}},  # missing proposal_id
            ]
        },
        pre_passing_qids=("q1",),
        post_passing_qids=("q1",),
        pre_iq=0.5,
        post_iq=0.5,
        arbiter_verdict_change="hold",
        pre_judge_failures=0,
        post_judge_failures=0,
    )
    assert len(effects) == 1
    assert effects[0].proposal_id == "P001"
