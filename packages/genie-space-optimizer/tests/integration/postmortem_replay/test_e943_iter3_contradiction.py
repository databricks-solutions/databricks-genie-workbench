"""Track A / A3 — e94376a3 iteration-3 candidate-lifecycle contradiction.

The e943 iteration-3 postmortem exposed a contradiction the canonical
:class:`CandidateOutcome` record did NOT close: on a NON-accepted
iteration, ``patch_survival`` recorded an applied ``add_instruction``
patch (``applied=1``), but the candidate ledger reported
``patches_applied=0`` / ``selected_proposal_id=""``. The cause is
structural: ``CandidateOutcome.ledger_overrides_for_iteration`` returns
``{}`` for any iteration that did not full-eval ACCEPT, so the applied
funnel facts were simply dropped on the floor for the most common
(non-accept) iteration shape.

This gate replays the iteration-3 state through the single-writer
:class:`CandidateLifecycle` and asserts the ledger-facing applied facts
agree with what ``patch_survival`` recorded — on a non-accepted
iteration. It is BLOCKING in ci-merge-gate.yml.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from genie_space_optimizer.optimization.candidate_outcome import (
    CandidateLifecycle,
    CandidateOutcome,
)
from genie_space_optimizer.optimization.patch_survival import (
    PatchSurvivalSnapshot,
)

FIXTURE = (
    Path(__file__).parent / "fixtures" / "e943_iter3_contradiction.json"
)


@pytest.fixture(scope="module")
def fx() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text())


def _snapshot(fx: dict[str, Any]) -> PatchSurvivalSnapshot:
    ps = fx["patch_survival"]
    return PatchSurvivalSnapshot(
        ag_id=str(ps["ag_id"]),
        proposed=list(ps.get("proposed") or []),
        normalized=list(ps.get("normalized") or []),
        applyable=list(ps.get("applyable") or []),
        capped=list(ps.get("capped") or []),
        applied=list(ps.get("applied") or []),
    )


def test_accept_only_outcome_drops_the_applied_facts(fx) -> None:
    """Documents the bug: the accept-only CandidateOutcome record yields
    NO ledger applied facts on a non-accepted iteration, even though a
    patch was applied. This is why the ledger reported patches_applied=0
    while patch_survival showed applied=1."""
    candidate = CandidateOutcome.from_full_eval_payload(fx["full_eval_payload"])
    assert candidate.accepted is False
    # The accept-only projection returns {} → the ledger keeps its
    # unassigned 0 / "" defaults, contradicting patch_survival.
    assert candidate.ledger_overrides_for_iteration(int(fx["iteration"])) == {}


def test_lifecycle_reports_applied_facts_on_non_accepted_iteration(fx) -> None:
    """The single-writer CandidateLifecycle reads the SAME applied
    funnel patch_survival recorded — regardless of accept — so the
    candidate ledger can no longer contradict patch_survival."""
    exp = fx["expected"]
    lifecycle = CandidateLifecycle.from_patch_survival(
        iteration=int(fx["iteration"]),
        per_ag_snapshots=[_snapshot(fx)],
    )
    assert lifecycle.patches_applied == exp["patches_applied"], (
        "A3: lifecycle.patches_applied must equal the patch_survival "
        f"applied count; got {lifecycle.patches_applied}"
    )
    assert lifecycle.selected_proposal_id == exp["selected_proposal_id"], (
        "A3: lifecycle.selected_proposal_id must be the applied patch's "
        f"proposal id; got {lifecycle.selected_proposal_id!r}"
    )


def test_lifecycle_floor_fills_ledger_when_no_accept(fx) -> None:
    """The ledger floor raises patches_applied / selected_proposal_id to
    the patch_survival-recorded values when the iteration locals carry
    the unassigned defaults (the e943 iter-3 shape)."""
    exp = fx["expected"]
    lifecycle = CandidateLifecycle.from_patch_survival(
        iteration=int(fx["iteration"]),
        per_ag_snapshots=[_snapshot(fx)],
    )
    floor = lifecycle.ledger_floor_for_iteration(
        int(fx["iteration"]),
        current_patches=0,
        current_selected="",
    )
    assert floor["patches_applied"] == exp["patches_applied"]
    assert floor["selected_proposal_id"] == exp["selected_proposal_id"]


def test_lifecycle_floor_never_lowers_an_accept_count(fx) -> None:
    """The floor is a floor, not an override: when an accept already set
    a higher patches_applied (e.g. from len(patches)), the lifecycle must
    not lower it."""
    lifecycle = CandidateLifecycle.from_patch_survival(
        iteration=int(fx["iteration"]),
        per_ag_snapshots=[_snapshot(fx)],
    )
    floor = lifecycle.ledger_floor_for_iteration(
        int(fx["iteration"]),
        current_patches=3,
        current_selected="accept_selected_pid",
    )
    assert floor["patches_applied"] == 3
    assert floor["selected_proposal_id"] == "accept_selected_pid"


def test_lifecycle_carries_applied_but_inert_flag(fx) -> None:
    """Track B / B2: the post-apply inertness verdict rides on the
    lifecycle record. Default is False; ``with_applied_but_inert`` sets it
    without disturbing the applied-funnel facts."""
    lifecycle = CandidateLifecycle.from_patch_survival(
        iteration=int(fx["iteration"]),
        per_ag_snapshots=[_snapshot(fx)],
    )
    assert lifecycle.applied_but_inert is False
    inert = lifecycle.with_applied_but_inert(True)
    assert inert.applied_but_inert is True
    # The funnel facts are preserved across the inertness annotation.
    assert inert.patches_applied == lifecycle.patches_applied
    assert inert.selected_proposal_id == lifecycle.selected_proposal_id
    # Original record is unchanged (frozen dataclass copy semantics).
    assert lifecycle.applied_but_inert is False
