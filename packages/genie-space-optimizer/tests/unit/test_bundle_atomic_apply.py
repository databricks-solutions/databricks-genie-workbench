"""Phase 2 P2.3 — atomic-bundle-apply contract.

Pins the partition / status / signature / survivor-selection helpers in
``bundle_atomic_apply``. The contract under test:

  * Singletons (``bundle_id == ""``) flow OUT of the bundle map and
    INTO the singleton list — they never see the atomic gate.
  * Bundle status is ``"all_applied"`` only when every member landed,
    ``"none_applied"`` when zero landed, ``"partial"`` otherwise.
  * Partial bundles mint a typed forbidden_signature with applied/
    failed counts so the next iteration's strategist pivots away.
  * Survivor-selection picks the highest-scoring fully-applied
    bundle deterministically (ties resolved by bundle_id).
  * Partial / none_applied bundles are NEVER selected — even when
    they would otherwise score highest.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.bundle_atomic_apply import (
    BundleApplyOutcome,
    bundle_apply_status,
    bundle_partial_apply_signature,
    partition_apply_outcomes_by_bundle,
    select_survivor_bundle,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


def _proposal(intent_id: str, bundle_id: str = "") -> RepairProposal:
    return RepairProposal(
        intent_id=intent_id,
        intent_name="n",
        intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_INSTRUCTION,
        rationale="r",
        confidence="medium",
        patch_body={"text": "x"},
        blame_set=(),
        bundle_id=bundle_id,
    )


def test_partition_separates_singletons_from_bundles() -> None:
    proposals = [
        _proposal("i1", bundle_id=""),
        _proposal("i2", bundle_id="b1"),
        _proposal("i3", bundle_id="b1"),
        _proposal("i4", bundle_id="b2"),
        _proposal("i5", bundle_id=""),
    ]
    outcomes, singletons = partition_apply_outcomes_by_bundle(
        proposals, applied_intent_ids={"i1", "i2", "i3", "i4"}
    )
    assert set(singletons) == {"i1", "i5"}
    assert set(outcomes) == {"b1", "b2"}


def test_all_applied_status_when_every_member_landed() -> None:
    proposals = [
        _proposal("i1", "b1"),
        _proposal("i2", "b1"),
    ]
    outcomes, _ = partition_apply_outcomes_by_bundle(
        proposals, applied_intent_ids={"i1", "i2"}
    )
    assert outcomes["b1"].status == "all_applied"
    assert outcomes["b1"].applied_intent_ids == ("i1", "i2")
    assert outcomes["b1"].failed_intent_ids == ()


def test_none_applied_status_when_zero_landed() -> None:
    proposals = [
        _proposal("i1", "b1"),
        _proposal("i2", "b1"),
    ]
    outcomes, _ = partition_apply_outcomes_by_bundle(
        proposals, applied_intent_ids=set()
    )
    assert outcomes["b1"].status == "none_applied"


def test_partial_status_when_some_landed() -> None:
    proposals = [
        _proposal("i1", "b1"),
        _proposal("i2", "b1"),
        _proposal("i3", "b1"),
    ]
    outcomes, _ = partition_apply_outcomes_by_bundle(
        proposals, applied_intent_ids={"i1", "i3"}
    )
    out = outcomes["b1"]
    assert out.status == "partial"
    assert set(out.applied_intent_ids) == {"i1", "i3"}
    assert set(out.failed_intent_ids) == {"i2"}


def test_bundle_apply_status_returns_status_field() -> None:
    o = BundleApplyOutcome(
        bundle_id="b",
        applied_intent_ids=(),
        failed_intent_ids=("i1",),
        status="none_applied",
    )
    assert bundle_apply_status(o) == "none_applied"


def test_partial_apply_signature_shape() -> None:
    o = BundleApplyOutcome(
        bundle_id="cluster_x.iter2.lever-1",
        applied_intent_ids=("i1",),
        failed_intent_ids=("i2", "i3"),
        status="partial",
    )
    sig = bundle_partial_apply_signature(o)
    assert sig == (
        "bundle_partial_apply:bundle=cluster_x.iter2.lever-1"
        ":applied=1:failed=2"
    )


def test_terminal_reason_bundle_partial_apply_exists() -> None:
    # The atomic-apply transformer fires this exact value when it
    # rolls back a partial bundle.
    assert TerminalReason.BUNDLE_PARTIAL_APPLY.value == "bundle_partial_apply"


def test_select_survivor_picks_highest_scoring_all_applied_bundle() -> None:
    outcomes = {
        "b1": BundleApplyOutcome(
            "b1", ("i1", "i2"), (), "all_applied",
        ),
        "b2": BundleApplyOutcome(
            "b2", ("i3",), (), "all_applied",
        ),
    }
    scores = {"b1": 0.85, "b2": 0.92}
    bid, out = select_survivor_bundle(outcomes, scores)
    assert bid == "b2"
    assert out is outcomes["b2"]


def test_select_survivor_skips_partial_even_when_highest_scoring() -> None:
    outcomes = {
        "b1": BundleApplyOutcome(
            "b1", ("i1",), ("i2",), "partial",
        ),
        "b2": BundleApplyOutcome(
            "b2", ("i3",), (), "all_applied",
        ),
    }
    # Score 0.99 vs 0.50: the partial bundle scores higher but is
    # ineligible by construction.
    scores = {"b1": 0.99, "b2": 0.50}
    bid, out = select_survivor_bundle(outcomes, scores)
    assert bid == "b2"


def test_select_survivor_returns_none_when_no_eligible_bundles() -> None:
    outcomes = {
        "b1": BundleApplyOutcome("b1", (), ("i1",), "none_applied"),
        "b2": BundleApplyOutcome("b2", ("i2",), ("i3",), "partial"),
    }
    bid, out = select_survivor_bundle(outcomes, {"b1": 0.9, "b2": 0.8})
    assert bid is None and out is None


def test_select_survivor_tie_break_by_lexicographic_bundle_id() -> None:
    outcomes = {
        "b_z": BundleApplyOutcome("b_z", ("i1",), (), "all_applied"),
        "b_a": BundleApplyOutcome("b_a", ("i2",), (), "all_applied"),
    }
    scores = {"b_z": 0.75, "b_a": 0.75}
    bid, _ = select_survivor_bundle(outcomes, scores)
    assert bid == "b_a"


def test_empty_inputs_return_empty_maps() -> None:
    outcomes, singletons = partition_apply_outcomes_by_bundle([], set())
    assert outcomes == {}
    assert singletons == ()
    bid, out = select_survivor_bundle({}, {})
    assert bid is None and out is None
