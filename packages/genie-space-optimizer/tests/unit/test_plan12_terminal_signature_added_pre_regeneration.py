"""Plan 12 — when an iteration terminates with no_applied_patches /
structural_gate_dropped_instruction_only / narrow_loop_exhausted, the
resulting TerminalSignature MUST be added to the AG forbidden_set
BEFORE the next iteration's AG regeneration call.

This closes the ``ag_collision_with_forbidden_set`` retry-budget
waste both 2026-05-20 postmortems observed: today the regenerator
was called with a stale forbidden_set, the LLM proposed the same
AG again, the validator rejected it, and the iteration consumed
budget without producing a meaningful retry.
"""
from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)


def test_terminal_signature_added_before_regenerate():
    from genie_space_optimizer.optimization.stages.action_groups import (
        regenerate_action_groups_with_signatures,
    )

    prior_signature = build_terminal_signature(
        root_cause="top_n_collapse",
        blame_set=["catalog.schema.orders"],
        lever_set={6},
        target_qids={"gs_009"},
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )

    captured_forbidden_sets: list[set] = []

    def _fake_inner_regen(*, prior_clusters, forbidden_set, **kwargs):
        captured_forbidden_sets.append(set(forbidden_set))
        return []

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[prior_signature],
        existing_forbidden_set=set(),
        inner_regenerate=_fake_inner_regen,
    )

    assert captured_forbidden_sets, "regenerator must be called"
    assert prior_signature in captured_forbidden_sets[0], (
        "prior terminal signature must be in forbidden_set BEFORE "
        "regeneration runs"
    )


def test_existing_forbidden_set_preserved():
    """The wrapper must UNION the prior signatures with the existing
    forbidden_set, not replace it."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        regenerate_action_groups_with_signatures,
    )

    prior_signature = build_terminal_signature(
        root_cause="top_n_collapse",
        blame_set=["catalog.schema.orders"],
        lever_set={6},
        target_qids={"gs_009"},
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    pre_existing = build_terminal_signature(
        root_cause="missing_filter",
        blame_set=["catalog.schema.orders.col"],
        lever_set={6},
        target_qids={"gs_021"},
        terminal_reason=TerminalReason.APPLYABILITY_REJECTED,
    )

    captured: list[set] = []

    def _fake(*, prior_clusters, forbidden_set, **kwargs):
        captured.append(set(forbidden_set))
        return []

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[prior_signature],
        existing_forbidden_set={pre_existing},
        inner_regenerate=_fake,
    )

    assert pre_existing in captured[0]
    assert prior_signature in captured[0]


def test_empty_prior_signatures_passes_through():
    """No prior signatures → forbidden_set is the original."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        regenerate_action_groups_with_signatures,
    )

    captured: list[set] = []

    def _fake(*, prior_clusters, forbidden_set, **kwargs):
        captured.append(set(forbidden_set))
        return []

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[],
        existing_forbidden_set=set(),
        inner_regenerate=_fake,
    )

    assert captured == [set()]
