"""Stable signature for iteration-level proposal failures. Used by the
stalemate detector — when the same signature recurs across iterations,
the harness escalates instead of looping."""

from __future__ import annotations

from genie_space_optimizer.optimization.iteration_signature import (
    iteration_failure_signature,
)


def test_signature_is_stable_across_call_orderings_of_tried_lever_families():
    """Tried-lever-families is a *set* semantically — the signature
    must not depend on its order."""
    sig_a = iteration_failure_signature(
        ag_id="AG1", failure_mode="proposal_generation_empty",
        root_cause="wrong_aggregation",
        lever_set=(2, 6),
        tried_lever_families=(2, 6),
        cluster_signature="sig-C1",
    )
    sig_b = iteration_failure_signature(
        ag_id="AG1", failure_mode="proposal_generation_empty",
        root_cause="wrong_aggregation",
        lever_set=(6, 2),
        tried_lever_families=(6, 2),
        cluster_signature="sig-C1",
    )
    assert sig_a == sig_b


def test_signature_differs_when_failure_mode_changes():
    sig_a = iteration_failure_signature(
        ag_id="AG1", failure_mode="proposal_generation_empty",
        root_cause="wrong_aggregation",
        lever_set=(2,), tried_lever_families=(),
        cluster_signature="sig-C1",
    )
    sig_b = iteration_failure_signature(
        ag_id="AG1", failure_mode="no_applied_patches",
        root_cause="wrong_aggregation",
        lever_set=(2,), tried_lever_families=(),
        cluster_signature="sig-C1",
    )
    assert sig_a != sig_b


def test_signature_differs_when_cluster_signature_changes():
    sig_a = iteration_failure_signature(
        ag_id="AG1", failure_mode="proposal_generation_empty",
        root_cause="wrong_aggregation",
        lever_set=(2,), tried_lever_families=(),
        cluster_signature="sig-C1",
    )
    sig_b = iteration_failure_signature(
        ag_id="AG1", failure_mode="proposal_generation_empty",
        root_cause="wrong_aggregation",
        lever_set=(2,), tried_lever_families=(),
        cluster_signature="sig-C2",
    )
    assert sig_a != sig_b


def test_signature_is_a_short_hex_string():
    """Signatures are used as dict keys and printed in logs — keep
    them short."""
    sig = iteration_failure_signature(
        ag_id="AG1", failure_mode="proposal_generation_empty",
        root_cause="wrong_aggregation",
        lever_set=(2,), tried_lever_families=(),
        cluster_signature="sig-C1",
    )
    assert isinstance(sig, str)
    assert len(sig) == 16  # 8 bytes hex
    assert all(c in "0123456789abcdef" for c in sig)


def test_signature_handles_empty_tried_lever_families():
    sig = iteration_failure_signature(
        ag_id="AG1", failure_mode="proposal_generation_empty",
        root_cause="wrong_aggregation",
        lever_set=(2,), tried_lever_families=(),
        cluster_signature="sig-C1",
    )
    assert isinstance(sig, str)
