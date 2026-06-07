"""Trial 29 W29.5 — decomposed architecture invariants (typed model).

The monolithic ``architecture_invariants_held: bool`` previously
masked progress whenever ANY orthogonal gap (e.g. bundle completeness)
forced it false. The new model splits the invariant into per-domain
sub-invariants so progress in one domain is visible even while another
is broken. ``ArchitectureInvariants.all_held`` preserves the existing
single-bool contract for backwards compatibility.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.architecture_invariants import (
    ArchitectureInvariants,
    legacy_architecture_invariants_held,
    render_postmortem_section,
)


def test_all_held_when_every_sub_invariant_true():
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=True,
        bundle_completeness_invariants_held=True,
    )
    assert inv.all_held is True


def test_all_held_false_when_any_sub_invariant_false():
    for falsified in (
        {"rca_invariants_held": False},
        {"lever_lattice_invariants_held": False},
        {"bundle_completeness_invariants_held": False},
    ):
        kwargs = dict(
            rca_invariants_held=True,
            lever_lattice_invariants_held=True,
            bundle_completeness_invariants_held=True,
        )
        kwargs.update(falsified)
        inv = ArchitectureInvariants(**kwargs)
        assert inv.all_held is False


def test_legacy_helper_matches_all_held():
    """Single-bool backwards-compat shim follows the same conjunction."""
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=False,  # the pre-W29.1 state
        bundle_completeness_invariants_held=False,
    )
    assert legacy_architecture_invariants_held(inv) == inv.all_held
    assert legacy_architecture_invariants_held(inv) is False


def test_model_round_trip():
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=False,
        bundle_completeness_invariants_held=True,
    )
    blob = inv.model_dump()
    rebuilt = ArchitectureInvariants.model_validate(blob)
    assert rebuilt == inv


def test_postmortem_section_renders_each_sub_invariant():
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=False,
        bundle_completeness_invariants_held=False,
    )
    section = render_postmortem_section(inv)
    assert "rca_invariants_held = true" in section
    assert "lever_lattice_invariants_held = false" in section
    assert "bundle_completeness_invariants_held = false" in section
    assert "architecture_invariants_held = false" in section  # backwards-compat


def test_postmortem_section_when_all_held():
    """When every sub-invariant holds, the aggregate also holds."""
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=True,
        bundle_completeness_invariants_held=True,
    )
    section = render_postmortem_section(inv)
    assert "architecture_invariants_held = true" in section
