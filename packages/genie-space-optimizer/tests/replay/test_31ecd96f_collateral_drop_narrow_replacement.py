"""Phase 5 fixture: blast-radius drop on a structural patch triggers
narrow-replacement synthesis with the dropped-for-dependents passed as
protected_dependents.

Maps to user-text ``test_collateral_drop_attempts_narrow_replacement``.
Anchor: 31ecd96f iter-2 (``add_sql_snippet_measure`` on ``tkt_payment``
dropped for ``gs_003`` collateral against the ``gs_024`` AG target).

API note (drift vs. plan pseudo-code, Task 9 in
``2026-05-14-final-closeout-phase-5-6-offline-gates.md``): the real
``try_narrow_replacement`` (post-Phase 2.4) takes synthesis callables as
parameters (no module-level patch of ``cluster_driven_synthesis``) and
threads ``protected_dependents`` from the ``outside_target_qids`` kwarg
into whichever callable matches the dropped patch's ``patch_type``. The
return shape is :class:`NarrowReplacementResult` (``attempted`` /
``replacement_patch`` / ``terminal_reason`` as a string), not the plan's
hypothetical ``outcome.narrow_patch`` / enum.

These tests therefore inject the synthesis callable directly (instead
of ``unittest.mock.patch``) and pass the fixture's
``dropped_for_dependents`` as ``outside_target_qids`` so the
"``protected_dependents`` is threaded from the dropped-for set"
invariant is exercised end-to-end.

User-text ``NO_STRUCTURAL_ALTERNATIVE`` maps to spec-canonical
``TerminalReason.BLAST_RADIUS_REJECTED`` (Phase 1+2 plan Tier A bulk
rename); the string form is what
:class:`NarrowReplacementResult.terminal_reason` carries.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.auto_narrow_replacement import (
    try_narrow_replacement,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason

from tests.replay.fixtures.phase5._helpers import load


def test_narrow_replacement_invoked_with_protected_dependents() -> None:
    """The first collateral-dropped L6 patch must invoke the L6
    synthesis callable with ``protected_dependents`` equal to the
    fixture's ``dropped_for_dependents`` (here: ``("gs_003",)``)."""
    iter2 = load("31ecd96f_iter2_collateral.json")
    dropped = iter2["dropped_patches"][0]
    protected = tuple(dropped["dropped_for_dependents"])

    call_args: dict = {}

    def _spy_l6(**kwargs):
        call_args.update(kwargs)
        return None  # force the BLAST_RADIUS_REJECTED branch

    def _stub_l5(**_kwargs):
        raise AssertionError(
            "L5 synthesis must not be called for an L6 sql_snippet drop"
        )

    result = try_narrow_replacement(
        dropped_patches=[
            {
                **dropped,
                # auto_narrow_replacement only recognizes ``sql_snippet``
                # family for L6; the fixture's ``add_sql_snippet_measure``
                # is the harness-level patch_type. Normalize to the L6
                # family the helper dispatches on.
                "patch_type": "sql_snippet",
            }
        ],
        outside_target_qids=protected,
        cluster={"cluster_id": "c1", "target_qids": iter2["target_qids"]},
        rca_card={"root_cause": "missing_measure"},
        synthesis_callable_l6=_spy_l6,
        synthesis_callable_l5=_stub_l5,
    )

    assert result.attempted is True
    assert call_args.get("protected_dependents") == protected
    assert "gs_003" in call_args["protected_dependents"]


def test_narrow_synthesis_failure_emits_blast_radius_rejected() -> None:
    """When the L6 synthesis callable returns ``None``, the result must
    record :attr:`TerminalReason.BLAST_RADIUS_REJECTED` so the caller
    emits the typed terminal marker and retires the signature."""
    iter2 = load("31ecd96f_iter2_collateral.json")
    dropped = iter2["dropped_patches"][0]
    protected = tuple(dropped["dropped_for_dependents"])

    result = try_narrow_replacement(
        dropped_patches=[
            {**dropped, "patch_type": "sql_snippet"},
        ],
        outside_target_qids=protected,
        cluster={"cluster_id": "c1", "target_qids": iter2["target_qids"]},
        rca_card={"root_cause": "missing_measure"},
        synthesis_callable_l6=lambda **_: None,
        synthesis_callable_l5=lambda **_: None,
    )

    assert result.attempted is True
    assert result.replacement_patch is None
    assert result.terminal_reason == TerminalReason.BLAST_RADIUS_REJECTED.value


def test_narrow_synthesis_success_returns_scoped_patch() -> None:
    """When the L6 synthesis callable returns a narrow patch, the
    result must surface it on ``replacement_patch`` with no
    ``terminal_reason``."""
    iter2 = load("31ecd96f_iter2_collateral.json")
    dropped = iter2["dropped_patches"][0]
    protected = tuple(dropped["dropped_for_dependents"])
    synthesized = {
        "patch_id": "L6_tkt_payment_narrow",
        "patch_type": "narrow_l6_sql",
        "scope": "scoped_to_target_qids",
        "target_table": dropped["target_table"],
    }

    result = try_narrow_replacement(
        dropped_patches=[
            {**dropped, "patch_type": "sql_snippet"},
        ],
        outside_target_qids=protected,
        cluster={"cluster_id": "c1", "target_qids": iter2["target_qids"]},
        rca_card={"root_cause": "missing_measure"},
        synthesis_callable_l6=lambda **_: synthesized,
        synthesis_callable_l5=lambda **_: None,
    )

    assert result.attempted is True
    assert result.replacement_patch == synthesized
    assert result.terminal_reason == ""
