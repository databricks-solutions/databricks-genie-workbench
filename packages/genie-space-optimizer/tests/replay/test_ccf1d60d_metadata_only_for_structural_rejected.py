"""Phase 5 fixture: metadata-only patches for structural-intent RCA card
rejected by the structural-repair gate BEFORE full eval.

Maps to user-text ``test_metadata_only_for_structural_rca_rejected``.
Anchor: ccf1d60d iter-1 surviving patches + iter-1 gs_026 RCA card.

User-text ``STRUCTURAL_REPAIR_MISSING`` maps to spec-canonical
``TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY`` (per Phase 1+2
plan Tier A bulk renames). This test uses the spec-canonical name.

API drift from plan template:
* The real ``enforce_structural_repair_shape`` takes
  ``intended_patch_shape: str`` and ``emitted_patch_shape: EmittedPatchShape``
  (not ``rca_card`` / ``surviving_patches`` dicts). The card's
  ``intended_patch_shape`` and the surviving patches' computed shape are
  passed in by callers; this test does that classification explicitly via
  ``resolve_emitted_patch_shape``.
* The verdict exposes ``outcome`` (``"admitted"`` | ``"rejected"``) and a
  string ``terminal_reason`` (the enum's ``.value``); there is no
  ``.admit`` attribute.
* There is no ``optimization/stages/full_eval`` module; the post-gate eval
  stage is ``optimization/stages/evaluation`` whose ``execute`` alias is
  the full-eval entrypoint. The "full eval NOT triggered" assertion mocks
  that symbol.
"""
from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.structural_repair_gate import (
    enforce_structural_repair_shape,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
    resolve_emitted_patch_shape,
)

from tests.replay.fixtures.phase5._helpers import load


def _shape_from_surviving(surviving: dict) -> EmittedPatchShape:
    """Classify the surviving-patches fixture into one EmittedPatchShape.

    The fixture wraps the patch list under a ``patches`` key (see
    ``_source`` note in the fixture body). All three surviving patches
    (``update_column_description``, ``add_column_synonym``,
    ``add_sql_snippet``) fall into the METADATA / INSTRUCTION families;
    none is a structural-repair member, so this returns METADATA per the
    classifier's tag heuristics.
    """
    return resolve_emitted_patch_shape(surviving["patches"])


def test_structural_intent_with_metadata_patches_rejected() -> None:
    rca_card = load("ccf1d60d_rca_card_gs026.json")
    surviving = load("ccf1d60d_iter1_surviving_patches.json")

    assert rca_card["intended_patch_shape"] == "structural"
    emitted = _shape_from_surviving(surviving)
    assert emitted != EmittedPatchShape.STRUCTURAL, (
        "fixture invariant: surviving patches are NOT structural; "
        f"got {emitted}"
    )

    verdict = enforce_structural_repair_shape(
        intended_patch_shape=rca_card["intended_patch_shape"],
        emitted_patch_shape=emitted,
        narrow_replacement_available=False,
    )

    assert verdict.outcome == "rejected"
    assert verdict.terminal_reason == (
        TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
    )


def test_full_eval_not_triggered_when_gate_rejects() -> None:
    """If ``enforce_structural_repair_shape`` returns outcome=="rejected",
    the harness must NOT call the full-eval stage. We assert this via a
    mock on the full-eval entrypoint; if it gets called inside the gate,
    this test fails.
    """
    rca_card = load("ccf1d60d_rca_card_gs026.json")
    surviving = load("ccf1d60d_iter1_surviving_patches.json")
    emitted = _shape_from_surviving(surviving)

    full_eval_called: list[bool] = []
    with patch(
        "genie_space_optimizer.optimization.stages.evaluation.execute",
        side_effect=lambda *a, **kw: full_eval_called.append(True),
    ):
        verdict = enforce_structural_repair_shape(
            intended_patch_shape=rca_card["intended_patch_shape"],
            emitted_patch_shape=emitted,
            narrow_replacement_available=False,
        )

    assert verdict.outcome == "rejected"
    assert not full_eval_called, (
        "structural gate should short-circuit before full_eval is called"
    )
