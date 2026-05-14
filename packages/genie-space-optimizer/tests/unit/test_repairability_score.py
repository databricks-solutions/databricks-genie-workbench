"""Spec Section 8.4 — RepairabilityScore."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.repairability_score import (
    RepairabilityScore,
    compute_repairability,
)
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
)


def test_score_is_1_when_intended_matches_emitted_structural():
    score = compute_repairability(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        narrow_replacement_available=False,
    )
    assert isinstance(score, RepairabilityScore)
    assert score.value == pytest.approx(1.0)
    assert score.intended_shape == "structural"
    assert score.emitted_shape == "structural"


def test_score_is_0_when_intended_structural_but_emitted_metadata():
    score = compute_repairability(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.METADATA,
        narrow_replacement_available=False,
    )
    assert score.value == pytest.approx(0.0)


def test_score_is_05_when_narrow_replacement_available():
    """Partial repairability: structural intent, metadata emission,
    but a narrow-replacement is available as a recovery path."""
    score = compute_repairability(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.METADATA,
        narrow_replacement_available=True,
    )
    assert score.value == pytest.approx(0.5)


def test_score_is_1_when_intended_is_empty_legacy_card():
    """Legacy RCA cards without intended_patch_shape — fail OPEN
    (repairability=1.0)."""
    score = compute_repairability(
        intended_patch_shape="",
        emitted_patch_shape=EmittedPatchShape.METADATA,
        narrow_replacement_available=False,
    )
    assert score.value == pytest.approx(1.0)


def test_score_is_clamped_to_unit_range():
    score = compute_repairability(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        narrow_replacement_available=True,
    )
    assert 0.0 <= score.value <= 1.0
