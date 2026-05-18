"""Phase 6.5 — GSO_NO_STRUCTURAL_CANDIDATE_V1 carries skipped_reason.

Phase 1.5 (2026-05-17) — additionally enforces the refuse-on-empty
invariant: a marker constructed with both ``skipped_reason`` empty
AND ``attempted_archetypes`` empty raises ``ValueError`` instead of
silently producing a payload with empty causal context.
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    no_structural_candidate_marker,
)


def test_marker_carries_skipped_reason():
    line = no_structural_candidate_marker(
        ag_id="AG_001",
        iteration=2,
        attempted_archetypes=("top_n_collapse_archetype",),
        skipped_reason="no_top_n_archetype",
    )
    assert line.startswith("GSO_NO_STRUCTURAL_CANDIDATE_V1 ")
    payload = json.loads(line[len("GSO_NO_STRUCTURAL_CANDIDATE_V1 "):])
    assert payload["skipped_reason"] == "no_top_n_archetype"
    assert payload["attempted_archetypes"] == ["top_n_collapse_archetype"]


def test_marker_refuses_when_both_empty():
    """Phase 1.5 refuse-on-empty — double-empty raises ValueError."""
    with pytest.raises(ValueError):
        no_structural_candidate_marker(
            ag_id="AG_001",
            iteration=2,
            attempted_archetypes=(),
        )
