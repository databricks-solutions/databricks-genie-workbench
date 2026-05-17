"""Phase 6.5 — GSO_NO_STRUCTURAL_CANDIDATE_V1 carries skipped_reason."""
from __future__ import annotations

import json

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


def test_marker_omits_skipped_reason_when_empty():
    line = no_structural_candidate_marker(
        ag_id="AG_001",
        iteration=2,
        attempted_archetypes=(),
    )
    payload = json.loads(line[len("GSO_NO_STRUCTURAL_CANDIDATE_V1 "):])
    assert payload["skipped_reason"] == ""
    assert payload["attempted_archetypes"] == []
