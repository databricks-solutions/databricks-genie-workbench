"""Phase 0.3 — GSO_ITERATION_NO_CANDIDATE_V1 producer."""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    iteration_no_candidate_marker,
)


def test_marker_has_v1_prefix():
    line = iteration_no_candidate_marker(
        optimization_run_id="opt-1",
        iteration=3,
        terminal_reason="no_structural_candidate",
        cluster_ids=("c1", "c2"),
        ag_id="ag-3",
    )
    assert line.startswith("GSO_ITERATION_NO_CANDIDATE_V1 ")


def test_marker_payload_round_trips():
    line = iteration_no_candidate_marker(
        optimization_run_id="opt-1",
        iteration=3,
        terminal_reason="no_structural_candidate",
        cluster_ids=("c1", "c2"),
        ag_id="ag-3",
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert payload == {
        "optimization_run_id": "opt-1",
        "iteration": 3,
        "terminal_reason": "no_structural_candidate",
        "cluster_ids": ["c1", "c2"],
        "ag_id": "ag-3",
    }


def test_marker_payload_keys_sorted_for_determinism():
    line = iteration_no_candidate_marker(
        optimization_run_id="opt-1",
        iteration=1,
        terminal_reason="proposal_generation_empty",
        cluster_ids=(),
        ag_id="",
    )
    # marker_line uses sort_keys=True; the keys must appear in
    # lexicographic order for byte-stable replay.
    raw = line.split(" ", 1)[1]
    assert raw.find('"ag_id"') < raw.find('"cluster_ids"') < raw.find('"iteration"')
