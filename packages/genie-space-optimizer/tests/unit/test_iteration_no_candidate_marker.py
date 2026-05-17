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


def test_marker_emit_pipeline_surfaces_archetypes_from_record_metrics() -> None:
    """Phase 0.5 (Bug 3) — end-to-end check that the
    record -> record.to_dict() -> marker_line consumer chain at
    harness.py:23035-23043 produces a marker with non-empty
    attempted_archetypes when the upstream record had them.

    We do not import harness here (to keep this test fast and free of
    Spark dependencies) — instead we replicate the consumer's lookup
    expression `_nsc_dict.get("metrics", {}).get("attempted_archetypes")`
    against the producer's output. If the producer ever drops the key
    again, this test catches it before the harness wiring does."""
    from genie_space_optimizer.optimization.decision_emitters import (
        no_structural_candidate_record,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        no_structural_candidate_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        parse_no_structural_candidate_marker,
    )

    record = no_structural_candidate_record(
        run_id="r1",
        iteration=4,
        ag_id="AG_H001",
        attempted_archetypes=("single_row_top_n", "ordered_list_by_metric"),
    )
    nsc_dict = record.to_dict()

    # Replicate harness.py:23035-23043 consumer expression exactly.
    archetypes_lookup = (
        nsc_dict.get("metrics", {}).get("attempted_archetypes") or ()
    )
    line = no_structural_candidate_marker(
        ag_id=str(nsc_dict.get("ag_id") or ""),
        iteration=int(nsc_dict.get("iteration") or 0),
        attempted_archetypes=archetypes_lookup,
    )
    parsed = parse_no_structural_candidate_marker(line)

    assert parsed["attempted_archetypes"] == [
        "single_row_top_n", "ordered_list_by_metric",
    ], (
        "Marker payload must surface the archetypes the producer reported. "
        "Empty list here means Bug 3 has regressed — the consumer at "
        "harness.py:23035-23043 is reading metrics['attempted_archetypes'] "
        "but the producer omitted the key."
    )
    assert parsed["ag_id"] == "AG_H001"
    assert parsed["iteration"] == 4
