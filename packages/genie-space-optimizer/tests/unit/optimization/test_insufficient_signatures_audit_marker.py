"""Trial 19 A5 — ``GSO_INSUFFICIENT_SIGNATURES_IN_CONTEXT_V1`` audit
marker shape.

Pins the marker builder's payload shape so postmortem joins (insufficient
signatures observed at the harness vs. count consumed at the LLM call)
remain stable.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    insufficient_signatures_in_context_marker,
)


def _parse(line: str) -> dict:
    assert line.startswith("GSO_INSUFFICIENT_SIGNATURES_IN_CONTEXT_V1 "), (
        f"unexpected marker prefix: {line!r}"
    )
    return json.loads(line.split(" ", 1)[1])


def test_marker_shape_round_trip():
    line = insufficient_signatures_in_context_marker(
        optimization_run_id="run-1",
        iteration=3,
        stage="plan11_synthesize",
        count=2,
        qid_rca_pairs=("gs_009:rank_to_limit_top_n", "gs_013:missing_filter"),
    )
    payload = _parse(line)
    assert payload["optimization_run_id"] == "run-1"
    assert payload["iteration"] == 3
    assert payload["stage"] == "plan11_synthesize"
    assert payload["count"] == 2
    assert payload["qid_rca_pairs"] == [
        "gs_009:rank_to_limit_top_n",
        "gs_013:missing_filter",
    ]


def test_marker_default_empty_pairs():
    """``qid_rca_pairs`` default is an empty list so postmortem joins
    can detect 'plumbing reached but no signatures consumed' cases."""
    line = insufficient_signatures_in_context_marker(
        optimization_run_id="run-1",
        iteration=1,
        stage="plan11_cluster",
        count=0,
    )
    payload = _parse(line)
    assert payload["qid_rca_pairs"] == []
    assert payload["count"] == 0


def test_marker_stage_is_typed_string():
    """``stage`` is a free-form string but in practice one of
    ``plan11_cluster`` / ``plan11_synthesize``. We pin the
    round-trip not the closed set."""
    for stage in ("plan11_cluster", "plan11_synthesize"):
        line = insufficient_signatures_in_context_marker(
            optimization_run_id="r",
            iteration=1,
            stage=stage,
            count=1,
            qid_rca_pairs=("q:r",),
        )
        payload = _parse(line)
        assert payload["stage"] == stage
