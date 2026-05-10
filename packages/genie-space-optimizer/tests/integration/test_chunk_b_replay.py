"""C15 Phase 3 Task 3.2 — Chunk B (action_group_selection) replay tests.

Two tests:

1. ``test_chunk_b_replay`` — generic replay parametrised over any anchor
   that has both ``input.json`` and ``expected_output.json`` under the
   ``action_group_selection`` stage dir. Currently no full-replay fixture
   exists (the 7Now iter_01 input is 2.5 MB and requires PII audit before
   vendoring), so this parametrised suite will skip if no cases are found.

2. ``test_iter02_does_not_reselect_forbidden_ag1_on_7now`` — CONTRACT test
   for the forbidden-AG no-op loop closure. Reads the vendored
   ``expected_output.json`` for 7Now iter_02 and asserts that:
   - ``selected_ag_id`` (if present) is not "AG1"
   - the ``admission_trace`` contains an AG1 entry with verdict="denied"
     and denial_reason="no_proposals"

   This is a contract test, not a full round-trip replay. The input.json
   for iter_02 was not captured by the postmortem bundle (only
   cluster_formation was logged for iter_02). See the fixture README for
   full context.

   Full round-trip replay (input → execute → compare output) is deferred
   to a follow-up fixture-capture task when a scrubbed/truncated
   input.json is available.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration._replay_helpers import (
    FIXTURES_ROOT,
    assert_replay_matches,
    cases_for_chunk,
)


_CHUNK_B_STAGES = ("action_group_selection",)


@pytest.mark.parametrize(
    "anchor,stage_key",
    cases_for_chunk(_CHUNK_B_STAGES),
)
def test_chunk_b_replay(anchor: str, stage_key: str) -> None:
    """Full round-trip replay for Chunk B stages.

    Parametrised — skips automatically when no fixture pairs exist.
    When the 7Now iter_01 input.json is scrubbed and vendored as a
    fixture, this test will cover it automatically without any change.
    """
    assert_replay_matches(anchor, stage_key)


def test_iter02_does_not_reselect_forbidden_ag1_on_7now() -> None:
    """C15 closes the forbidden-AG no-op loop at the contract level.

    7Now run 960148942255012: iter_01 selected AG1 but produced zero
    proposals. iter_02 must NOT re-select AG1. This contract test
    verifies the expected_output fixture encodes the correct closure:
    AG1 appears in admission_trace with verdict="denied" and
    denial_reason="no_proposals".

    If the fixture is absent this test skips with a clear message —
    it does NOT silently pass. The fixture at
    ``fixtures/7now_960148942255012_iter02/action_group_selection/expected_output.json``
    is the vendored contract evidence.
    """
    fixture = (
        FIXTURES_ROOT
        / "7now_960148942255012_iter02"
        / "action_group_selection"
        / "expected_output.json"
    )
    if not fixture.exists():
        pytest.skip(
            "iter02 fixture not vendored yet — "
            "run scripts/capture_stage_fixture.py for 7now_960148942255012 iter02 "
            "once the input.json PII audit is complete"
        )

    expected = json.loads(fixture.read_text())

    # AG1 must not be the selected AG (ags must not contain AG1)
    ags = expected.get("ags", [])
    ag1_selected = [a for a in ags if (a.get("id") or a.get("ag_id")) == "AG1"]
    assert not ag1_selected, (
        "7now iter_02 must not select AG1 when iter_01 had zero proposals; "
        "found AG1 in output ags: this is the forbidden-AG no-op loop regression"
    )

    # admission_trace must record AG1 as DENIED with reason no_proposals
    trace = expected.get("admission_trace", [])
    ag1_traces = [t for t in trace if t.get("ag_id") == "AG1"]
    assert ag1_traces, (
        "admission_trace must contain an entry for AG1 on iter_02 "
        "(it should be DENIED/no_proposals)"
    )
    ag1_entry = ag1_traces[0]
    assert ag1_entry.get("verdict") == "denied", (
        f"AG1 admission verdict expected 'denied', got {ag1_entry.get('verdict')!r}"
    )
    assert ag1_entry.get("denial_reason") == "no_proposals", (
        f"AG1 denial_reason expected 'no_proposals', got {ag1_entry.get('denial_reason')!r}"
    )
