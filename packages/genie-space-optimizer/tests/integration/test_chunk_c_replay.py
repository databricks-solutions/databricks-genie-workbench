"""C15 Phase 4.5 — Chunk C (proposal_generation, safety_gates, applied_patches) replay.

Generic parametrised replay over any anchor that has both ``input.json`` and
``expected_output.json`` under the stage fixture dir.

Currently no fixture pairs exist for any Chunk C stage — the inputs carry
``proposals_by_ag`` (LLM-generated SQL / patch text) that requires a PII
redaction pass before vendoring. The test auto-skips via
``cases_for_chunk()`` returning an empty list when no json pairs are found.

When a scrubbed input.json is vendored for any anchor/stage pair, this test
will pick it up automatically without any change here.
"""

from __future__ import annotations

import pytest

from tests.integration._replay_helpers import assert_replay_matches, cases_for_chunk


_CHUNK_C_STAGES = ("proposal_generation", "safety_gates", "applied_patches")


@pytest.mark.parametrize("anchor,stage_key", cases_for_chunk(_CHUNK_C_STAGES))
def test_chunk_c_replay(anchor: str, stage_key: str) -> None:
    assert_replay_matches(anchor, stage_key)
