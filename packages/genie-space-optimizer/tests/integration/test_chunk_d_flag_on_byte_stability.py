"""C15 Phase 1 Task 1.9 — Chunk D flag-on byte-stability placeholder.

When ``GSO_STAGE_HANDLERS_CHUNK_D=1``, the harness's Chunk D call sites
delegate to typed stage handlers instead of the legacy inline functions.
The flag-off (legacy) path must produce identical ``loop_out`` bytes
(modulo non-deterministic timestamps) so no regression is introduced.

The actual byte-stability comparison requires a runnable replay fixture
and a live harness invocation, which is not available in CI. The test
is kept as a placeholder so the test runner sees it and the manual
pre-merge gate is documented here.

Manual gate (run before merging the Phase 1 PR):

    GSO_STAGE_HANDLERS_CHUNK_D=0 uv run ... > /tmp/legacy.txt
    GSO_STAGE_HANDLERS_CHUNK_D=1 uv run ... > /tmp/typed.txt
    diff /tmp/legacy.txt /tmp/typed.txt

Expected diff: only timestamp/run_id fields differ.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.parametrize("flag_value", ["0", "1"])
def test_legacy_replay_byte_stable_under_flag(flag_value: str, tmp_path: Path) -> None:
    """Drive a legacy local replay with the flag on and off; the
    final loop_out dict must be byte-identical (modulo non-deterministic
    timestamps which the compare strips)."""
    pytest.skip(
        "Hooked up after harness wiring lands; placeholder for the manual "
        "byte-stability gate. Run manually per the docstring above."
    )
