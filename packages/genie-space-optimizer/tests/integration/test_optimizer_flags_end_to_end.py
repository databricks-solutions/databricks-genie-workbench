"""Optimizer Control-Plane Hardening Plan — Task G integration test.

With all five control-plane flags enabled, the synthetic 3-cluster
airline-shaped fixture should produce the expected outcomes
(target-aware acceptance rejects below threshold; halts when no
causal applyable; non-semantic patches survive blast-radius;
diagnostic AGs inherit cluster RCA so their proposals don't get
dropped at the rca_groundedness gate).

Step 1 lands this scaffolding test against today's airline fixture.
The active assertion checks that the replay path runs to completion
with the flags on; the richer outcome assertions are exercised when
the cycle-9 fixture (Task G Steps 2-4) is intaken.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_FX = (
    Path(__file__).resolve().parents[1]
    / "replay" / "fixtures" / "airline_real_v1.json"
)


@pytest.fixture
def all_flags_on(monkeypatch):
    for env in (
        "GSO_TARGET_AWARE_ACCEPTANCE",
        "GSO_NO_CAUSAL_APPLYABLE_HALT",
        "GSO_BUCKET_DRIVEN_AG_SELECTION",
        "GSO_RCA_AWARE_PATCH_CAP",
        "GSO_LEVER_AWARE_BLAST_RADIUS",
    ):
        monkeypatch.setenv(env, "1")


@pytest.mark.skipif(
    not _FX.exists(),
    reason="airline replay fixture not present",
)
def test_synthetic_airline_run_completes_with_all_flags_on(
    all_flags_on,
) -> None:
    """Smoke: with all five control-plane flags on, the lever_loop
    replay over the airline fixture runs to completion. The richer
    target-aware outcome assertions land once the cycle-9 fixture
    (Task G Steps 2-4) is intaken — until then this guards against
    the flags-on path crashing on a representative input."""
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    fx = json.loads(_FX.read_text())
    result = run_replay(fx)
    # Replay returned a structured result; the harness did not raise
    # under flags-on.
    assert result is not None
