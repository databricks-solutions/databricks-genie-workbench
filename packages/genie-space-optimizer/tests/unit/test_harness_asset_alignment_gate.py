"""Pin L5/L6 asset alignment gate."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_harness_invokes_proposal_aligns_with_cluster_for_l5_l6() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "proposal_aligns_with_cluster" in src
    assert "l5_l6_patch_requires_asset_alignment" in src


def test_harness_emits_asset_alignment_dropped_audit() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "asset_alignment_dropped" in src
