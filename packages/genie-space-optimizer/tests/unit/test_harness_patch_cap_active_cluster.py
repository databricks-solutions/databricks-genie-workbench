"""Pin that the harness passes active_cluster_ids and per-cluster slot floor."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_harness_patch_cap_call_passes_active_cluster_ids() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "select_target_aware_causal_patch_cap(" in src, "Cap call site moved"
    cap_block = src.split("select_target_aware_causal_patch_cap(", 1)[1].split(")", 1)[0]
    assert "active_cluster_ids=" in cap_block, "harness must pass active_cluster_ids"
    assert "per_cluster_slot_floor=" in cap_block, (
        "harness must pass per_cluster_slot_floor so each active cluster keeps a slot"
    )
