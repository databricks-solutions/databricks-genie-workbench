from __future__ import annotations


def test_harness_marks_no_applied_bundle_as_dead_on_arrival() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    skip_idx = source.index("_apply_skip = _should_skip_eval_for_patch_bundle(")
    snippet = source[skip_idx - 800 : skip_idx + 2200]

    assert "deterministic_no_applied_patches" in snippet
    assert "_dead_on_arrival_patch_signatures" in source
    assert "_dead_on_arrival_ag_ids" in source
    assert "all_selected_patches_dropped_by_applier" in snippet
    assert "pending_action_groups = []" in snippet
    assert "pending_strategy = None" in snippet


def test_harness_blocks_retry_of_same_dead_patch_signature() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    assert "_selected_patch_signature = tuple(sorted(" in source
    assert "_selected_patch_signature in _dead_on_arrival_patch_signatures" in source
    assert "Skipping dead-on-arrival AG retry" in source
