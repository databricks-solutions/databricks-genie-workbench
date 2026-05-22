"""The harness's per-iteration call site must call
run_state_machine_iteration_and_persist as the authoritative iteration
body, not maybe_run_state_machine_canary_iteration as a sidecar."""

def test_harness_imports_authoritative_sm_entry_point():
    import inspect
    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)
    assert "run_state_machine_iteration_and_persist" in src, (
        "harness.py must call the authoritative SM entry point"
    )


def test_harness_does_not_call_canary_helper_anymore():
    import inspect
    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)
    # After Phase 4: the canary helper is deleted. Before Phase 5 deletes
    # it from optimizer.py, we still need the harness to stop calling it.
    assert "maybe_run_state_machine_canary_iteration" not in src, (
        "harness.py must not call the sidecar canary helper any longer"
    )
