def test_plan_v3_state_machine_iteration_flag_no_longer_exists():
    import importlib

    config = importlib.import_module("genie_space_optimizer.common.config")
    assert not hasattr(config, "plan_v3_state_machine_iteration_enabled"), (
        "Phase 4 removes the Plan v3 flag — the SM is unconditional now"
    )
