"""Cycle 15.2 / C12-T5 part 3 — substrate-gate semantics."""
from __future__ import annotations


def test_substrate_gate_false_for_unpersisted_iteration() -> None:
    """Iterations not in _PATCH_SURVIVAL_MATERIALIZED_ITERS must
    return False — the live arm cannot run without a substrate.
    """
    from genie_space_optimizer.optimization import harness as _h

    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.clear()
    assert _h._patch_survival_json_at_contract_path(iteration=1) is False
    assert _h._patch_survival_json_at_contract_path(iteration=0) is False
    assert _h._patch_survival_json_at_contract_path(iteration=99) is False


def test_substrate_gate_true_after_persist_helper_records_success() -> None:
    """After _persist_iter_patch_survival_to_anchor records an
    iteration in _PATCH_SURVIVAL_MATERIALIZED_ITERS, the substrate
    gate returns True for that exact iteration and False for others.
    """
    from genie_space_optimizer.optimization import harness as _h

    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.clear()
    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.add(2)
    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.add(4)

    assert _h._patch_survival_json_at_contract_path(iteration=2) is True
    assert _h._patch_survival_json_at_contract_path(iteration=4) is True
    assert _h._patch_survival_json_at_contract_path(iteration=3) is False
    assert _h._patch_survival_json_at_contract_path(iteration=0) is False


def test_substrate_gate_resets_set_between_runs() -> None:
    """The materialised set is process-scoped. Explicit clear (the
    pattern run_lever_loop's entry point uses) returns the gate to
    its initial state for every iteration.
    """
    from genie_space_optimizer.optimization import harness as _h

    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.add(1)
    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.add(2)
    assert _h._patch_survival_json_at_contract_path(iteration=1) is True

    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.clear()
    assert _h._patch_survival_json_at_contract_path(iteration=1) is False
    assert _h._patch_survival_json_at_contract_path(iteration=2) is False


def test_run_lever_loop_clears_substrate_set_at_entry(monkeypatch) -> None:
    """run_lever_loop must clear _PATCH_SURVIVAL_MATERIALIZED_ITERS
    so a previous run's iterations cannot pollute the gate. We assert
    the clear by patching run_lever_loop's first downstream call (the
    Spark session resolver) to raise — the clear must run before
    that point, so a pre-seeded set ends up empty after the call.
    """
    import pytest
    from genie_space_optimizer.optimization import harness as _h

    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.clear()
    _h._PATCH_SURVIVAL_MATERIALIZED_ITERS.add(42)

    def _boom(*args, **kwargs):
        raise RuntimeError("intentional early exit for assertion")

    # Patch write_stage — the first real I/O call inside _run_lever_loop
    # after the substrate-set clear and the imports. The clear happens
    # before write_stage, so a pre-seeded set ends up empty.
    monkeypatch.setattr(_h, "write_stage", _boom, raising=False)

    with pytest.raises(RuntimeError, match="intentional early exit"):
        _h._run_lever_loop(
            w=None,
            spark=None,
            run_id="r",
            space_id="s",
            domain="d",
            benchmarks=[],
            exp_name="e",
            prev_scores={},
            prev_accuracy=0.0,
            prev_model_id="",
            config={},
            catalog="c",
            schema="s",
        )
    assert _h._PATCH_SURVIVAL_MATERIALIZED_ITERS == set()
