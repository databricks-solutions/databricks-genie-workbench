"""RCO-4b consolidating-trial preflight — proves the codebase is
ready to submit the trial.

Asserts:
1. All nine feature-flag accessors exist and default to False.
2. The harness imports each accessor at the expected line range
   (catches accidental flag rename / removal during Phase A–E hygiene
   sweeps).
3. The sequence-guard test for ``_run_gate_checks`` audit ordering
   still passes.

Gate: this test MUST pass before the operator runs Task 6.
"""

from __future__ import annotations

import importlib

import pytest

NINE_FLAGS = (
    ("GSO_STAGE6_BLAST_RADIUS_PURE", "stage6_blast_radius_pure_enabled"),
    ("GSO_STAGE6_NARROW_REPL_PURE", "stage6_narrow_repl_pure_enabled"),
    ("GSO_STAGE6_APPLYABILITY_PURE", "stage6_applyability_pure_enabled"),
    (
        "GSO_GATE_CHECKS_PROPAGATION_PURE",
        "gate_checks_propagation_pure_enabled",
    ),
    ("GSO_GATE_CHECKS_SLICE_PURE", "gate_checks_slice_pure_enabled"),
    ("GSO_GATE_CHECKS_P0_PURE", "gate_checks_p0_pure_enabled"),
    (
        "GSO_GATE_CHECKS_ASI_EXTRACTION_PURE",
        "gate_checks_asi_extraction_pure_enabled",
    ),
    (
        "GSO_GATE_CHECKS_BASELINE_DRIFT_PURE",
        "gate_checks_baseline_drift_pure_enabled",
    ),
    (
        "GSO_GATE_CHECKS_FULL_EVAL_ACCEPTANCE_PURE",
        "gate_checks_full_eval_acceptance_pure_enabled",
    ),
)


@pytest.fixture
def fresh_config(monkeypatch):
    """Reload config with every trial env var cleared so default-off
    behavior is observable."""
    for env_var, _ in NINE_FLAGS:
        monkeypatch.delenv(env_var, raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    return config


@pytest.mark.parametrize("env_var,accessor_name", NINE_FLAGS)
def test_each_flag_accessor_exists_and_defaults_off(
    env_var, accessor_name, fresh_config
):
    accessor = getattr(fresh_config, accessor_name, None)
    assert accessor is not None, (
        f"missing accessor {accessor_name} for env var {env_var} "
        f"— RCO-4b trial submission blocked"
    )
    assert accessor() is False, (
        f"{accessor_name}() must default to False when {env_var} is "
        f"unset; got True"
    )


@pytest.mark.parametrize("env_var,accessor_name", NINE_FLAGS)
def test_each_flag_accessor_honors_truthy_env_var(
    env_var, accessor_name, monkeypatch
):
    monkeypatch.setenv(env_var, "1")
    from genie_space_optimizer.common import config

    importlib.reload(config)
    accessor = getattr(config, accessor_name)
    assert accessor() is True, (
        f"{accessor_name}() must return True when {env_var}=1; got False"
    )


def test_harness_imports_all_nine_accessors():
    """Grep guard: the harness must reference every accessor by name
    at least once. Catches accidental removal of the flag-gated
    delegation in the harness body."""
    from pathlib import Path

    harness_src = (
        Path(__file__).parent.parent.parent
        / "src"
        / "genie_space_optimizer"
        / "optimization"
        / "harness.py"
    ).read_text()
    for _, accessor_name in NINE_FLAGS:
        assert accessor_name in harness_src, (
            f"harness.py does not reference {accessor_name} — the "
            f"RCO-4 / RCO-4b delegation may have been removed"
        )


def test_lever_loop_strict_default_unchanged():
    """The lever-loop job must still default ``GSO_LOOP_INVARIANTS_STRICT``
    to '0'. RCO-2b will flip this; before RCO-2b ships, the trial must
    run under the same posture as production."""
    from pathlib import Path

    job_src = (
        Path(__file__).parent.parent.parent
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_lever_loop.py"
    ).read_text()
    assert (
        '_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")'
        in job_src
    ), (
        "RCO-2a structural guard violated: lever-loop job no longer "
        "defaults strict mode to '0'. RCO-2b must flip this — but "
        "only after the trial captures the marker payload."
    )
