"""RCO-4b consolidating-trial preflight — proves the codebase is
ready to submit the trial via the standard lever-loop submission
path (no per-submission env-var setup).

Asserts:
1. All nine feature-flag accessors exist and default to True (the
   default-ON posture introduced by the RCO-4b consolidating-trial
   plan; the lever-loop job exercises every new pure helper on the
   standard submission path).
2. Setting the env var to a falsy value disables — the rollback
   escape hatch.
3. The harness imports each accessor by name (catches accidental
   flag rename / removal during Phase A–E hygiene sweeps).
4. The lever-loop job still defaults ``GSO_LOOP_INVARIANTS_STRICT``
   to ``"0"`` (RCO-2b owns that flip, not this trial).

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
def fresh_config_no_env(monkeypatch):
    """Reload config with every trial env var cleared so default-ON
    behavior is observable."""
    for env_var, _ in NINE_FLAGS:
        monkeypatch.delenv(env_var, raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    return config


@pytest.mark.parametrize("env_var,accessor_name", NINE_FLAGS)
def test_each_flag_accessor_defaults_on(
    env_var, accessor_name, fresh_config_no_env
):
    accessor = getattr(fresh_config_no_env, accessor_name, None)
    assert accessor is not None, (
        f"missing accessor {accessor_name} for env var {env_var} "
        f"— RCO-4b trial submission blocked"
    )
    assert accessor() is True, (
        f"{accessor_name}() must default to True when {env_var} is "
        f"unset; got False. The RCO-4b consolidating-trial plan "
        f"flipped this default — restore the flip in config.py."
    )


@pytest.mark.parametrize("env_var,accessor_name", NINE_FLAGS)
@pytest.mark.parametrize("falsy_value", ["0", "false", "False", "no", "off"])
def test_each_flag_accessor_honors_falsy_env_var(
    env_var, accessor_name, falsy_value, monkeypatch
):
    """Rollback escape hatch — setting the env var to a falsy value
    must restore the legacy code path."""
    monkeypatch.setenv(env_var, falsy_value)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    accessor = getattr(config, accessor_name)
    assert accessor() is False, (
        f"{accessor_name}() must return False when "
        f"{env_var}={falsy_value!r}; got True. Rollback escape hatch "
        f"is broken."
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


def test_accessors_use_flag_default_on_helper():
    """The default-flip uses the canonical ``_flag_default_on`` helper
    (the same one ``gso_contract_health_summary_enabled`` uses). Each
    of the nine accessor bodies must reference it."""
    from pathlib import Path

    config_src = (
        Path(__file__).parent.parent.parent
        / "src"
        / "genie_space_optimizer"
        / "common"
        / "config.py"
    ).read_text()
    for env_var, _ in NINE_FLAGS:
        marker = f'_flag_default_on("{env_var}")'
        assert marker in config_src, (
            f"config.py is missing the default-on accessor body "
            f"{marker!r}. The RCO-4b default-flip did not land for "
            f"this flag."
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
