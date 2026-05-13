"""Default-on flip inventory for three observability-only flags.

Closes the wiring gap identified in the 2314bb2c trial postmortem:
the trial's executor ran with all commits deployed yet the markers
for these flags' code paths were silent because every accessor
defaulted OFF.

Asserts:
1. Each flag accessor defaults to True (post-flip behavior).
2. Setting the env var to a falsy value disables the path
   (rollback escape hatch — same contract as the RCO-4b nine).
3. Each accessor body uses the canonical ``_flag_default_on`` helper
   (catches accidental rewrites of the body).
4. Each gated production code path still references the accessor
   (catches accidental orphaning of the flag — same grep guard as
   ``test_rco4b_trial_preflight_flag_inventory``).

Mirrors ``tests/unit/test_rco4b_trial_preflight_flag_inventory.py``
so the in-repo pattern is consistent.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


# (env_var, accessor_name, production_callsite_substring)
THREE_FLAGS = (
    (
        "GSO_PROPOSAL_FAILURE_DECIDED",
        "proposal_failure_decided_enabled",
        "proposal_failure_decided_enabled",
    ),
    (
        "GSO_STAGE4_CONTEXT_PERSISTENCE",
        "stage4_context_persistence_enabled",
        "stage4_context_persistence_enabled",
    ),
    (
        "GSO_PATCH_SUBSET_ISOLATION",
        "patch_subset_isolation_enabled",
        "patch_subset_isolation_enabled",
    ),
)


@pytest.fixture
def fresh_config_no_env(monkeypatch):
    for env_var, _, _ in THREE_FLAGS:
        monkeypatch.delenv(env_var, raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)
    return config


@pytest.mark.parametrize("env_var,accessor_name,_callsite", THREE_FLAGS)
def test_each_flag_accessor_defaults_on(
    env_var, accessor_name, _callsite, fresh_config_no_env
):
    accessor = getattr(fresh_config_no_env, accessor_name, None)
    assert accessor is not None, (
        f"missing accessor {accessor_name} for env var {env_var}"
    )
    assert accessor() is True, (
        f"{accessor_name}() must default to True when {env_var} is unset; "
        f"got False. The default-on flip plan flipped this default — "
        f"restore the flip in config.py."
    )


@pytest.mark.parametrize("env_var,accessor_name,_callsite", THREE_FLAGS)
@pytest.mark.parametrize("falsy_value", ["0", "false", "False", "no", "off"])
def test_each_flag_accessor_honors_falsy_env_var(
    env_var, accessor_name, _callsite, falsy_value, monkeypatch
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


def test_accessors_use_flag_default_on_helper():
    """Each flipped accessor body must reference the canonical
    ``_flag_default_on(env_name)`` helper. Catches accidental
    rewrites that bring back ``_flag_enabled``."""
    config_src = (
        Path(__file__).parent.parent.parent
        / "src"
        / "genie_space_optimizer"
        / "common"
        / "config.py"
    ).read_text()
    for env_var, _, _ in THREE_FLAGS:
        marker = f'_flag_default_on("{env_var}")'
        assert marker in config_src, (
            f"config.py is missing the default-on accessor body "
            f"{marker!r}. The default-on flip did not land for this flag."
        )


@pytest.mark.parametrize("_env_var,_accessor,callsite", THREE_FLAGS)
def test_production_call_site_exists(_env_var, _accessor, callsite):
    """Grep guard: each flipped flag must remain referenced from
    production source code outside ``config.py`` itself. Catches
    accidental orphaning (the same antipattern we are deleting
    in Task 7 for GSO_PHASE_H_CANONICAL_CONSUMER)."""
    src_root = (
        Path(__file__).parent.parent.parent
        / "src"
        / "genie_space_optimizer"
    )
    referenced_outside_config = False
    for path in src_root.rglob("*.py"):
        if path.name == "config.py":
            continue
        if callsite in path.read_text():
            referenced_outside_config = True
            break
    assert referenced_outside_config, (
        f"{callsite} has no production call site — flag is orphaned. "
        f"Either wire it or delete the accessor."
    )
