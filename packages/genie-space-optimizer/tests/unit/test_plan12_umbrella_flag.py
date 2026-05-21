"""Plan 12 umbrella flag tests — ``GSO_PLAN12_LIVE_ALL`` flips every
Plan 12 sub-flag ON for the canary, with explicit per-flag overrides
taking precedence.

Override semantics:

  * Sub-flag env var TRUTHY  → sub-flag ON regardless of umbrella
  * Sub-flag env var FALSY   → sub-flag OFF regardless of umbrella
  * Sub-flag env var UNSET   → sub-flag defaults to umbrella state

The canary deploy sets ``GSO_PLAN12_LIVE_ALL=true`` once and all
seven Plan 12 live-wires activate together. If a specific wire
misbehaves, the operator overrides only that sub-flag.
"""
import os
from unittest.mock import patch


SUB_FLAGS = (
    (
        "GSO_PLAN12_LIVE_NARROW_REPLACEMENT",
        "plan12_live_narrow_replacement_enabled",
    ),
    (
        "GSO_PLAN12_LIVE_AG_RETRY_PIVOT",
        "plan12_live_ag_retry_pivot_enabled",
    ),
    (
        "GSO_PLAN12_LIVE_AG_RETRY_PIVOT_MUTATE",
        "plan12_live_ag_retry_pivot_mutate_enabled",
    ),
    (
        "GSO_PLAN12_LIVE_EVIDENCE_ROUTING",
        "plan12_live_evidence_routing_enabled",
    ),
    (
        "GSO_PLAN12_LIVE_PROPOSAL_ATTEMPTS_DERIVE",
        "plan12_live_proposal_attempts_derive_enabled",
    ),
    (
        "GSO_PLAN12_LIVE_RUN_SUMMARY_EVAL_DERIVE",
        "plan12_live_run_summary_eval_derive_enabled",
    ),
    (
        "GSO_PLAN12_LIVE_L6_APPLIER_EMIT_OUTCOMES",
        "plan12_live_l6_applier_emit_outcomes_enabled",
    ),
)


def _clear_plan12_env() -> dict[str, str]:
    """Return an empty env dict (used with patch.dict to start from a
    clean slate)."""
    return {}


def _call(fn_name: str) -> bool:
    from genie_space_optimizer.common import config as _cfg
    return getattr(_cfg, fn_name)()


# ── Umbrella flag itself ──────────────────────────────────────────────


def test_umbrella_off_by_default():
    from genie_space_optimizer.common.config import (
        plan12_live_all_enabled,
    )
    with patch.dict(os.environ, _clear_plan12_env(), clear=False):
        # Strip any pre-existing setting from the OS env.
        for env_name, _ in SUB_FLAGS:
            os.environ.pop(env_name, None)
        os.environ.pop("GSO_PLAN12_LIVE_ALL", None)
        assert plan12_live_all_enabled() is False


def test_umbrella_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_all_enabled,
    )
    for val in ("true", "1", "yes", "on"):
        with patch.dict(os.environ, {"GSO_PLAN12_LIVE_ALL": val}):
            assert plan12_live_all_enabled() is True, (
                f"Expected True for {val!r}"
            )


# ── Override semantics — umbrella enables every sub-flag ──────────────


def test_umbrella_on_enables_every_sub_flag():
    """The canary scenario: setting only ``GSO_PLAN12_LIVE_ALL=true``
    causes all seven sub-flags to report ON."""
    with patch.dict(os.environ, {"GSO_PLAN12_LIVE_ALL": "true"}):
        for env_name, fn_name in SUB_FLAGS:
            # Ensure no per-flag override is set.
            os.environ.pop(env_name, None)
            assert _call(fn_name) is True, (
                f"sub-flag {fn_name} did NOT pick up the umbrella "
                f"default; canary deploy would silently miss the wire"
            )


def test_umbrella_off_leaves_every_sub_flag_off():
    """The default state: no umbrella, no per-flag setting → all
    sub-flags OFF. Preserves byte-stable replay."""
    with patch.dict(os.environ, _clear_plan12_env(), clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_ALL", None)
        for env_name, fn_name in SUB_FLAGS:
            os.environ.pop(env_name, None)
            assert _call(fn_name) is False, (
                f"sub-flag {fn_name} reported ON without umbrella OR "
                f"per-flag setting — breaks byte-stable replay"
            )


# ── Override semantics — per-flag truthy overrides umbrella OFF ───────


def test_per_flag_truthy_overrides_umbrella_off():
    """An operator can enable a SINGLE sub-flag for a targeted test
    without flipping the umbrella."""
    with patch.dict(os.environ, _clear_plan12_env(), clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_ALL", None)
        for env_name, fn_name in SUB_FLAGS:
            # Set ONLY this sub-flag truthy; umbrella stays off.
            with patch.dict(os.environ, {env_name: "true"}):
                assert _call(fn_name) is True, (
                    f"sub-flag {fn_name} did NOT honor its own "
                    f"truthy override when umbrella is OFF"
                )


# ── Override semantics — per-flag falsy overrides umbrella ON ─────────


def test_per_flag_falsy_overrides_umbrella_on():
    """Mid-canary, an operator can disable a SINGLE misbehaving
    sub-flag without unsetting the umbrella. The other six remain ON."""
    for env_name, fn_name in SUB_FLAGS:
        # Clean slate, then umbrella on + this sub-flag explicitly off.
        env_block = {
            "GSO_PLAN12_LIVE_ALL": "true",
            env_name: "false",
        }
        # Need to strip OTHER sub-flag overrides too so they pick up
        # the umbrella ON.
        with patch.dict(os.environ, _clear_plan12_env(), clear=False):
            for other_env, _ in SUB_FLAGS:
                if other_env != env_name:
                    os.environ.pop(other_env, None)
            with patch.dict(os.environ, env_block):
                assert _call(fn_name) is False, (
                    f"sub-flag {fn_name} did NOT honor its own falsy "
                    f"override when umbrella is ON — canary cannot "
                    f"disable a misbehaving wire"
                )
                # The OTHER sub-flags should still be ON.
                for other_env, other_fn in SUB_FLAGS:
                    if other_env == env_name:
                        continue
                    assert _call(other_fn) is True, (
                        f"disabling {fn_name} broke {other_fn} — "
                        f"per-flag override leaked"
                    )


# ── Tri-state helper itself ───────────────────────────────────────────


def test_flag_state_tri_state():
    from genie_space_optimizer.common.config import _flag_state
    with patch.dict(os.environ, {"X_TEST_FLAG": "true"}):
        assert _flag_state("X_TEST_FLAG") == "on"
    with patch.dict(os.environ, {"X_TEST_FLAG": "false"}):
        assert _flag_state("X_TEST_FLAG") == "off"
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("X_TEST_FLAG", None)
        assert _flag_state("X_TEST_FLAG") == "unset"


def test_flag_state_normalizes_case_and_whitespace():
    from genie_space_optimizer.common.config import _flag_state
    for raw in (" True ", "TRUE", "  yes\n", "On"):
        with patch.dict(os.environ, {"X_TEST_FLAG": raw}):
            assert _flag_state("X_TEST_FLAG") == "on", (
                f"Expected 'on' for {raw!r}"
            )
    for raw in (" False ", "OFF", "  no\n"):
        with patch.dict(os.environ, {"X_TEST_FLAG": raw}):
            assert _flag_state("X_TEST_FLAG") == "off", (
                f"Expected 'off' for {raw!r}"
            )


# ── Notebook canary setdefault ────────────────────────────────────────


def test_run_lever_loop_notebook_sets_umbrella_default():
    """The lever-loop notebook stamps ``GSO_PLAN12_LIVE_ALL=true`` via
    ``os.environ.setdefault`` so the canary deploy sees every wire
    activated. Static check: the line exists in the notebook source."""
    from pathlib import Path
    notebook_path = (
        Path(__file__).parent.parent.parent
        / "src" / "genie_space_optimizer" / "jobs"
        / "run_lever_loop.py"
    )
    text = notebook_path.read_text()
    assert 'GSO_PLAN12_LIVE_ALL' in text, (
        "lever-loop notebook is missing the Plan 12 umbrella default; "
        "canary would not activate any of the live-wires"
    )
    assert 'setdefault' in text and 'GSO_PLAN12_LIVE_ALL' in text, (
        "Plan 12 umbrella default must use setdefault so operators "
        "can override via the Databricks job spec"
    )
