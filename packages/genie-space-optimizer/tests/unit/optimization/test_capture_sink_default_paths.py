"""Permanent regression test — capture sinks default to a deterministic
local path so a UI-triggered trial run produces NDJSON without the
operator setting GSO_*_CAPTURE_PATH.

If any of these defaults change, update both this test and
``scripts/regen_fixtures_from_bundle.py`` (which assumes the same
path layout).
"""
from __future__ import annotations

import importlib
import os
import sys

import pytest


_DEFAULT_PATHS = {
    "_NARROWING_CAPTURE_SINK": "/tmp/gso_trial_captures/narrowing_v1.ndjson",
    "_LEVER_FIVE_CAPTURE_SINK": "/tmp/gso_trial_captures/lever5_split_v1.ndjson",
    "_THREE_STAGE_CAPTURE_SINK": "/tmp/gso_trial_captures/three_stage_v1.ndjson",
    "_RAW_EVIDENCE_CAPTURE_SINK": "/tmp/gso_trial_captures/raw_evidence_v1.ndjson",
}

_ENV_KEYS = {
    "_NARROWING_CAPTURE_SINK": "GSO_NARROWING_CAPTURE_PATH",
    "_LEVER_FIVE_CAPTURE_SINK": "GSO_LEVER5_SPLIT_CAPTURE_PATH",
    "_THREE_STAGE_CAPTURE_SINK": "GSO_THREE_STAGE_CAPTURE_PATH",
    "_RAW_EVIDENCE_CAPTURE_SINK": "GSO_RAW_EVIDENCE_CAPTURE_PATH",
}


def _reload_config_no_env() -> object:
    for env_key in _ENV_KEYS.values():
        os.environ.pop(env_key, None)
    sys.modules.pop("genie_space_optimizer.common.config", None)
    return importlib.import_module("genie_space_optimizer.common.config")


@pytest.mark.parametrize("sink_attr,expected_path", list(_DEFAULT_PATHS.items()))
def test_sink_resolves_default_path_when_env_unset(
    sink_attr: str, expected_path: str,
):
    cfg = _reload_config_no_env()
    sink = getattr(cfg, sink_attr)
    sink.reset_for_test()
    resolved = sink._resolve_sink_path()  # noqa: SLF001
    assert resolved == expected_path, (
        f"{sink_attr} resolved {resolved!r}, expected {expected_path!r}. "
        f"If you intentionally changed the default, update "
        f"scripts/regen_fixtures_from_bundle.py to match."
    )


@pytest.mark.parametrize("sink_attr,env_key", list(_ENV_KEYS.items()))
def test_sink_honors_env_var_override(
    sink_attr: str, env_key: str, tmp_path,
):
    custom = str(tmp_path / "custom.ndjson")
    os.environ[env_key] = custom
    try:
        sys.modules.pop("genie_space_optimizer.common.config", None)
        cfg = importlib.import_module("genie_space_optimizer.common.config")
        sink = getattr(cfg, sink_attr)
        sink.reset_for_test()
        resolved = sink._resolve_sink_path()  # noqa: SLF001
        assert resolved == custom
    finally:
        os.environ.pop(env_key, None)
