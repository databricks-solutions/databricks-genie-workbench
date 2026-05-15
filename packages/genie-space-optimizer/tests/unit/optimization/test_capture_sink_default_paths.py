"""Permanent regression test — capture sinks resolve sink_path purely
from the ``GSO_*_CAPTURE_PATH`` env var.

Plan 5 (Option B) — sinks always buffer in memory and are uploaded to
MLflow at end-of-run via ``MlflowClient.log_text``. There is no
filesystem-staging default anymore: when the env var is unset, the
sink resolves ``sink_path`` to ``None`` and only the in-memory buffer
holds records. The previous ``/tmp/gso_trial_captures/...`` defaults
silently dropped every record on Databricks serverless because
``/tmp`` is not writable there (PermissionError [Errno 13]).

The env-var path is retained as a debug-only opt-in: setting
``GSO_NARROWING_CAPTURE_PATH=/path/to/file.ndjson`` (etc.) ALSO
mirrors each record to disk for offline inspection. The MLflow
upload still happens regardless.
"""
from __future__ import annotations

import importlib
import os
import sys

import pytest


_SINK_ATTRS = (
    "_NARROWING_CAPTURE_SINK",
    "_LEVER_FIVE_CAPTURE_SINK",
    "_THREE_STAGE_CAPTURE_SINK",
    "_RAW_EVIDENCE_CAPTURE_SINK",
)

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


@pytest.mark.parametrize("sink_attr", list(_SINK_ATTRS))
def test_sink_resolves_to_none_when_env_unset(sink_attr: str):
    """Plan 5 (Option B): no auto-/tmp default. Default sink_path is None
    so the in-memory buffer is the sole source of truth."""
    cfg = _reload_config_no_env()
    sink = getattr(cfg, sink_attr)
    sink.reset_for_test()
    resolved = sink._resolve_sink_path()  # noqa: SLF001
    assert resolved is None, (
        f"{sink_attr} resolved {resolved!r}, expected None. "
        f"If you re-introduce a /tmp default, update Option B's "
        f"in-memory upload contract too."
    )


@pytest.mark.parametrize("sink_attr,env_key", list(_ENV_KEYS.items()))
def test_sink_honors_env_var_override(
    sink_attr: str, env_key: str, tmp_path,
):
    """Debug-only opt-in still works: setting GSO_*_CAPTURE_PATH mirrors
    records to that file in addition to the in-memory buffer."""
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


@pytest.mark.parametrize("sink_attr", list(_SINK_ATTRS))
def test_sink_exposes_consume_records_for_in_memory_uploads(sink_attr: str):
    """Plan 5 (Option B) contract: every sink must expose
    ``consume_records()`` so the harness post-loop hook can serialize
    the buffer to NDJSON and upload via MlflowClient.log_text."""
    cfg = _reload_config_no_env()
    sink = getattr(cfg, sink_attr)
    sink.reset_for_test()
    assert callable(getattr(sink, "consume_records", None)), (
        f"{sink_attr} is missing consume_records(); the in-memory "
        f"upload helper relies on it."
    )
    initial = sink.consume_records()
    assert isinstance(initial, tuple)
    assert initial == ()
