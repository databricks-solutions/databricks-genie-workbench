"""Cycle 14-W T3 — ``_databricks_ids_from_env`` must emit a
``GSO_DATABRICKS_IDS_RESOLVED_V1`` marker recording which
resolution path fired.

Discipline B: multi-path resolvers ship tracing markers, not just
decision functions, so postmortems can catch
function-reached-but-wrong-path regressions like D-5.
"""

from __future__ import annotations

import io
import json
import os
import re
from contextlib import redirect_stdout
from unittest.mock import patch

from genie_space_optimizer.optimization.harness import (
    _databricks_ids_from_env,
)


def _extract_trace_payload(stdout_text: str) -> dict:
    match = re.search(
        r"GSO_DATABRICKS_IDS_RESOLVED_V1\s+(\{.*\})", stdout_text
    )
    assert match is not None, stdout_text
    return json.loads(match.group(1))


def test_env_var_path_emits_trace(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_JOB_ID", "488860692117207")
    monkeypatch.setenv("DATABRICKS_RUN_ID", "503586982599093")
    monkeypatch.setenv("DATABRICKS_TASK_RUN_ID", "1105451933925748")

    buf = io.StringIO()
    with redirect_stdout(buf):
        ids = _databricks_ids_from_env()

    assert ids["databricks_job_id"] == "488860692117207"
    assert ids["databricks_parent_run_id"] == "503586982599093"
    assert ids["lever_loop_task_run_id"] == "1105451933925748"

    payload = _extract_trace_payload(buf.getvalue())
    assert payload["resolution_path"] == "env"
    assert payload["fields_resolved"] == 3
    assert payload["fields_total"] == 3
    assert payload["dbutils_attempted"] is False


def test_sentinel_path_emits_trace(monkeypatch) -> None:
    """Env vars unset and dbutils unavailable → every field is
    sentinel and the trace records ``resolution_path=sentinel``."""
    for var in (
        "DATABRICKS_JOB_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    buf = io.StringIO()
    with redirect_stdout(buf):
        ids = _databricks_ids_from_env()

    assert all(v == "unknown" for v in ids.values())

    payload = _extract_trace_payload(buf.getvalue())
    assert payload["resolution_path"] == "sentinel"
    assert payload["fields_resolved"] == 0
    assert payload["dbutils_attempted"] is True
    assert payload["dbutils_succeeded"] is False


def test_resolver_never_returns_blank(monkeypatch) -> None:
    """Invariant: every key in the returned dict has a non-empty
    string value. Sentinel is ``'unknown'``, never ``''``."""
    for var in (
        "DATABRICKS_JOB_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    ids = _databricks_ids_from_env()
    assert all(v != "" for v in ids.values()), ids


def test_trace_silent_when_flag_off(monkeypatch) -> None:
    """``GSO_DATABRICKS_IDS_RESOLUTION_TRACE=0`` suppresses the
    trace marker; the resolver still returns the canonical sentinel
    map."""
    monkeypatch.setenv("GSO_DATABRICKS_IDS_RESOLUTION_TRACE", "0")
    for var in (
        "DATABRICKS_JOB_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    buf = io.StringIO()
    with redirect_stdout(buf):
        ids = _databricks_ids_from_env()

    assert all(v == "unknown" for v in ids.values())
    assert "GSO_DATABRICKS_IDS_RESOLVED_V1" not in buf.getvalue()
