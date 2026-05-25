"""Trial 13j — ``write_capture`` v2.1 / v1 schema version selection.

The v2 schema is opt-in: only when ``schema_columns`` is non-empty does
``write_capture`` upgrade the payload version. Empty/omitted columns
preserve v1 for backward compat with existing capture bundles.

Trial 14 bumped the v2 token to ``workbench_eval_capture_v2.1`` to
signal that rows now carry typed ASI ``blame_set_structured`` flat keys.
The bundle loader (``input_bundle.from_run_analysis_dir``) accepts both
``v2`` and ``v2.1`` so existing fixtures keep loading.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from local_lever_workbench.mlflow_eval_capture import (
    CaptureSpec,
    default_output_path,
    write_capture,
)


def _spec(genie_space_id: str = "01f143dfbeec15a3a0e87ced8662f4ed") -> CaptureSpec:
    return CaptureSpec(
        experiment_id="123",
        experiment_name="/test/exp",
        optimization_run_id="run-uuid",
        job_id="job-1",
        task_run_id="task-1",
        task_key="enrichment",
        genie_space_id=genie_space_id,
    )


def _output_path(tmp_path: Path) -> Path:
    return default_output_path(
        docs_root=tmp_path,
        optimization_run_id="run-uuid",
        task_run_id="task-1",
    )


@pytest.mark.workbench
def test_write_capture_with_columns_emits_v2(tmp_path: Path) -> None:
    """Non-empty ``schema_columns`` triggers the v2.1 envelope.

    The bundle carries both ``serialized_space`` (so the loader can
    merge into ``metadata_snapshot``) and ``schema_columns`` (so Trial
    13i ``_derive_schema_columns`` priority step 1 fires).

    Trial 14 bumped the v2 token to ``workbench_eval_capture_v2.1`` to
    signal that rows now carry typed ASI blame_set_structured flat keys.
    ``input_bundle.from_run_analysis_dir`` accepts both ``v2`` and
    ``v2.1`` (see ``input_bundle.py``), so the wire-level evolution is
    additive — only the writer's emitted token changed.
    """
    spec = _spec()
    ss = {"version": 1, "data_sources": {"tables": [], "metric_views": []}}
    cols = ("ctx.sch.tbl.colA", "ctx.sch.tbl.colB")
    written = write_capture(
        spec=spec,
        rows=[{"request": "{}", "response": "{}", "_asi_source": "qid_X"}],
        output_path=_output_path(tmp_path),
        serialized_space=ss,
        schema_columns=cols,
        schema_columns_source="genie_api",
    )
    payload = json.loads(written.read_text())
    assert payload["_schema_version"] == "workbench_eval_capture_v2.1"
    assert payload["schema_columns"] == list(cols)
    assert payload["serialized_space"] == ss
    assert payload["_provenance"]["genie_space_id"] == spec.genie_space_id
    assert payload["_provenance"]["schema_columns_source"] == "genie_api"
    assert payload["_provenance"]["schema_columns_count"] == 2


@pytest.mark.workbench
def test_write_capture_without_columns_stays_v1(tmp_path: Path) -> None:
    """Empty ``schema_columns`` preserves the v1 envelope.

    Older operator workflows that call ``write_capture`` without the
    Trial 13j kwargs must produce a v1-shaped file so the existing
    fixtures keep loading. The provenance still records the absence
    of schema_columns so postmortems can surface the deficit.
    """
    spec = _spec(genie_space_id="")
    written = write_capture(
        spec=spec,
        rows=[{"request": "{}", "response": "{}", "_asi_source": "qid_X"}],
        output_path=_output_path(tmp_path),
    )
    payload = json.loads(written.read_text())
    assert payload["_schema_version"] == "workbench_eval_capture_v1"
    assert "schema_columns" not in payload
    assert "serialized_space" not in payload
    assert payload["_provenance"]["schema_columns_count"] == 0
    assert payload["_provenance"]["schema_columns_source"] == ""


@pytest.mark.workbench
def test_write_capture_v1_default_kwargs_unchanged(tmp_path: Path) -> None:
    """V1 callers (Trial 13h and earlier) keep their behavior.

    The pre-Trial-13j ``write_capture(spec=..., rows=..., output_path=...)``
    call site must continue to work with no extra kwargs — and produce
    a v1 payload identical in structure (modulo two informational
    provenance fields that default to empty for v1).
    """
    spec = _spec(genie_space_id="")
    written = write_capture(
        spec=spec,
        rows=[{"request": "{}", "response": "{}", "_asi_source": "qid_X"}],
        output_path=_output_path(tmp_path),
    )
    payload = json.loads(written.read_text())
    assert payload["_schema_version"] == "workbench_eval_capture_v1"
    # eval_rows shape preserved
    assert isinstance(payload["eval_rows"], list)
    assert len(payload["eval_rows"]) == 1
