from __future__ import annotations

import hashlib
import json

import pandas as pd

from genie_space_optimizer.optimization import state


def test_space_metadata_is_a_supported_artifact_kind() -> None:
    assert "space_metadata" in state.ARTIFACT_KINDS


def test_load_latest_artifact_payload_parses_newest_json(monkeypatch) -> None:
    monkeypatch.setattr(
        state,
        "load_artifacts",
        lambda *_args, **_kwargs: pd.DataFrame([
            {"artifact_json": '{"version": 1, "uc_columns": []}'},
        ]),
    )

    payload = state.load_latest_artifact_payload(
        object(),
        "run",
        "cat",
        "sch",
        "space_metadata",
    )

    assert payload == {"version": 1, "uc_columns": []}


def test_load_latest_artifact_payload_rejects_non_object(monkeypatch) -> None:
    monkeypatch.setattr(
        state,
        "load_artifacts",
        lambda *_args, **_kwargs: pd.DataFrame([
            {"artifact_json": '[1, 2, 3]'},
        ]),
    )

    assert state.load_latest_artifact_payload(
        object(),
        "run",
        "cat",
        "sch",
        "space_metadata",
    ) is None


def test_write_artifact_uses_byte_preserving_json_transport(monkeypatch) -> None:
    captured: dict = {}

    def _capture(_spark, _catalog, _schema, _table, row, **kwargs) -> None:
        captured["row"] = row
        captured["kwargs"] = kwargs

    monkeypatch.setattr(state, "insert_row", _capture)
    payload = {
        "description": "O'Brien\\nSnowman: ☃",
        "expression": r"CASE WHEN path = 'C:\\tmp' THEN 1 END",
    }

    artifact_id = state.write_artifact(
        object(),
        "run",
        "wide_schema_inventory",
        payload,
        catalog="cat",
        schema="sch",
        stage_name="intake_and_snapshot",
    )

    raw = captured["row"]["artifact_json"]
    assert artifact_id == captured["row"]["artifact_id"]
    assert json.loads(raw) == payload
    assert captured["row"]["content_hash"] == hashlib.sha256(raw.encode("utf-8")).hexdigest()
    assert captured["kwargs"] == {"base64_string_columns": {"artifact_json"}}
