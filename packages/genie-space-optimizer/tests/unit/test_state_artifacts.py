from __future__ import annotations

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
