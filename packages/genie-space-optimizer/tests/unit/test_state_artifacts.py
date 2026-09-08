from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from genie_space_optimizer.optimization import ddl, state


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


def test_the_ddl_column_comment_enumerates_exactly_the_registered_kinds() -> None:
    """The comment is the enumeration a reader of the table sees, so it has to be
    the enumeration the code enforces.

    It was byte-for-byte in sync with ``ARTIFACT_KINDS`` before ``mv_candidate_ddl``
    was added, which makes it a maintained list rather than an already-drifted one —
    and the reason to pin it now is that nothing else would have caught the drift.
    An unregistered kind only warns, and a column comment is invisible to every
    test that exercises a write.
    """
    import re

    ddl_src = Path(ddl.__file__).read_text(encoding="utf-8")
    match = re.search(
        r"artifact_kind\s+STRING\s+NOT NULL COMMENT '([^']*)'", ddl_src
    )
    assert match, "the artifact_kind column comment moved or changed shape"
    documented = {part.strip() for part in match.group(1).split("|")}

    assert documented == set(state.ARTIFACT_KINDS)


def test_an_explicit_content_hash_overrides_the_payload_digest(monkeypatch) -> None:
    """MV-D7's cross-reference key, which the default digest cannot be.

    ``genie_opt_mv_candidates.dedup_fingerprint`` is documented as "also the
    content_hash of the rendered-DDL genie_opt_artifacts row for this candidate",
    and that was false until this override existed. It has to be an override
    rather than the default: for every other artifact the blob's identity *is* its
    bytes, and only the metric view rows key on something the bytes do not
    determine — MV-D15 has Prompt 9 regenerate the YAML under different
    capabilities, so the same candidate's text legitimately changes.
    """
    captured: dict = {}
    monkeypatch.setattr(
        state, "insert_row",
        lambda _spark, _c, _s, _t, row, **kw: captured.update(row=row),
    )
    fingerprint = "9f2a" * 16

    state.write_artifact(
        object(),
        "run",
        "mv_candidate_ddl",
        {"ddl": "CREATE VIEW x WITH METRICS LANGUAGE YAML AS $$version: 1.1$$"},
        catalog="cat",
        schema="sch",
        stage_name="mv_advisor",
        content_hash=fingerprint,
    )

    raw = captured["row"]["artifact_json"]
    assert captured["row"]["content_hash"] == fingerprint
    assert captured["row"]["content_hash"] != hashlib.sha256(raw.encode("utf-8")).hexdigest()


def test_the_default_content_hash_is_unchanged_for_every_other_kind(monkeypatch) -> None:
    """The override must not have moved the default for existing callers."""
    captured: dict = {}
    monkeypatch.setattr(
        state, "insert_row",
        lambda _spark, _c, _s, _t, row, **kw: captured.update(row=row),
    )

    state.write_artifact(
        object(), "run", "publish_record", {"a": 1},
        catalog="cat", schema="sch", stage_name="publish_and_audit",
    )

    raw = captured["row"]["artifact_json"]
    assert captured["row"]["content_hash"] == hashlib.sha256(raw.encode("utf-8")).hexdigest()
