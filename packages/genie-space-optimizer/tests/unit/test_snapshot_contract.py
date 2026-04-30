from __future__ import annotations


def test_snapshot_digest_ignores_runtime_private_keys() -> None:
    from genie_space_optimizer.optimization.snapshot_contract import snapshot_digest

    left = {
        "data_sources": {"tables": [{"name": "sales"}]},
        "_uc_columns": [{"name": "runtime-only"}],
    }
    right = {
        "data_sources": {"tables": [{"name": "sales"}]},
        "_different_runtime_key": "ignored",
    }

    assert snapshot_digest(left) == snapshot_digest(right)


def test_compare_live_to_expected_uses_live_parsed_space_not_full_api_response(monkeypatch) -> None:
    from genie_space_optimizer.optimization import snapshot_contract

    parsed = {
        "instructions": {"text_instructions": [{"content": "before"}]},
        "data_sources": {"tables": [{"name": "sales"}]},
    }
    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: {
            "id": "space_1",
            "title": "Runtime metadata must not participate in rollback compare",
            "serialized_space": "{\"instructions\": {}}",
            "_parsed_space": dict(parsed),
            "_uc_columns": [{"runtime": "ignored"}],
        },
    )

    result = snapshot_contract.compare_live_to_expected_snapshot(
        w=object(),
        space_id="space_1",
        expected_snapshot=dict(parsed),
    )

    assert result["verified"] is True
    assert result["reason"] == "matched_pre_snapshot"
    assert result["expected_digest"] == result["live_digest"]


def test_compare_live_to_expected_reports_first_meaningful_diff(monkeypatch) -> None:
    from genie_space_optimizer.optimization import snapshot_contract

    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: {
            "_parsed_space": {
                "instructions": {
                    "text_instructions": [{"content": "after rollback"}],
                },
            },
        },
    )

    result = snapshot_contract.compare_live_to_expected_snapshot(
        w=object(),
        space_id="space_1",
        expected_snapshot={
            "instructions": {
                "text_instructions": [{"content": "before rollback"}],
            },
        },
    )

    assert result["verified"] is False
    assert result["reason"] == "live_config_differs_from_pre_snapshot"
    assert result["expected_digest"] != result["live_digest"]
    assert result["first_diff_path"] == "instructions.text_instructions[0].content"
    assert result["first_diff_expected"] == "before rollback"
    assert result["first_diff_live"] == "after rollback"


def test_capture_pre_ag_snapshot_returns_snapshot_and_digest(monkeypatch) -> None:
    from genie_space_optimizer.optimization import snapshot_contract

    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: {
            "serialized_space": "{\"instructions\": {}}",
            "_parsed_space": {
                "instructions": {"text_instructions": [{"content": "before"}]},
            },
        },
    )

    captured = snapshot_contract.capture_pre_ag_snapshot(
        w=object(),
        space_id="space_1",
        ag_id="AG1",
    )

    assert captured["captured"] is True
    assert captured["ag_id"] == "AG1"
    assert captured["snapshot"]["instructions"]["text_instructions"][0]["content"] == "before"
    assert len(captured["digest"]) == 64
