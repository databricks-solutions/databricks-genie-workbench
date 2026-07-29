from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.optimization import applier


def test_post_patch_read_back_returns_authoritative_serialized_space(monkeypatch) -> None:
    observed = {
        "version": 2,
        "instructions": {
            "text_instructions": [{"id": "a", "content": ["PURPOSE:\n", "- Help"]}],
        },
    }
    monkeypatch.setattr(
        applier,
        "fetch_space_config",
        lambda _w, _space_id: {"_parsed_space": observed},
    )

    result = applier._read_back_serialized_space(MagicMock(), "space")

    assert result == observed
    assert result is not observed


def test_post_patch_read_back_failure_is_non_fatal(monkeypatch) -> None:
    def fail(_w, _space_id):
        raise RuntimeError("temporary GET failure")

    monkeypatch.setattr(applier, "fetch_space_config", fail)
    assert applier._read_back_serialized_space(MagicMock(), "space") is None


def test_successful_config_patch_reads_back_authoritative_snapshot(monkeypatch) -> None:
    events: list[str] = []
    observed = {
        "version": 2,
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "instructions": {
            "text_instructions": [
                {"id": "a", "content": ["PURPOSE:\n", "- Help users"]},
            ],
        },
    }

    def patch_config(_w, _space_id, _config):
        events.append("patch")
        return {}

    def fetch_config(_w, _space_id):
        events.append("get")
        return {"_parsed_space": observed}

    monkeypatch.setattr(applier, "patch_space_config", patch_config)
    monkeypatch.setattr(applier, "fetch_space_config", fetch_config)
    monkeypatch.setattr(
        applier,
        "_canonicalize_and_dedup_instructions",
        lambda _config: False,
    )

    config = {
        "version": 2,
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "instructions": {"text_instructions": []},
    }
    out = applier.apply_patch_set(
        MagicMock(),
        "space",
        [{"type": "add_instruction", "lever": 5, "new_text": "Help users"}],
        config,
    )

    assert out["patch_deployed"] is True
    assert events == ["patch", "get"]
    assert out["observed_post_snapshot"] == observed
    assert out["observed_post_snapshot"] is not observed


def test_update_space_description_uses_metadata_patch_not_serialized_space(monkeypatch) -> None:
    descriptions: list[str] = []
    serialized_patch_calls: list[dict] = []

    monkeypatch.setattr(
        applier,
        "update_space_description",
        lambda _w, _space_id, desc: descriptions.append(desc),
    )
    monkeypatch.setattr(
        applier,
        "patch_space_config",
        lambda _w, _space_id, config: serialized_patch_calls.append(config),
    )

    config = {
        "version": 2,
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "instructions": {"text_instructions": []},
    }

    out = applier.apply_patch_set(
        MagicMock(),
        "space",
        [
            {
                "type": "update_space_description",
                "lever": 0,
                "old_text": "",
                "new_text": "Sales analytics space for regional order reporting.",
            }
        ],
        config,
    )

    assert out["patch_deployed"] is True
    assert descriptions == ["Sales analytics space for regional order reporting."]
    assert serialized_patch_calls == []
    assert out["applied"][0]["patch"]["type"] == "update_space_description"
