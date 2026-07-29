from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.optimization import applier


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
