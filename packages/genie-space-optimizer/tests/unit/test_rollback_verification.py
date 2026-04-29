from __future__ import annotations


def test_rollback_verification_passes_when_fetch_matches_pre_snapshot(monkeypatch) -> None:
    from genie_space_optimizer.optimization import applier

    expected = {
        "instructions": {"text_instructions": [{"content": "original"}]},
        "data_sources": {"tables": []},
    }

    monkeypatch.setattr(applier, "fetch_space_config", lambda _w, _space_id: expected)

    result = applier.verify_rollback_restored(
        w=object(),
        space_id="space-1",
        expected_snapshot=expected,
    )

    assert result["verified"] is True
    assert result["reason"] == "matched_pre_snapshot"


def test_rollback_verification_reports_mismatch(monkeypatch) -> None:
    from genie_space_optimizer.optimization import applier

    expected = {"instructions": {"text_instructions": [{"content": "original"}]}}
    live = {"instructions": {"text_instructions": [{"content": "patched"}]}}

    monkeypatch.setattr(applier, "fetch_space_config", lambda _w, _space_id: live)

    result = applier.verify_rollback_restored(
        w=object(),
        space_id="space-1",
        expected_snapshot=expected,
    )

    assert result["verified"] is False
    assert result["reason"] == "live_config_differs_from_pre_snapshot"
