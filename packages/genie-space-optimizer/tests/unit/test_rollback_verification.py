from __future__ import annotations


def test_rollback_verification_passes_when_fetch_matches_pre_snapshot(monkeypatch) -> None:
    """Task 1/2 — ``verify_rollback_restored`` delegates to
    ``snapshot_contract.compare_live_to_expected_snapshot``, so stub the
    fetch on that module rather than ``applier``."""
    from genie_space_optimizer.optimization import applier, snapshot_contract

    expected = {
        "instructions": {"text_instructions": [{"content": "original"}]},
        "data_sources": {"tables": []},
    }

    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: expected,
    )

    result = applier.verify_rollback_restored(
        w=object(),
        space_id="space-1",
        expected_snapshot=expected,
    )

    assert result["verified"] is True
    assert result["reason"] == "matched_pre_snapshot"


def test_rollback_verification_reports_mismatch(monkeypatch) -> None:
    from genie_space_optimizer.optimization import applier, snapshot_contract

    expected = {"instructions": {"text_instructions": [{"content": "original"}]}}
    live = {"instructions": {"text_instructions": [{"content": "patched"}]}}

    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: live,
    )

    result = applier.verify_rollback_restored(
        w=object(),
        space_id="space-1",
        expected_snapshot=expected,
    )

    assert result["verified"] is False
    assert result["reason"] == "live_config_differs_from_pre_snapshot"
