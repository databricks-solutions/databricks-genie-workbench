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


def test_harness_captures_pre_ag_snapshot_before_apply() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)

    capture_idx = source.index("capture_pre_ag_snapshot(")
    apply_idx = source.index("        apply_log = apply_patch_set(")
    rollback_idx = source.index("rollback(apply_log, w, space_id,")

    assert capture_idx < apply_idx
    assert "metadata_snapshot = _pre_ag_snapshot_capture[\"snapshot\"]" in source
    assert "expected_snapshot=metadata_snapshot" in source
    assert rollback_idx > apply_idx


def test_failed_rollback_verification_is_terminal() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness)

    assert "class FailedRollbackVerification(RuntimeError):" in source
    assert 'convergence_reason="failed_rollback_verification"' in source
    assert "raise FailedRollbackVerification(" in source
    assert "_kv(\"First diff\", _restore_decision.get(\"first_diff_path\", \"(none)\"))" in source


def test_harness_warns_when_run_level_config_snapshot_is_missing() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    assert "RUN-LEVEL CONFIG SNAPSHOT MISSING" in source
    assert "should have been captured at trigger time" in source
    assert "capture_pre_ag_snapshot(" in source
