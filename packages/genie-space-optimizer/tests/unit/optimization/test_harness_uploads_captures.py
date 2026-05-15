"""Unit tests for Plan 5 (Option B) ``_upload_trial_captures_to_phase_h_anchor``.

The helper now reads in-memory buffers from the four capture sinks
(``sink.consume_records()``) and uploads each non-empty buffer as an
NDJSON MLflow artifact under ``gso_trial_captures/<plan>.ndjson`` via
``MlflowClient.log_text``. The previous file-based design depended on
``/tmp`` being writable, which fails on Databricks serverless.

Contract:
  * No-op when ``anchor_run_id`` is ``None`` or ``""``.
  * For each sink: consume_records() → if empty, skip; else serialize
    to NDJSON (one ``json.dumps`` per record, joined by ``\\n``,
    trailing newline) and call ``client.log_text(run_id=..., text=...,
    artifact_file="gso_trial_captures/<plan>.ndjson")``.
  * Filenames and order are fixed (narrowing → lever5_split →
    three_stage → raw_evidence) so the resulting MLflow tree is
    deterministic.
  * Any ``log_text`` exception is logged at WARNING and swallowed —
    telemetry must never break a real run.
"""
from __future__ import annotations

import importlib
import json
import sys
from unittest.mock import MagicMock


def _fresh_config():
    """Re-import config to wipe sink state between tests."""
    sys.modules.pop("genie_space_optimizer.common.config", None)
    return importlib.import_module("genie_space_optimizer.common.config")


def _reset_all_sinks(cfg) -> None:
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._LEVER_FIVE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._THREE_STAGE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    cfg._RAW_EVIDENCE_CAPTURE_SINK.reset_for_test()  # noqa: SLF001


def test_upload_skips_when_anchor_is_none():
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(anchor_run_id=None, client=client)
    client.log_text.assert_not_called()
    client.log_artifact.assert_not_called()


def test_upload_skips_when_anchor_is_empty():
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(anchor_run_id="", client=client)
    client.log_text.assert_not_called()


def test_upload_skips_sinks_with_empty_buffers():
    cfg = _fresh_config()
    _reset_all_sinks(cfg)
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="anchor-1", client=client,
    )
    client.log_text.assert_not_called()


def test_upload_emits_log_text_for_each_non_empty_buffer():
    cfg = _fresh_config()
    _reset_all_sinks(cfg)

    # Seed each sink with a record via its public emitter so we exercise
    # the same code path the harness uses at runtime.
    cfg._NARROWING_CAPTURE_SINK.record_hit(  # noqa: SLF001
        "lever-4-join-discovery", header_omitted_bytes=512,
    )
    cfg._LEVER_FIVE_CAPTURE_SINK.record_shadow_comparison({  # noqa: SLF001
        "skill_id": "lever-5a-instructions", "decision": "split_won",
    })
    cfg._THREE_STAGE_CAPTURE_SINK.record_shadow_comparison({  # noqa: SLF001
        "skill_id": "lever-1-table-column-description",
        "decision": "three_stage_won",
    })
    cfg._RAW_EVIDENCE_CAPTURE_SINK.record_shadow_comparison({  # noqa: SLF001
        "skill_id": "lever-1-table-column-description",
        "decision": "raw_evidence_won",
    })

    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="anchor-1", client=client,
    )

    assert client.log_text.call_count == 4
    seen_artifacts = []
    seen_run_ids = []
    seen_texts = []
    for call in client.log_text.call_args_list:
        seen_run_ids.append(call.kwargs.get("run_id") or call.args[0])
        seen_texts.append(call.kwargs.get("text") or call.args[1])
        seen_artifacts.append(
            call.kwargs.get("artifact_file") or call.args[2]
        )

    assert seen_run_ids == ["anchor-1"] * 4
    # Order is locked to make MLflow trees deterministic.
    assert seen_artifacts == [
        "gso_trial_captures/narrowing_v1.ndjson",
        "gso_trial_captures/lever5_split_v1.ndjson",
        "gso_trial_captures/three_stage_v1.ndjson",
        "gso_trial_captures/raw_evidence_v1.ndjson",
    ]
    # Each text must be valid NDJSON: trailing newline, one JSON object
    # per line, parseable.
    for text in seen_texts:
        assert text.endswith("\n"), (
            "NDJSON files must end with a newline so concatenation is safe."
        )
        lines = text.strip().splitlines()
        assert len(lines) >= 1
        for line in lines:
            json.loads(line)  # raises if malformed


def test_upload_omits_sinks_that_remain_empty():
    """If only one sink has buffered records, only one log_text call
    should fire — the other three are skipped silently."""
    cfg = _fresh_config()
    _reset_all_sinks(cfg)

    cfg._NARROWING_CAPTURE_SINK.record_hit(  # noqa: SLF001
        "preflight-instruction-expand", header_omitted_bytes=128,
    )

    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="anchor-2", client=client,
    )

    assert client.log_text.call_count == 1
    call = client.log_text.call_args
    artifact = call.kwargs.get("artifact_file") or call.args[2]
    assert artifact == "gso_trial_captures/narrowing_v1.ndjson"


def test_upload_swallows_log_text_exceptions(caplog):
    cfg = _fresh_config()
    _reset_all_sinks(cfg)
    cfg._NARROWING_CAPTURE_SINK.record_hit(  # noqa: SLF001
        "lever-4-join-discovery", header_omitted_bytes=64,
    )

    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    client.log_text.side_effect = RuntimeError("network down")
    # MUST NOT raise — observability never breaks the optimizer.
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="anchor-3", client=client,
    )
    assert any(
        "upload failed" in r.message.lower() for r in caplog.records
    )


def test_consume_records_does_not_clear_buffer():
    """consume_records() returns a snapshot but does not drain the
    buffer — repeat calls are safe (e.g. atexit retry)."""
    cfg = _fresh_config()
    _reset_all_sinks(cfg)
    cfg._NARROWING_CAPTURE_SINK.record_hit(  # noqa: SLF001
        "lever-4-join-discovery", header_omitted_bytes=64,
    )
    first = cfg._NARROWING_CAPTURE_SINK.consume_records()  # noqa: SLF001
    second = cfg._NARROWING_CAPTURE_SINK.consume_records()  # noqa: SLF001
    assert first == second
    assert len(first) == 1
