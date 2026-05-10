"""Cycle 14-W hardening — D-7 production wiring.

Anchor: airline run 1105451933925748 — 3 iterations attempted,
1 GSO_ITERATION_SUMMARY_V1 emitted, iter_record_counts has 4
buckets. The totality alarm must fire at lever-loop terminate.
"""
from __future__ import annotations

import io
import json
import re
from contextlib import redirect_stdout


def _extract_payload(stdout_text: str, marker: str) -> dict | None:
    match = re.search(rf"{marker}\s+(\{{.*?\}})", stdout_text)
    if match is None:
        return None
    return json.loads(match.group(1))


def test_totality_alarm_fires_on_airline_anchor_shape(monkeypatch) -> None:
    """3 iterations, 1 summary emitted, 4 record-count buckets ->
    totality alarm fires with the expected violation payload."""
    monkeypatch.setenv("GSO_ITERATION_SUMMARY_TOTALITY", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_iteration_summary_totality_at_terminate,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_iteration_summary_totality_at_terminate(
            run_id="airline_anchor_13",
            iteration_counter=3,
            iteration_summary_count=1,
            phase_b_iter_record_counts_length=4,
        )
    payload = _extract_payload(buf.getvalue(), "GSO_ITERATION_SUMMARY_TOTALITY_V1")
    assert payload is not None
    assert payload["iteration_counter"] == 3
    assert payload["iteration_summary_count"] == 1
    assert payload["phase_b_iter_record_counts_length"] == 4


def test_totality_alarm_silent_on_clean_run(monkeypatch) -> None:
    """3 = 3 = 3 -> no alarm."""
    monkeypatch.setenv("GSO_ITERATION_SUMMARY_TOTALITY", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_iteration_summary_totality_at_terminate,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_iteration_summary_totality_at_terminate(
            run_id="clean_run",
            iteration_counter=3,
            iteration_summary_count=3,
            phase_b_iter_record_counts_length=3,
        )
    assert "GSO_ITERATION_SUMMARY_TOTALITY_V1" not in buf.getvalue()


def test_totality_alarm_disabled_when_flag_off(monkeypatch) -> None:
    """Flag-off path: alarm never emits even on disagreement."""
    monkeypatch.setenv("GSO_ITERATION_SUMMARY_TOTALITY", "0")
    from genie_space_optimizer.optimization.harness import (
        _emit_iteration_summary_totality_at_terminate,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_iteration_summary_totality_at_terminate(
            run_id="airline_anchor_13",
            iteration_counter=3,
            iteration_summary_count=1,
            phase_b_iter_record_counts_length=4,
        )
    assert "GSO_ITERATION_SUMMARY_TOTALITY_V1" not in buf.getvalue()
