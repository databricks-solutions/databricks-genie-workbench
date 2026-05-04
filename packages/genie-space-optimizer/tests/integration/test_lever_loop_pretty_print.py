"""Verify the pretty-print contract end-to-end:

1. The notebook source (run_lever_loop.py) contains a single cell
   that consumes loop_out['pretty_print_transcript'].
2. The harness build helper attaches the transcript when
   phase_h_full_transcript and phase_h_anchor_run_id are both truthy.
3. The bundle-assembly marker round-trips through the parser.

This is a structural test. It does not start a Spark session or
talk to MLflow."""

from pathlib import Path

from genie_space_optimizer.optimization.harness import (
    _build_loop_out_with_pretty_print,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    bundle_assembly_failed_marker,
)
from genie_space_optimizer.tools.marker_parser import parse_markers


_NOTEBOOK = (
    Path(__file__).resolve().parents[2]
    / "src" / "genie_space_optimizer" / "jobs" / "run_lever_loop.py"
)


def test_notebook_consumes_pretty_print_transcript():
    src = _NOTEBOOK.read_text()
    assert 'loop_out.get("pretty_print_transcript")' in src
    assert "GSO Run Pretty-Print" in src


def test_pretty_print_attached_when_phase_h_succeeded():
    out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript="GSO LEVER LOOP RUN\nbody",
        phase_h_anchor_run_id="a",
    )
    assert out["pretty_print_transcript"].startswith("GSO LEVER LOOP RUN")


def test_pretty_print_absent_on_replay_path():
    out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript=None,
        phase_h_anchor_run_id=None,
    )
    assert "pretty_print_transcript" not in out


def test_bundle_assembly_failed_marker_round_trip():
    marker = bundle_assembly_failed_marker(
        optimization_run_id="r1",
        parent_bundle_run_id="a1",
        error_type="RuntimeError",
        error_message="boom",
    )
    parsed = parse_markers(marker + "\n")
    assert parsed.bundle_assembly_failed
    assert parsed.bundle_assembly_failed[0]["optimization_run_id"] == "r1"
