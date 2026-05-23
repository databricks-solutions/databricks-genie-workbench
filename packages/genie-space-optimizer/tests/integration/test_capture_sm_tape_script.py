"""PR-B — unit tests for ``scripts/capture_sm_tape.py``.

Pins the offline tape-capture workflow: given a postmortem evidence
bundle (the on-disk shape every lever-loop trial produces), the script
emits a JSONL tape the ``TapeReplayHarness`` consumes.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts" / "capture_sm_tape.py"
)


def _load_capture_module():
    """Load capture_sm_tape.py as an importable module — scripts/ is
    not on sys.path so we wire it in here for unit-test access."""
    spec = importlib.util.spec_from_file_location(
        "capture_sm_tape", _SCRIPT_PATH,
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["capture_sm_tape"] = mod
    spec.loader.exec_module(mod)
    return mod


def _evidence_dir_with_markers(tmp_path: Path, markers: list[dict]) -> Path:
    """Build a minimal evidence directory carrying a marker text file
    in the same format the lever-loop export writes."""
    bundle = tmp_path / "evidence"
    bundle.mkdir()
    text = "\n".join(
        f"GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {json.dumps(m)}"
        for m in markers
    )
    (bundle / "markers.txt").write_text(text)
    return bundle


def test_capture_script_writes_one_entry_per_iteration(tmp_path: Path) -> None:
    """Multiple markers from the same iteration collapse to one tape
    entry because ``diagnose_failing_qids`` makes one batched LLM call
    per iteration regardless of QID count."""
    mod = _load_capture_module()

    bundle = _evidence_dir_with_markers(tmp_path, markers=[
        {
            "outcome": "llm_error", "qid": "gs_001", "iteration": 1,
            "duration_ms": 4904, "exception_class": "BadRequestError",
        },
        {
            "outcome": "llm_error", "qid": "gs_004", "iteration": 1,
            "duration_ms": 4202, "exception_class": "BadRequestError",
        },
        {
            "outcome": "llm_error", "qid": "gs_001", "iteration": 2,
            "duration_ms": 4500, "exception_class": "BadRequestError",
        },
    ])
    out = tmp_path / "tape.jsonl"
    rc = mod.main([
        "--evidence-dir", str(bundle),
        "--out", str(out),
        "--error-message-from", str(_write(tmp_path, "Error code: 400 - body")),
    ])
    assert rc == 0
    lines = out.read_text().strip().splitlines()
    assert len(lines) == 2
    iters = sorted(json.loads(ln)["iteration"] for ln in lines)
    assert iters == [1, 2]
    # Every entry has the body fallback wired through.
    for ln in lines:
        e = json.loads(ln)
        assert e["kind"] == "exception"
        assert e["skill_id"] == "plan11_diagnose"
        assert e["exception_class"] == "BadRequestError"
        assert "Error code: 400" in e["exception_message"]


def test_capture_script_prefers_disk_dump_over_fallback(tmp_path: Path) -> None:
    """When the evidence bundle includes PR-A on-disk dumps the script
    must prefer them over the operator-supplied fallback — the dumps
    carry the real Databricks-side body, not a placeholder."""
    mod = _load_capture_module()

    bundle = _evidence_dir_with_markers(tmp_path, markers=[
        {
            "outcome": "llm_error", "qid": "gs_001", "iteration": 1,
            "duration_ms": 4904, "exception_class": "BadRequestError",
        },
    ])
    dump_dir = bundle / "llm_errors"
    dump_dir.mkdir()
    (dump_dir / "stage1_1_gs_001.json").write_text(json.dumps({
        "iteration": 1,
        "qid": "gs_001",
        "error_message": "Error code: 400 - REAL_BODY_FROM_DUMP",
    }))

    out = tmp_path / "tape.jsonl"
    rc = mod.main([
        "--evidence-dir", str(bundle),
        "--out", str(out),
        "--error-message-from", str(_write(tmp_path, "FALLBACK_BODY")),
    ])
    assert rc == 0
    entry = json.loads(out.read_text().strip().splitlines()[0])
    assert "REAL_BODY_FROM_DUMP" in entry["exception_message"]
    assert "FALLBACK_BODY" not in entry["exception_message"]


def test_capture_script_errors_when_no_markers(tmp_path: Path) -> None:
    mod = _load_capture_module()
    bundle = tmp_path / "evidence"
    bundle.mkdir()
    (bundle / "noise.txt").write_text("no markers here")
    rc = mod.main([
        "--evidence-dir", str(bundle),
        "--out", str(tmp_path / "out.jsonl"),
        "--error-message-from", str(_write(tmp_path, "body")),
    ])
    assert rc == 2


def test_capture_script_errors_when_no_body_source(tmp_path: Path) -> None:
    """Without dumps and without --error-message-from the script must
    refuse rather than emit empty exception bodies."""
    mod = _load_capture_module()
    bundle = _evidence_dir_with_markers(tmp_path, markers=[
        {
            "outcome": "llm_error", "qid": "gs_001", "iteration": 1,
            "exception_class": "BadRequestError",
        },
    ])
    rc = mod.main([
        "--evidence-dir", str(bundle),
        "--out", str(tmp_path / "out.jsonl"),
    ])
    assert rc == 2


def _write(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "body.txt"
    p.write_text(content)
    return p
