"""RCO-8 — shared fixture loader for sub-stage production-shape pairs.

Each helper has a subdirectory under ``tests/unit/fixtures/rco8/<helper>/``.
Inside that helper directory, each case is its own subdirectory containing
either:

  * ``input.json`` + ``expected_output.json`` (the common JSON shape), or
  * ``input.txt`` + ``expected_output.json`` (the marker-parser shape:
    raw stdout in, normalized MarkerLog dict out).

This loader is intentionally tiny — fixture discovery, not test
abstraction. Tests parametrize over its return value and own their
assertions.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


FIXTURES_ROOT: Path = Path(__file__).resolve().parent


def helper_dir(helper_name: str) -> Path:
    """Return the fixture directory for a given helper name."""
    return FIXTURES_ROOT / helper_name


def load_json_pairs(helper_name: str) -> list[tuple[str, Any, Any]]:
    """Yield ``(case_name, input_data, expected_data)`` triples for
    every subdirectory containing an ``input.json`` /
    ``expected_output.json`` pair."""
    out: list[tuple[str, Any, Any]] = []
    root = helper_dir(helper_name)
    if not root.is_dir():
        return out
    for case_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        input_path = case_dir / "input.json"
        expected_path = case_dir / "expected_output.json"
        if not (input_path.exists() and expected_path.exists()):
            continue
        with input_path.open() as fp:
            input_data = json.load(fp)
        with expected_path.open() as fp:
            expected_data = json.load(fp)
        out.append((case_dir.name, input_data, expected_data))
    return out


def load_text_in_json_out_pairs(
    helper_name: str,
) -> list[tuple[str, str, Any]]:
    """Yield ``(case_name, input_text, expected_data)`` triples for
    every subdirectory containing an ``input.txt`` /
    ``expected_output.json`` pair (the marker-parser shape)."""
    out: list[tuple[str, str, Any]] = []
    root = helper_dir(helper_name)
    if not root.is_dir():
        return out
    for case_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        input_path = case_dir / "input.txt"
        expected_path = case_dir / "expected_output.json"
        if not (input_path.exists() and expected_path.exists()):
            continue
        input_text = input_path.read_text()
        with expected_path.open() as fp:
            expected_data = json.load(fp)
        out.append((case_dir.name, input_text, expected_data))
    return out
