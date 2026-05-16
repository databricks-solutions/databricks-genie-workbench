"""Track E regression guard: byte-stability of fixture filenames must
not depend on volatile per-process metadata (``captured_at``,
``process_pid``).

Background — trial-4 (2026-05-15) revealed that three "self-hashed"
exporters (``export_raw_evidence_fixtures``, ``export_three_stage_fixtures``,
``export_lever5_split_fixtures``) computed their content hash over the
full record dict, which includes ``captured_at`` (float) and (for raw
evidence + three stage) ``process_pid`` (int). Two trials over the
same logical content therefore produced two different fixture files
— defeating the byte-stability gate (no contract change is detected
because the hash always changes).

This test pins the contract: each exporter must expose
``_VOLATILE_HASH_KEYS`` (frozenset of strings) and
``_stable_hash_input(record_or_payload)`` that returns a dict with
those keys removed. The same record observed by two different
processes (different timestamps + pids) must hash to the same
fixture filename.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


_SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"


def _load_exporter(script_basename: str):
    """Load ``scripts/<script_basename>.py`` as an importable module."""
    script_path = _SCRIPTS_DIR / f"{script_basename}.py"
    spec = importlib.util.spec_from_file_location(script_basename, script_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[script_basename] = mod
    spec.loader.exec_module(mod)
    return mod


_EXPORTER_BASENAMES = (
    "export_raw_evidence_fixtures",
    "export_three_stage_fixtures",
    "export_lever5_split_fixtures",
)


@pytest.mark.parametrize("script_basename", _EXPORTER_BASENAMES)
def test_exporter_declares_volatile_hash_keys(script_basename: str):
    mod = _load_exporter(script_basename)
    assert hasattr(mod, "_VOLATILE_HASH_KEYS"), (
        f"{script_basename} must declare _VOLATILE_HASH_KEYS so the "
        f"hash is stable across processes."
    )
    keys = mod._VOLATILE_HASH_KEYS
    assert isinstance(keys, frozenset), type(keys)
    # captured_at + process_pid are the two volatile fields trial-4
    # surfaced. Future fields (e.g., wall-clock-only counters) should
    # be added here too.
    assert "captured_at" in keys, keys
    assert "process_pid" in keys, keys


@pytest.mark.parametrize("script_basename", _EXPORTER_BASENAMES)
def test_stable_hash_input_strips_volatile_fields(script_basename: str):
    mod = _load_exporter(script_basename)
    assert hasattr(mod, "_stable_hash_input"), (
        f"{script_basename} must expose _stable_hash_input(record_or_payload) "
        f"that returns a dict with _VOLATILE_HASH_KEYS removed."
    )
    record = {
        "ag_id": "AG1",
        "skill_id": "lever-1-table-column-description",
        "captured_at": 1778894532.7577753,
        "process_pid": 4026,
        "stable_field": "abc",
    }
    out = mod._stable_hash_input(record)
    assert "captured_at" not in out, out
    assert "process_pid" not in out, out
    assert out.get("ag_id") == "AG1"
    assert out.get("stable_field") == "abc"


@pytest.mark.parametrize("script_basename", _EXPORTER_BASENAMES)
def test_two_records_differing_only_in_volatile_fields_hash_equal(
    script_basename: str,
):
    """Pin the byte-stability property: identical logical content
    captured by two different processes must produce identical fixture
    filenames."""
    mod = _load_exporter(script_basename)
    rec_process_a = {
        "ag_id": "AG1",
        "skill_id": "lever-1-table-column-description",
        "captured_at": 1778868723.0494423,
        "process_pid": 3203,
        "stable_field": "abc",
        "n_evidence": 2,
    }
    rec_process_b = {
        "ag_id": "AG1",
        "skill_id": "lever-1-table-column-description",
        "captured_at": 1778894532.7577753,
        "process_pid": 4026,
        "stable_field": "abc",
        "n_evidence": 2,
    }
    a = mod._stable_hash_input(rec_process_a)
    b = mod._stable_hash_input(rec_process_b)
    assert a == b, (
        f"{script_basename}._stable_hash_input did not produce equal "
        f"output for records differing only in volatile fields:\n"
        f"a={a}\nb={b}"
    )
