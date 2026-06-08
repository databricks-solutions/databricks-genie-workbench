"""Behavioural tests for the HAND_ROLLED_QID_EXTRACTION scanner.

The scanner must flag real-code occurrences of ``row.get("question_id")`` /
``row["question_id"]`` while leaving the SAME text inside comments and
docstrings untouched (the false-positive class that made the rg-based rule
impossible to satisfy without deleting prose).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SCAN_PATH = Path(__file__).resolve().parents[2] / "scripts" / "_qid_hardroll_scan.py"


def _load():
    spec = importlib.util.spec_from_file_location("_qid_hardroll_scan", _SCAN_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


SCAN = _load()


def _write(tmp_path, body: str) -> str:
    f = tmp_path / "mod.py"
    f.write_text(body, encoding="utf-8")
    return str(f)


def test_flags_real_get_call(tmp_path):
    p = _write(tmp_path, "def g(row):\n    return row.get(\"question_id\")\n")
    v = SCAN.find_violations([p])
    assert [(line, snip) for _, line, snip in v] == [(2, 'row.get("question_id")')]


def test_flags_real_subscript(tmp_path):
    p = _write(tmp_path, "def g(row):\n    return row[\"question_id\"]\n")
    v = SCAN.find_violations([p])
    assert [(line, snip) for _, line, snip in v] == [(2, 'row["question_id"]')]


def test_ignores_full_line_comment(tmp_path):
    p = _write(
        tmp_path,
        "def g(row):\n    # row.get(\"question_id\") is forbidden here\n    return None\n",
    )
    assert SCAN.find_violations([p]) == []


def test_ignores_docstring_mention(tmp_path):
    p = _write(
        tmp_path,
        'def g(row):\n    """Single-key ``row.get("question_id")`` is the old way."""\n    return None\n',
    )
    assert SCAN.find_violations([p]) == []


def test_ignores_trailing_comment(tmp_path):
    p = _write(
        tmp_path,
        'def g(row):\n    x = 1  # see row.get("question_id")\n    return x\n',
    )
    assert SCAN.find_violations([p]) == []


def test_does_not_match_other_keys_or_vars(tmp_path):
    # row.get("qid") is allowed (rule targets question_id); other.get is not `row`.
    p = _write(
        tmp_path,
        'def g(row, other):\n    return row.get("qid") or other.get("question_id")\n',
    )
    assert SCAN.find_violations([p]) == []


def test_directory_excludes_tests_and_qid_extraction(tmp_path):
    (tmp_path / "test_foo.py").write_text('def g(row):\n    return row.get("question_id")\n')
    (tmp_path / "_qid_extraction.py").write_text('def g(row):\n    return row.get("question_id")\n')
    (tmp_path / "real.py").write_text('def g(row):\n    return row.get("question_id")\n')
    v = SCAN.find_violations([str(tmp_path)])
    files = {Path(p).name for p, _, _ in v}
    assert files == {"real.py"}


def test_production_optimizer_and_state_machine_are_clean():
    """The live tree must have zero real-code violations (the gate fix)."""
    src = Path(__file__).resolve().parents[2] / "src" / "genie_space_optimizer" / "optimization"
    v = SCAN.find_violations([str(src / "state_machine"), str(src / "optimizer.py")])
    assert v == [], f"unexpected hand-rolled qid extraction: {v}"
