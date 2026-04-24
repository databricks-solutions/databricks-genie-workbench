"""Unit tests for ``scripts/dedupe_benchmark_qids.py`` (C1)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def dedupe_mod():
    """Import the CLI script by path (it lives outside the package)."""
    script_path = (
        Path(__file__).resolve().parent.parent.parent
        / "scripts"
        / "dedupe_benchmark_qids.py"
    )
    spec = importlib.util.spec_from_file_location(
        "dedupe_benchmark_qids", script_path
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_no_collisions_is_noop(dedupe_mod) -> None:
    rows = [
        {"id": "q1", "question": "what is foo"},
        {"id": "q2", "question": "what is bar"},
    ]
    new, diff = dedupe_mod.dedupe_question_ids(rows)
    assert [r["id"] for r in new] == ["q1", "q2"]
    assert diff == []


def test_simple_collision_gets_v2(dedupe_mod) -> None:
    rows = [
        {"id": "q1", "question": "first"},
        {"id": "q1", "question": "second"},
    ]
    new, diff = dedupe_mod.dedupe_question_ids(rows)
    assert [r["id"] for r in new] == ["q1", "q1:v2"]
    assert len(diff) == 1
    assert diff[0]["old_id"] == "q1"
    assert diff[0]["new_id"] == "q1:v2"


def test_triple_collision_numbers_ascending(dedupe_mod) -> None:
    rows = [
        {"id": "q1", "question": "a"},
        {"id": "q1", "question": "b"},
        {"id": "q1", "question": "c"},
    ]
    new, _ = dedupe_mod.dedupe_question_ids(rows)
    assert [r["id"] for r in new] == ["q1", "q1:v2", "q1:v3"]


def test_collision_skips_taken_suffix(dedupe_mod) -> None:
    """If a manually-assigned ``:v2`` already exists, dedupe must skip it."""
    rows = [
        {"id": "q1", "question": "a"},
        {"id": "q1:v2", "question": "manual"},
        {"id": "q1", "question": "needs dedup"},
    ]
    new, _ = dedupe_mod.dedupe_question_ids(rows)
    assert [r["id"] for r in new] == ["q1", "q1:v2", "q1:v3"]


def test_rewrites_both_id_and_question_id_if_present(dedupe_mod) -> None:
    rows = [
        {"id": "q1", "question_id": "q1", "question": "a"},
        {"id": "q1", "question_id": "q1", "question": "b"},
    ]
    new, diff = dedupe_mod.dedupe_question_ids(rows)
    assert new[1]["id"] == "q1:v2"
    assert new[1]["question_id"] == "q1:v2"
    assert len(diff) == 1


def test_missing_id_passthrough(dedupe_mod) -> None:
    """Rows with no id / question_id must not be mutated (loader bug)."""
    rows = [{"question": "no-id"}, {"question": "still-no-id"}]
    new, diff = dedupe_mod.dedupe_question_ids(rows)
    assert new == rows  # shallow copies compare equal
    assert diff == []


def test_cli_dry_run_offline(tmp_path: Path, dedupe_mod, capsys) -> None:
    benchmarks_path = tmp_path / "benchmarks.jsonl"
    benchmarks_path.write_text(
        "\n".join(
            [
                json.dumps({"id": "q1", "question": "first"}),
                json.dumps({"id": "q1", "question": "second"}),
            ]
        ),
        encoding="utf-8",
    )

    rc = dedupe_mod.main(["--jsonl", str(benchmarks_path)])
    assert rc == 0
    stdout = capsys.readouterr().out
    report = json.loads(stdout)
    assert report["total_rows"] == 2
    assert report["rewrites"] == 1
    assert report["entries"][0]["new_id"] == "q1:v2"

    # Dry-run must NOT mutate the source file.
    lines = benchmarks_path.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(line)["id"] for line in lines] == ["q1", "q1"]


def test_cli_apply_offline_writes_new_ids(
    tmp_path: Path, dedupe_mod
) -> None:
    benchmarks_path = tmp_path / "benchmarks.jsonl"
    output_path = tmp_path / "diff.json"
    benchmarks_path.write_text(
        "\n".join(
            [
                json.dumps({"id": "q1", "question": "first"}),
                json.dumps({"id": "q1", "question": "second"}),
                json.dumps({"id": "q1", "question": "third"}),
            ]
        ),
        encoding="utf-8",
    )

    rc = dedupe_mod.main(
        [
            "--jsonl",
            str(benchmarks_path),
            "--apply",
            "--output",
            str(output_path),
        ]
    )
    assert rc == 0

    rewritten = [
        json.loads(line)
        for line in benchmarks_path.read_text(encoding="utf-8").strip().splitlines()
    ]
    assert [r["id"] for r in rewritten] == ["q1", "q1:v2", "q1:v3"]

    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["rewrites"] == 2
