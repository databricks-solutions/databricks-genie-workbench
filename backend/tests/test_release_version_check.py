"""The release guard fails a published release whose tag does not match VERSION (spec 5.3)."""

import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(tmp_path: Path, tag: str, version: str | None) -> subprocess.CompletedProcess:
    (tmp_path / "scripts").mkdir(exist_ok=True)
    shutil.copy(_REPO_ROOT / "scripts" / "check_release_version.sh", tmp_path / "scripts")
    if version is not None:
        (tmp_path / "VERSION").write_text(version)
    return subprocess.run(
        ["bash", "scripts/check_release_version.sh", tag],
        cwd=tmp_path, capture_output=True, text=True,
    )


def test_matching_tag_passes(tmp_path):
    proc = _run(tmp_path, "0.2.1", "0.2.1\n")
    assert proc.returncode == 0, proc.stderr


def test_v_prefixed_tag_fails(tmp_path):
    # The app links Release notes to /releases/tag/<VERSION>, so the tag must equal VERSION exactly.
    proc = _run(tmp_path, "v0.2.1", "0.2.1\n")
    assert proc.returncode == 1
    assert "must be exactly '0.2.1'" in proc.stderr


def test_mismatched_tag_fails_with_fix_instructions(tmp_path):
    proc = _run(tmp_path, "0.3", "0.2.1\n")
    assert proc.returncode == 1
    assert "Bump VERSION to '0.3'" in proc.stderr


def test_missing_version_file_fails(tmp_path):
    proc = _run(tmp_path, "0.2.1", None)
    assert proc.returncode == 1
    assert "<missing>" in proc.stderr


def test_workflow_runs_the_guard_on_published_releases():
    wf = (_REPO_ROOT / ".github" / "workflows" / "release-version-check.yml").read_text()
    assert "types: [published]" in wf
    assert 'bash scripts/check_release_version.sh "$TAG"' in wf
    assert "contents: read" in wf
    # The tag reaches the script through env, never interpolated into the shell line.
    run_lines = [line for line in wf.splitlines() if line.strip().startswith("run:")]
    assert run_lines and all("${{" not in line for line in run_lines)
