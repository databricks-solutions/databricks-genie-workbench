"""Grep-guard preventing reintroduction of the seven env vars retired
by the 2026-05-16 dead-flag cleanup.

The seven retired flags were rollout/shadow gates for Plans 1-4. After
trial-5 produced byte-stable fixtures for all four plans under the
default-on posture, the gates became dead weight. This test fails the
build if anyone re-adds a reference to the matching env-var name
inside ``src/`` or ``scripts/`` (test files and docs are allowed to
reference them historically)."""
from __future__ import annotations

from pathlib import Path

import pytest


_RETIRED_FLAGS: frozenset[str] = frozenset({
    "GSO_RCA_CONTRACT_NARROW_V1",
    "GSO_LEVER5_SPLIT_V1",
    "GSO_LEVER5_SHADOW_V1",
    "GSO_THREE_STAGE_V1",
    "GSO_THREE_STAGE_SHADOW_V1",
    "GSO_RAW_EVIDENCE_V1",
    "GSO_RAW_EVIDENCE_SHADOW_V1",
})

_RETIRED_HELPERS: frozenset[str] = frozenset({
    "rca_contract_narrowed_enabled",
    "lever5_split_enabled",
    "lever5_shadow_enabled",
    "three_stage_enabled",
    "three_stage_shadow_enabled",
    "raw_evidence_v1_enabled",
    "raw_evidence_v1_shadow_enabled",
})

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCAN_DIRS = (
    _REPO_ROOT / "src",
    _REPO_ROOT / "scripts",
)


def _iter_py_files() -> list[Path]:
    files: list[Path] = []
    for root in _SCAN_DIRS:
        if not root.is_dir():
            continue
        files.extend(p for p in root.rglob("*.py") if p.is_file())
    return files


@pytest.mark.parametrize("needle", sorted(_RETIRED_FLAGS))
def test_retired_flag_env_var_absent_from_src_and_scripts(needle: str) -> None:
    offenders: list[str] = []
    for path in _iter_py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if needle in text:
            offenders.append(str(path.relative_to(_REPO_ROOT)))
    assert not offenders, (
        f"Retired env var {needle!r} reintroduced into src/ or scripts/. "
        f"Offending files: {offenders}. See "
        f"docs/prompt_improvements/2026-05-16-dead-flag-cleanup.md."
    )


@pytest.mark.parametrize("needle", sorted(_RETIRED_HELPERS))
def test_retired_helper_absent_from_src_and_scripts(needle: str) -> None:
    offenders: list[str] = []
    for path in _iter_py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if needle in text:
            offenders.append(str(path.relative_to(_REPO_ROOT)))
    assert not offenders, (
        f"Retired helper {needle!r} reintroduced into src/ or scripts/. "
        f"Offending files: {offenders}. See "
        f"docs/prompt_improvements/2026-05-16-dead-flag-cleanup.md."
    )
