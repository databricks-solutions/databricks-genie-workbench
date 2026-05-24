"""Codebase boundary tests for the local lever-loop workbench.

The workbench may freely import from ``src/genie_space_optimizer/`` and
from ``tests/``. The reverse is forbidden: production code under
``src/genie_space_optimizer/`` must never import from
``devtools/local_lever_workbench/`` or from ``tests.workbench``.

Without this guard, a single ``import local_lever_workbench`` slipping
into a production module would silently couple a deploy to dev-only
code, fail at deploy time, and require a hotfix.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


_PKG_ROOT = Path(__file__).resolve().parents[2]
_SRC_DIR = _PKG_ROOT / "src" / "genie_space_optimizer"

_FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*(?:from|import)\s+local_lever_workbench(?:\b|\.)"),
    re.compile(r"^\s*(?:from|import)\s+devtools\."),
    re.compile(r"^\s*(?:from|import)\s+tests\.workbench(?:\b|\.)"),
    re.compile(r"^\s*(?:from|import)\s+tests\.integration(?:\b|\.)"),
    re.compile(r"^\s*(?:from|import)\s+tests\.unit(?:\b|\.)"),
)


@pytest.mark.workbench
def test_production_src_does_not_import_workbench_or_tests() -> None:
    """Walk ``src/genie_space_optimizer/`` and fail if any forbidden import is found."""
    assert _SRC_DIR.is_dir(), f"unexpected src layout: {_SRC_DIR}"
    offenders: list[tuple[str, int, str]] = []
    for path in _SRC_DIR.rglob("*.py"):
        # ``__pycache__`` and editable-install metadata are ignored.
        if "__pycache__" in path.parts:
            continue
        try:
            text = path.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for pat in _FORBIDDEN_PATTERNS:
                if pat.match(line):
                    offenders.append(
                        (str(path.relative_to(_PKG_ROOT)), lineno, line.strip())
                    )
                    break
    assert not offenders, (
        "Production code MUST NOT import dev-only or test packages. "
        "Offending imports:\n  "
        + "\n  ".join(f"{p}:{ln}  {src}" for p, ln, src in offenders)
        + "\n\nMove the dependency into devtools/ or tests/, or, if the "
          "production code genuinely needs the helper, lift it into "
          "src/genie_space_optimizer/ with its own test coverage."
    )
