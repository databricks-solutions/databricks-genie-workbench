"""Layering guard (MV-D65): the wheel must never import ``backend``.

The allowed dependency direction is app → wheel (the FastAPI backend depends on
``genie_space_optimizer``). A ``wheel → backend`` import inverts that and breaks on
the job cluster, where ``backend`` is absent (the Stage-4.1c root cause: the
ontology enrichers degraded → all Pages ``certify=false``). This test fails if any
source file under ``genie_space_optimizer`` reintroduces such an import.
"""

from __future__ import annotations

import pathlib
import re

import genie_space_optimizer

_BACKEND_IMPORT = re.compile(r"(from|import)\s+backend\b")


def test_wheel_never_imports_backend():
    root = pathlib.Path(genie_space_optimizer.__file__).resolve().parent
    offenders: list[str] = []
    for py in sorted(root.rglob("*.py")):
        for lineno, line in enumerate(py.read_text(encoding="utf-8").splitlines(), 1):
            if _BACKEND_IMPORT.match(line.strip()):
                offenders.append(f"{py}:{lineno}: {line.strip()}")
    assert not offenders, (
        "genie_space_optimizer must not import `backend` (MV-D65 layering):\n"
        + "\n".join(offenders)
    )
