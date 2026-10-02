"""Every file `tsc -b` compiles must exist in a notebook install's source copy.

`tsconfig.app.json` compiles all of `frontend/src` except test files, and the
notebook installer ships only what `should_copy` keeps — no `tests` directory,
no prebuilt `frontend/dist` — before the platform runs `npm run build`.
"""

import re
from pathlib import Path

from scripts.deploy_lib.workspace_source import should_copy

REPO = Path(__file__).resolve().parents[2]
FRONTEND = REPO / "frontend"
SRC = FRONTEND / "src"
_IMPORT = re.compile(r"""(?:\bfrom\s+|\bimport\s*\(\s*|\bimport\s+)['"]([^'"]+)['"]""")
_TEST_FILE = re.compile(r"\.(test|spec)\.tsx?$")
_SUFFIXES = ("", ".ts", ".tsx", ".d.ts", "/index.ts", "/index.tsx")


def _resolve(module: Path, specifier: str) -> Path | None:
    specifier = specifier.split("?", 1)[0]
    if specifier.startswith("@/"):
        base = SRC / specifier[2:]
    elif specifier.startswith("."):
        base = (module.parent / specifier).resolve()
    else:
        return None
    for suffix in _SUFFIXES:
        candidate = Path(f"{base}{suffix}")
        if candidate.is_file():
            return candidate
    return base


def test_app_modules_import_only_files_the_notebook_install_ships():
    offenders = []
    for module in sorted(SRC.rglob("*")):
        if module.suffix not in {".ts", ".tsx"} or _TEST_FILE.search(module.name):
            continue
        for specifier in _IMPORT.findall(module.read_text()):
            target = _resolve(module, specifier)
            if target is None:
                continue
            if (not target.is_file() or not target.is_relative_to(FRONTEND)
                    or not should_copy(target, REPO)):
                offenders.append(f"{module.relative_to(REPO)} -> {specifier}")
    assert offenders == []
