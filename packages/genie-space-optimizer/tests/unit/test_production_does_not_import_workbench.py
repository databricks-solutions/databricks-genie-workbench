"""Trial 13l — layer-direction guard: production must not import workbench.

The project's architectural invariant is one-way: ``devtools/`` may
import from ``src/``; the production package
(``genie_space_optimizer``) must never reach into
``devtools/local_lever_workbench/``. AST-walks every production source
file and asserts no ``Import`` / ``ImportFrom`` node references
``devtools`` or ``local_lever_workbench``.

If this test ever fails, the fix is to invert the dependency: move the
shared code into the production package and let the workbench module
re-export it, mirroring the Trial 13l pattern for
``_extract_fqn_columns`` / ``_fetch_schema_columns_for_space``.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

_PROD_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
)

_FORBIDDEN_TOKENS: tuple[str, ...] = (
    "devtools",
    "local_lever_workbench",
)


def _imported_module_names(source: str) -> list[str]:
    """Return every dotted module name imported by ``source``."""
    tree = ast.parse(source)
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.append(node.module)
    return names


def _production_python_files() -> list[Path]:
    return sorted(p for p in _PROD_ROOT.rglob("*.py") if p.is_file())


@pytest.mark.parametrize("path", _production_python_files(), ids=str)
def test_production_module_does_not_import_workbench(path: Path) -> None:
    """Every production module: no import references devtools/workbench."""
    source = path.read_text(encoding="utf-8")
    for module_name in _imported_module_names(source):
        for token in _FORBIDDEN_TOKENS:
            assert token not in module_name.split("."), (
                f"{path.relative_to(_PROD_ROOT.parents[1])} imports "
                f"forbidden module {module_name!r} (token {token!r}). "
                "Production code must not reach into devtools/workbench; "
                "promote the shared code into src/ and re-export from "
                "the workbench module instead."
            )
