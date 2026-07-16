"""Keep the supported four-notebook workflow independent of retired engines."""

from __future__ import annotations

import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "genie_space_optimizer"
ENTRYPOINTS = (
    "jobs.run_intake_and_snapshot",
    "jobs.run_benchmark_qc_and_repair",
    "jobs.run_optimize",
    "jobs.run_publish_and_audit",
)
FORBIDDEN = (
    "optimization.harness",
    "optimization.evaluation",
    "optimization.optimizer",
    "optimization.scorers",
    "optimization.stages",
    "optimization.afs",
    "optimization.control_plane",
    "optimization.strategist",
    "optimization.synthesis",
    "optimization.preflight_synthesis",
    "optimization.cluster_driven_synthesis",
    "optimization.rca",
    "optimization.repeatability",
    "optimization.scoreboard",
    "optimization.labeling",
    "optimization.ground_truth_corrections",
    "optimization.lever_loop_replay",
)


def _path_for(module: str) -> Path | None:
    path = PACKAGE_ROOT.joinpath(*module.split(".")).with_suffix(".py")
    if path.exists():
        return path
    init = PACKAGE_ROOT.joinpath(*module.split("."), "__init__.py")
    return init if init.exists() else None


def _local_imports(module: str, path: Path) -> set[str]:
    imports: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                package = module.split(".")[:-node.level]
                base = ".".join(package + ([node.module] if node.module else []))
            else:
                base = node.module or ""
            names = [base]
        else:
            continue
        for name in names:
            prefix = "genie_space_optimizer."
            if name.startswith(prefix):
                imports.add(name[len(prefix):])
            elif _path_for(name) is not None:
                imports.add(name)
    return imports


def test_four_notebook_dependency_closure_excludes_retired_engines() -> None:
    pending = list(ENTRYPOINTS)
    visited: set[str] = set()
    violations: set[str] = set()

    while pending:
        module = pending.pop()
        if module in visited:
            continue
        visited.add(module)
        if any(module == item or module.startswith(f"{item}.") for item in FORBIDDEN):
            violations.add(module)
            continue
        path = _path_for(module)
        if path is not None:
            pending.extend(_local_imports(module, path) - visited)

    assert not violations, f"four-notebook workflow imports retired modules: {sorted(violations)}"
