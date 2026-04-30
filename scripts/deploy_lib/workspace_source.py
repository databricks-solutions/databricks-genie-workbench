"""Generate the curated workspace source folder used by notebook installs."""

from __future__ import annotations

import base64
from pathlib import Path
import shutil
from typing import Iterable

from .config import InstallConfig


EXCLUDE_NAMES = {
    ".DS_Store",
    ".databricks",
    ".env",
    ".env.deploy",
    ".env.deploy.template",
    ".git",
    ".gitignore",
    ".gitleaksignore",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    ".vscode",
    "CLAUDE.md",
    "CODEOWNERS.txt",
    "LICENSE.md",
    "NOTICE.md",
    "README.md",
    "SECURITY.md",
    "__pycache__",
    "databricks.yml",
    "deploy-config.sh",
    "deploy.sh",
    "mlflow.db",
    "node_modules",
    "requirements.txt",
    "venv",
}

EXCLUDE_DIRS = {
    ".build",
    ".cursor",
    ".databricks",
    ".git",
    ".idea",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    ".vscode",
    "__pycache__",
    "mlruns",
    "node_modules",
    "notebooks",
    "output",
    "scratch",
    "scripts",
    "sql",
    "tests",
    "TODO",
    "venv",
}

EXCLUDE_SUFFIXES = {".pyc", ".pyo"}


def default_source_path(deployer_user: str, app_name: str) -> str:
    return f"/Workspace/Users/{deployer_user}/.genie-workbench-deploy/{app_name}/app"


def default_gso_path(deployer_user: str, app_name: str) -> str:
    return f"/Workspace/Users/{deployer_user}/.genie-workbench-deploy/{app_name}/gso"


def should_copy(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    parts = rel.split("/")
    name = path.name

    if rel == "backend/references/schema.md":
        return True
    if name in EXCLUDE_NAMES:
        return False
    if path.suffix in EXCLUDE_SUFFIXES:
        return False
    if path.suffix == ".md":
        return False
    if any(part in EXCLUDE_DIRS for part in parts):
        return False
    if rel.startswith("frontend/node_modules/"):
        return False
    if rel.startswith("packages/genie-space-optimizer/node_modules/"):
        return False
    if rel.startswith("packages/genie-space-optimizer/tests/"):
        return False
    if rel.startswith("packages/genie-space-optimizer/browser-test-output/"):
        return False
    if rel.startswith("packages/genie-space-optimizer/.build/"):
        return False
    return True


def iter_runtime_files(repo_root: Path) -> Iterable[Path]:
    for path in repo_root.rglob("*"):
        if path.is_file() and should_copy(path, repo_root):
            yield path


def _can_use_local_path(path: str) -> bool:
    p = Path(path)
    return path.startswith("/Workspace/") and (Path("/Workspace").exists() or p.parent.exists())


def _api_do(w, method: str, path: str, body: dict | None = None) -> dict:
    return w.api_client.do(method=method, path=path, body=body)


def mkdirs(w, workspace_path: str) -> None:
    if _can_use_local_path(workspace_path):
        Path(workspace_path).mkdir(parents=True, exist_ok=True)
        return
    _api_do(w, "POST", "/api/2.0/workspace/mkdirs", {"path": workspace_path})


def delete_workspace_path(w, workspace_path: str) -> None:
    if _can_use_local_path(workspace_path):
        shutil.rmtree(workspace_path, ignore_errors=True)
    try:
        _api_do(
            w,
            "POST",
            "/api/2.0/workspace/delete",
            {"path": workspace_path, "recursive": True},
        )
    except Exception:
        # Deleting a missing generated folder should be idempotent.
        pass


def write_workspace_file(w, workspace_path: str, content: bytes) -> None:
    if _can_use_local_path(workspace_path):
        dst = Path(workspace_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(content)
        return

    if hasattr(w, "files"):
        from io import BytesIO

        try:
            parent = str(Path(workspace_path).parent)
            if parent:
                w.files.create_directory(parent)
            w.files.upload(workspace_path, BytesIO(content), overwrite=True)
            return
        except Exception:
            pass

    mkdirs(w, str(Path(workspace_path).parent))
    _api_do(
        w,
        "POST",
        "/api/2.0/workspace/import",
        {
            "path": workspace_path,
            "format": "AUTO",
            "overwrite": True,
            "content": base64.b64encode(content).decode("ascii"),
        },
    )


def upload_source_notebook(w, src_path: Path, workspace_path_without_ext: str) -> None:
    mkdirs(w, str(Path(workspace_path_without_ext).parent))
    _api_do(
        w,
        "POST",
        "/api/2.0/workspace/import",
        {
            "path": workspace_path_without_ext,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": base64.b64encode(src_path.read_bytes()).decode("ascii"),
        },
    )


def prepare_workspace_source(w, cfg: InstallConfig, deployer_user: str) -> str:
    repo_root = Path(cfg.repo_root or "").resolve()
    source_path = cfg.deploy_workspace_path or default_source_path(deployer_user, cfg.app_name)

    delete_workspace_path(w, source_path)
    mkdirs(w, source_path)

    for src in iter_runtime_files(repo_root):
        rel = src.relative_to(repo_root).as_posix()
        write_workspace_file(w, f"{source_path}/{rel}", src.read_bytes())

    return source_path

