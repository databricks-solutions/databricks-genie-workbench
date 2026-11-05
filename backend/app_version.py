"""Installed Genie Workbench version and deploy-time build info (spec 5.1, 5.2, 5.4).

Stdlib only: backend/_telemetry.py imports this at module load, so it must not
import anything from the app.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

RELEASES_REPO = "databricks-solutions/databricks-genie-workbench"
UNKNOWN_VERSION = "unknown"
VERSION_FILE = Path(__file__).resolve().parent.parent / "VERSION"

_PLACEHOLDER_RE = re.compile(r"^__[A-Z0-9_]+__$")


def read_version_file(path: Path) -> str:
    """Return the stripped contents of a VERSION file, or "unknown"."""
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return UNKNOWN_VERSION
    return value or UNKNOWN_VERSION


@lru_cache(maxsize=1)
def read_version() -> str:
    """The installed app version from the repo-root VERSION file (cached)."""
    return read_version_file(VERSION_FILE)


def env_value(name: str) -> str | None:
    """An env var's value, or None when empty or an unrendered app.yaml placeholder."""
    value = (os.environ.get(name) or "").strip()
    if not value or _PLACEHOLDER_RE.match(value):
        return None
    return value


@dataclass(frozen=True)
class BuildInfo:
    version: str
    commit: str | None
    deployed_at: str | None
    install_method: str | None


def get_build_info() -> BuildInfo:
    """Version plus the build details the installers stamp into app.yaml."""
    return BuildInfo(
        version=read_version(),
        commit=env_value("GWB_BUILD_COMMIT"),
        deployed_at=env_value("GWB_DEPLOYED_AT"),
        install_method=env_value("GWB_INSTALL_METHOD"),
    )


def release_url(version: str) -> str | None:
    """GitHub release page for a version, or None when the version is unknown."""
    if version == UNKNOWN_VERSION:
        return None
    return f"https://github.com/{RELEASES_REPO}/releases/tag/{version}"


def install_time_update_check() -> bool | None:
    """UPDATE_CHECK_ENABLED from app.yaml: True/False when decided at install, else None."""
    value = (env_value("UPDATE_CHECK_ENABLED") or "").lower()
    if value == "true":
        return True
    if value == "false":
        return False
    return None
