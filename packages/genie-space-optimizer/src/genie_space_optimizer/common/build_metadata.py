"""Cycle 12-T1: best-effort readers for run-time build metadata.

These helpers are deliberately defensive — they MUST NOT raise. Any failure
returns an empty string so the caller (the GSO_RUN_MANIFEST_V2 emitter) can
always produce a valid marker line.
"""
from __future__ import annotations

import os
import subprocess
import sys
from importlib import metadata as _importlib_metadata


def read_python_version() -> str:
    """Return the running interpreter version, e.g. ``"3.11.6"``."""
    try:
        info = sys.version_info
        return f"{info.major}.{info.minor}.{info.micro}"
    except Exception:
        return ""


def read_git_sha() -> str:
    """Return the current git HEAD sha, best-effort.

    Resolution order:
    1. ``GSO_GIT_SHA`` environment variable (set by the build/deploy step).
    2. ``git rev-parse HEAD`` in the current working directory.
    3. Empty string.
    """
    env_sha = (os.environ.get("GSO_GIT_SHA") or "").strip()
    if env_sha:
        return env_sha
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return ""


def read_wheel_sha(distribution_name: str) -> str:
    """Return a wheel/build identifier for ``distribution_name``.

    Uses ``importlib.metadata`` to read the installed distribution's version.
    For wheels built with a build-id appended (e.g. ``1.2.3+g0a1b2c3``), the
    full local-version segment is preserved.

    Returns ``""`` when the distribution is not installed or metadata is
    unreadable.
    """
    try:
        return _importlib_metadata.version(distribution_name) or ""
    except Exception:
        return ""


def read_domain() -> str:
    """Return the data domain for this run, e.g. ``"airline"``.

    Sourced from the ``GSO_DOMAIN`` environment variable. Returns ``""``
    when unset. Future cycles may derive this from Genie space metadata.
    """
    try:
        return (os.environ.get("GSO_DOMAIN") or "").strip()
    except Exception:
        return ""
