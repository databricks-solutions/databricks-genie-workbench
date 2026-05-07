"""Cycle 12-T1: best-effort build metadata helpers."""
from __future__ import annotations

import re


def test_read_python_version_returns_dotted_string() -> None:
    from genie_space_optimizer.common.build_metadata import read_python_version

    version = read_python_version()

    assert isinstance(version, str)
    assert re.match(r"^\d+\.\d+\.\d+", version), version


def test_read_git_sha_from_env_var(monkeypatch) -> None:
    from genie_space_optimizer.common.build_metadata import read_git_sha

    monkeypatch.setenv("GSO_GIT_SHA", "deadbeef" * 5)
    monkeypatch.setenv("PATH", "/nonexistent")  # ensure git binary not used

    sha = read_git_sha()

    assert sha == "deadbeef" * 5


def test_read_git_sha_returns_empty_when_no_source(monkeypatch, tmp_path) -> None:
    from genie_space_optimizer.common.build_metadata import read_git_sha

    monkeypatch.delenv("GSO_GIT_SHA", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PATH", "/nonexistent")

    sha = read_git_sha()

    assert sha == ""


def test_read_wheel_sha_returns_empty_for_unknown_package(monkeypatch) -> None:
    from genie_space_optimizer.common.build_metadata import read_wheel_sha

    sha = read_wheel_sha("definitely_not_a_real_package_zzz_12345")

    assert sha == ""


def test_read_wheel_sha_returns_version_when_installed() -> None:
    """``genie_space_optimizer`` is installed in editable mode for tests; its
    package metadata is available via ``importlib.metadata``."""
    from genie_space_optimizer.common.build_metadata import read_wheel_sha

    sha = read_wheel_sha("genie_space_optimizer")

    assert sha != ""  # some non-empty version/build identifier


def test_read_domain_from_env_var(monkeypatch) -> None:
    from genie_space_optimizer.common.build_metadata import read_domain

    monkeypatch.setenv("GSO_DOMAIN", "airline")

    assert read_domain() == "airline"


def test_read_domain_defaults_to_empty(monkeypatch) -> None:
    from genie_space_optimizer.common.build_metadata import read_domain

    monkeypatch.delenv("GSO_DOMAIN", raising=False)

    assert read_domain() == ""


def test_helpers_never_raise(monkeypatch) -> None:
    """Each reader must swallow every exception and return ''."""
    from genie_space_optimizer.common import build_metadata

    monkeypatch.delenv("GSO_GIT_SHA", raising=False)
    monkeypatch.delenv("GSO_DOMAIN", raising=False)
    monkeypatch.setenv("PATH", "/nonexistent")

    # Should not raise even when no metadata sources are reachable.
    build_metadata.read_python_version()
    build_metadata.read_git_sha()
    build_metadata.read_wheel_sha("genie_space_optimizer")
    build_metadata.read_wheel_sha("nonexistent_pkg_zzz")
    build_metadata.read_domain()
