"""The app version comes from the repo-root VERSION file; build info from app.yaml env (spec 5.1, 5.2)."""

from pathlib import Path

import pytest

from backend import app_version

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_version_file_ships_0_2_1():
    assert (_REPO_ROOT / "VERSION").read_text(encoding="utf-8") == "0.2.1\n"


def test_read_version_matches_the_version_file():
    assert app_version.read_version() == "0.2.1"


@pytest.mark.parametrize("raw", [b"0.2.1\n", b"  0.2.1  ", b"0.2.1\r\n"])
def test_version_file_whitespace_is_stripped(tmp_path, raw):
    f = tmp_path / "VERSION"
    f.write_bytes(raw)
    assert app_version.read_version_file(f) == "0.2.1"


def test_missing_or_empty_version_file_is_unknown(tmp_path):
    assert app_version.read_version_file(tmp_path / "missing") == "unknown"
    empty = tmp_path / "VERSION"
    empty.write_text("")
    assert app_version.read_version_file(empty) == "unknown"


def test_sdk_user_agent_reports_the_version_file():
    from backend._telemetry import PRODUCT_NAME, PRODUCT_VERSION

    assert PRODUCT_NAME == "genie-workbench"
    assert PRODUCT_VERSION == app_version.read_version()


def test_build_info_reads_installer_env(monkeypatch):
    monkeypatch.setenv("GWB_BUILD_COMMIT", "99e7783ab12c")
    monkeypatch.setenv("GWB_DEPLOYED_AT", "2026-11-05T15:04:00Z")
    monkeypatch.setenv("GWB_INSTALL_METHOD", "cli")
    info = app_version.get_build_info()
    assert info == app_version.BuildInfo("0.2.1", "99e7783ab12c", "2026-11-05T15:04:00Z", "cli")


# Review Focus 2: an unrendered template must never leak placeholder text.
@pytest.mark.parametrize("value", ["", "   ", "__GWB_BUILD_COMMIT__"])
def test_empty_or_unrendered_build_env_is_not_recorded(monkeypatch, value):
    monkeypatch.setenv("GWB_BUILD_COMMIT", value)
    assert app_version.get_build_info().commit is None


@pytest.mark.parametrize(
    "value,expected",
    [("true", True), ("TRUE", True), (" false ", False), ("", None), ("__UPDATE_CHECK_ENABLED__", None), ("maybe", None)],
)
def test_install_time_update_check(monkeypatch, value, expected):
    monkeypatch.setenv("UPDATE_CHECK_ENABLED", value)
    assert app_version.install_time_update_check() is expected


def test_install_time_update_check_unset(monkeypatch):
    monkeypatch.delenv("UPDATE_CHECK_ENABLED", raising=False)
    assert app_version.install_time_update_check() is None


def test_release_url():
    assert app_version.release_url("0.2.1") == (
        "https://github.com/databricks-solutions/databricks-genie-workbench/releases/tag/0.2.1"
    )
    assert app_version.release_url("unknown") is None


def test_version_file_is_shipped_by_the_notebook_installer():
    from scripts.deploy_lib.workspace_source import should_copy

    assert should_copy(_REPO_ROOT / "VERSION", _REPO_ROOT)
