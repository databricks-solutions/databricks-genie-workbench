"""Guards the certifi floor for the App, and keeps certifi out of the GSO wheel.

Apps deployments are blocked for certifi older than 2026.10.19, so the root
pyproject.toml pins it exactly and uv.lock / requirements.txt must resolve to
at least that version.

The GSO serverless job takes certifi from the serverless base image instead.
An exact certifi pin in the GSO wheel would downgrade a newer base-image
certifi and break again at the next floor bump, so the GSO package must not
declare certifi, and requirements-job.txt must not pin it.
"""

import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.version import Version

from scripts.export_gso_job_requirements import OUTPUT_PATH, requirement_name

REPO_ROOT = Path(__file__).resolve().parents[2]
MIN_CERTIFI = Version("2026.10.19")


def _certifi_requirements(lines: list[str]) -> list[Requirement]:
    return [Requirement(line) for line in lines if requirement_name(line) == "certifi"]


def _pyproject_certifi(pyproject: Path) -> list[Requirement]:
    return _certifi_requirements(tomllib.loads(pyproject.read_text())["project"]["dependencies"])


def _pinned_version(req: Requirement) -> Version:
    (spec,) = req.specifier
    assert spec.operator == "==", f"certifi must be an exact pin, got {req}"
    return Version(spec.version)


def test_root_pyproject_pins_certifi_at_or_above_floor():
    (req,) = _pyproject_certifi(REPO_ROOT / "pyproject.toml")
    assert _pinned_version(req) >= MIN_CERTIFI


def test_gso_package_does_not_declare_certifi():
    assert _pyproject_certifi(REPO_ROOT / "packages/genie-space-optimizer/pyproject.toml") == []


def test_gso_job_requirements_do_not_pin_certifi():
    assert _certifi_requirements(OUTPUT_PATH.read_text().splitlines()) == []


def test_uv_lock_resolves_certifi_at_or_above_floor():
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    versions = [Version(pkg["version"]) for pkg in lock["package"] if pkg["name"] == "certifi"]
    assert versions, "certifi missing from uv.lock"
    assert min(versions) >= MIN_CERTIFI


def test_requirements_txt_pins_certifi_at_or_above_floor():
    reqs = _certifi_requirements((REPO_ROOT / "requirements.txt").read_text().splitlines())
    assert reqs, "certifi missing from requirements.txt"
    for req in reqs:
        assert _pinned_version(req) >= MIN_CERTIFI
