"""Guards the certifi floor for the App and the GSO serverless job.

Deployments are blocked for certifi older than 2026.10.19. The GSO job's
serverless base image ships an older certifi (serverless-env-v4-base-packages.txt),
and base packages are filtered out of requirements-job.txt, so the GSO wheel's own
direct pin is what upgrades certifi in the job environment.
"""

import tomllib
from pathlib import Path

from scripts.export_gso_job_requirements import requirement_name

REPO_ROOT = Path(__file__).resolve().parents[2]
MIN_CERTIFI = (2026, 10, 19)


def _version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split("."))


def _certifi_pins(pyproject: Path) -> list[str]:
    deps = tomllib.loads(pyproject.read_text())["project"]["dependencies"]
    return [dep for dep in deps if requirement_name(dep) == "certifi"]


def test_pyprojects_pin_certifi_at_or_above_floor():
    for pyproject in (
        REPO_ROOT / "pyproject.toml",
        REPO_ROOT / "packages/genie-space-optimizer/pyproject.toml",
    ):
        (pin,) = _certifi_pins(pyproject)
        assert pin.startswith("certifi=="), f"{pyproject}: certifi must be an exact pin, got {pin!r}"
        assert _version(pin.split("==", 1)[1]) >= MIN_CERTIFI, pyproject


def test_uv_lock_resolves_certifi_at_or_above_floor():
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    versions = [pkg["version"] for pkg in lock["package"] if pkg["name"] == "certifi"]
    assert versions, "certifi missing from uv.lock"
    for version in versions:
        assert _version(version) >= MIN_CERTIFI, f"uv.lock certifi=={version}"


def test_requirements_txt_pins_certifi_at_or_above_floor():
    pins = [
        line.split(";", 1)[0].strip()
        for line in (REPO_ROOT / "requirements.txt").read_text().splitlines()
        if requirement_name(line) == "certifi"
    ]
    assert pins, "certifi missing from requirements.txt"
    for pin in pins:
        assert _version(pin.split("==", 1)[1]) >= MIN_CERTIFI, pin
