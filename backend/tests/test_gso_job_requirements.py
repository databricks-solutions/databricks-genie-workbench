"""Guards for the GSO job's locked requirements file (requirements-job.txt)."""

import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from scripts.deploy_lib.config import InstallConfig
from scripts.deploy_lib.gso_job import upload_gso_requirements
from scripts.deploy_lib.workspace_source import PRIVATE_PYTHON_REGISTRY_HOSTS
from scripts.export_gso_job_requirements import (
    BASE_PACKAGES_SNAPSHOT_PATH,
    OUTPUT_PATH,
    load_base_package_names,
    normalize_name,
    render,
    requirement_name,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _committed_pins() -> dict[str, set[str]]:
    pins: dict[str, set[str]] = {}
    for line in OUTPUT_PATH.read_text().splitlines():
        name = requirement_name(line)
        if name:
            spec = line.split(";", 1)[0].strip()
            pins.setdefault(name, set()).add(spec.split("==", 1)[1])
    return pins


def test_render_drops_base_environment_packages():
    requirements = [
        "huggingface-hub==1.8.0",
        "Protobuf==6.33.6",
        "typing_extensions==4.15.0",
        "numpy==2.4.3 ; python_full_version >= '3.12'",
    ]
    content = render(requirements, {"protobuf", "typing-extensions", "numpy"})
    assert [line for line in content.splitlines() if not line.startswith("#")] == ["huggingface-hub==1.8.0"]


def test_requirement_name_normalizes_and_skips_non_requirements():
    assert requirement_name("Typing_Extensions==4.15.0") == "typing-extensions"
    assert requirement_name("numpy>=1.26 ; python_version < '3.12'") == "numpy"
    assert requirement_name("# comment") is None
    assert requirement_name("-c other.txt") is None
    assert normalize_name("zope.interface") == "zope-interface"


def test_committed_requirements_match_uv_lock_versions():
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    locked: dict[str, set[str]] = {}
    for package in lock["package"]:
        # Workspace members (editable/virtual) carry no version.
        if "version" in package:
            locked.setdefault(normalize_name(package["name"]), set()).add(package["version"])

    pins = _committed_pins()
    assert pins, "requirements-job.txt has no pins"
    for name, versions in pins.items():
        assert versions <= locked.get(name, set()), (
            f"{name}=={versions} not in uv.lock; run python scripts/export_gso_job_requirements.py"
        )


def test_committed_requirements_exclude_base_packages_and_private_registries():
    content = OUTPUT_PATH.read_text()
    assert not set(_committed_pins()) & load_base_package_names(BASE_PACKAGES_SNAPSHOT_PATH)
    assert not any(host in content for host in PRIVATE_PYTHON_REGISTRY_HOSTS)
    assert "--index-url" not in content and "--extra-index-url" not in content


def test_bundles_install_requirements_with_the_wheel():
    yaml = pytest.importorskip("yaml")
    for bundle_path, expected in (
        (REPO_ROOT / "databricks.yml", "-r ./packages/genie-space-optimizer/requirements-job.txt"),
        (REPO_ROOT / "packages/genie-space-optimizer/databricks.yml", "-r ./requirements-job.txt"),
    ):
        bundle = yaml.safe_load(bundle_path.read_text())
        (job,) = bundle["resources"]["jobs"].values()
        deps = job["environments"][0]["spec"]["dependencies"]
        assert deps[0] == expected, bundle_path
        assert (bundle_path.parent / expected.removeprefix("-r ./")).exists()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not installed")
def test_committed_requirements_are_fresh():
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "export_gso_job_requirements.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


class _FakeFiles:
    def __init__(self):
        self.uploads = {}

    def upload(self, path, f, overwrite=False):
        self.uploads[path] = f.read()


class _FakeWorkspace:
    def __init__(self):
        self.files = _FakeFiles()


def test_upload_gso_requirements_places_file_next_to_wheel():
    w = _FakeWorkspace()
    cfg = InstallConfig(app_name="genie-workbench", catalog="main", warehouse_id="wh", repo_root=str(REPO_ROOT))
    wheel = "/Volumes/main/genie_space_optimizer/app_artifacts/genie_space_optimizer-0.0.0-py3-none-any.whl"

    path = upload_gso_requirements(w, cfg, wheel)

    assert path == "/Volumes/main/genie_space_optimizer/app_artifacts/genie_space_optimizer-requirements-job.txt"
    assert w.files.uploads[path] == OUTPUT_PATH.read_bytes()
