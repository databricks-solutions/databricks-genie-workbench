"""app.yaml carries the release-check setting and build info as installer placeholders (spec 5.2, 5.4)."""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _app_env() -> dict[str, str]:
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((_REPO_ROOT / "app.yaml").read_text())
    return {e["name"]: e.get("value") for e in doc["env"] if "name" in e}


@pytest.mark.parametrize("name", ["UPDATE_CHECK_ENABLED", "GWB_BUILD_COMMIT", "GWB_DEPLOYED_AT", "GWB_INSTALL_METHOD"])
def test_release_check_and_build_info_are_installer_placeholders(name):
    assert _app_env().get(name) == f"__{name}__"
