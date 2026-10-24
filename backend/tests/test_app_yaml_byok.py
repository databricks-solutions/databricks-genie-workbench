"""Pin how app.yaml carries the BYOK flag (Phase 2).

validate_chat_model reads GENIE_BYOK_ENABLED. app.yaml ships it as an installer
placeholder (the notebook always fills "false"; deploy.sh reads GENIE_BYOK_ENABLED,
default "false"), so BYOK acceptance stays off unless the operator enables it
(spec §2, §9 Decision 2).
"""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _app_env() -> dict[str, str]:
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((_REPO_ROOT / "app.yaml").read_text())
    return {e["name"]: e.get("value") for e in doc["env"] if "name" in e}


def test_genie_byok_enabled_is_an_installer_placeholder():
    env = _app_env()
    assert env.get("GENIE_BYOK_ENABLED") == "__GENIE_BYOK_ENABLED__"
